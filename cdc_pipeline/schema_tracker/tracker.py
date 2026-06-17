"""Schema tracker core implementation plus a minimal DDL -> schema rewriter.

Real DDL parsing is very complex and, in production, CDC pipelines
usually consult the source database's INFORMATION_SCHEMA after every
DDL to capture the authoritative new table definition.  The parser
below is intentionally small and illustrative; it covers the most
common column-level operations (ADD / DROP / MODIFY COLUMN, RENAME
COLUMN) and delegates everything else to a "re-fetch from source"
callback that production code would wire to a real DB connection.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..config import SchemaStorageConfig
from ..log_parser.base import BinlogPosition
from ..models.schema import ColumnDef, TableSchema


# ---------------------------------------------------------------------------
# DDL rewriting helpers
# ---------------------------------------------------------------------------

_ADD_COLUMN_RE = re.compile(
    r"ADD\s+(?:COLUMN\s+)?[`\"]?(\w+)[`\"]?\s+(\w+(?:\s*\(\s*\d+(?:\s*,\s*\d+)*\s*\))?(?:\s+UNSIGNED)?(?:\s+ZEROFILL)?)",
    re.IGNORECASE,
)
_DROP_COLUMN_RE = re.compile(r"DROP\s+(?:COLUMN\s+)?[`\"]?(\w+)[`\"]?", re.IGNORECASE)
_MODIFY_COLUMN_RE = re.compile(
    r"MODIFY\s+(?:COLUMN\s+)?[`\"]?(\w+)[`\"]?\s+(\w+(?:\s*\(\s*\d+(?:\s*,\s*\d+)*\s*\))?(?:\s+UNSIGNED)?(?:\s+ZEROFILL)?)",
    re.IGNORECASE,
)
_CHANGE_COLUMN_RE = re.compile(
    r"CHANGE\s+(?:COLUMN\s+)?[`\"]?(\w+)[`\"]?\s+[`\"]?(\w+)[`\"]?\s+(\w+(?:\s*\(\s*\d+(?:\s*,\s*\d+)*\s*\))?(?:\s+UNSIGNED)?(?:\s+ZEROFILL)?)",
    re.IGNORECASE,
)
_RENAME_TABLE_RE = re.compile(
    r"RENAME\s+TABLE\s+(?:IF\s+EXISTS\s+)?[`\"]?([\w.]+)[`\"]?\s+TO\s+[`\"]?([\w.]+)[`\"]?",
    re.IGNORECASE,
)
_AFTER_RE = re.compile(r"AFTER\s+[`\"]?(\w+)[`\"]?", re.IGNORECASE)
_FIRST_RE = re.compile(r"\bFIRST\b", re.IGNORECASE)


def apply_ddl_to_schema(
    ddl: str,
    schema: Optional[TableSchema],
    database: str,
    table: str,
) -> Optional[TableSchema]:
    """Apply a DDL statement to ``schema`` and return the new version.

    Returns ``None`` if the statement was a ``DROP TABLE`` (the caller
    should then evict the schema from the tracker), or a new
    :class:`TableSchema` on success.  Unknown DDL forms return the
    input schema unchanged but with a bumped version so downstream
    consumers can at least observe that something happened.
    """
    upper = ddl.strip().upper()

    if "DROP TABLE" in upper:
        return None

    if "RENAME TABLE" in upper:
        m = _RENAME_TABLE_RE.search(ddl)
        if m and schema is not None:
            new = schema
            return TableSchema(
                database=new.database,
                table=new.table,
                columns=[ColumnDef.from_dict(c.to_dict()) for c in new.columns],
                primary_key_columns=list(new.primary_key_columns),
                version=new.version + 1,
                captured_at=time.time(),
            )
        return schema

    if "CREATE TABLE" in upper and schema is None:
        # Without fully parsing the CREATE TABLE we can't build a real
        # schema, so we return an empty placeholder.  Production code
        # would call back into the source database here.
        return TableSchema(
            database=database,
            table=table,
            columns=[],
            primary_key_columns=[],
            version=1,
            captured_at=time.time(),
        )

    if schema is None:
        # Can't mutate a schema we haven't seen; return None.
        return None

    if "ALTER TABLE" not in upper:
        return _bump_version(schema)

    columns: List[ColumnDef] = [ColumnDef.from_dict(c.to_dict()) for c in schema.columns]
    pks: List[str] = list(schema.primary_key_columns)

    # --- DROP COLUMN ---------------------------------------------------
    for m in _DROP_COLUMN_RE.finditer(ddl):
        name = m.group(1)
        columns = [c for c in columns if c.name != name]
        if name in pks:
            pks.remove(name)

    # --- MODIFY COLUMN -------------------------------------------------
    for m in _MODIFY_COLUMN_RE.finditer(ddl):
        name, data_type = m.group(1), m.group(2)
        for col in columns:
            if col.name == name:
                col.data_type = data_type

    # --- CHANGE COLUMN (rename + retype) ------------------------------
    for m in _CHANGE_COLUMN_RE.finditer(ddl):
        old, new, data_type = m.group(1), m.group(2), m.group(3)
        for col in columns:
            if col.name == old:
                col.name = new
                col.data_type = data_type
        if old in pks:
            pks[pks.index(old)] = new

    # --- ADD COLUMN ----------------------------------------------------
    for m in _ADD_COLUMN_RE.finditer(ddl):
        name, data_type = m.group(1), m.group(2)
        if any(c.name == name for c in columns):
            continue
        new_col = ColumnDef(name=name, data_type=data_type, ordinal_position=len(columns))
        after_match = _AFTER_RE.search(ddl, m.end())
        if _FIRST_RE.search(ddl, m.start(), m.end() + 50):
            columns.insert(0, new_col)
        elif after_match:
            after_name = after_match.group(1)
            for idx, col in enumerate(columns):
                if col.name == after_name:
                    columns.insert(idx + 1, new_col)
                    break
            else:
                columns.append(new_col)
        else:
            columns.append(new_col)

    for i, col in enumerate(columns):
        col.ordinal_position = i

    return TableSchema(
        database=schema.database,
        table=schema.table,
        columns=columns,
        primary_key_columns=pks,
        version=schema.version + 1,
        captured_at=time.time(),
    )


def _bump_version(schema: TableSchema) -> TableSchema:
    return TableSchema(
        database=schema.database,
        table=schema.table,
        columns=[ColumnDef.from_dict(c.to_dict()) for c in schema.columns],
        primary_key_columns=list(schema.primary_key_columns),
        version=schema.version + 1,
        captured_at=time.time(),
    )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

class SchemaHistoryStore(ABC):
    @abstractmethod
    def load(self) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def save(self, entries: List[Dict[str, Any]]) -> None:
        raise NotImplementedError


class FileSchemaHistoryStore(SchemaHistoryStore):
    def __init__(self, file_path: str) -> None:
        self._file_path = os.path.abspath(file_path)
        self._tmp_path = self._file_path + ".tmp"

    def load(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self._file_path):
            return []
        try:
            with open(self._file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return []

    def save(self, entries: List[Dict[str, Any]]) -> None:
        parent = os.path.dirname(self._file_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(self._tmp_path, "w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(self._tmp_path, self._file_path)


# ---------------------------------------------------------------------------
# Tracker
# ---------------------------------------------------------------------------

@dataclass
class _SchemaEntry:
    """A single versioned schema entry tagged with the log position where
    it became active."""

    position: BinlogPosition
    schema: Optional[TableSchema]
    ddl: Optional[str] = None


class SchemaTracker:
    """Tracks the versioned schema history of all observed tables.

    The tracker stores, for every ``(database, table)`` key, a list of
    :class:`_SchemaEntry` records sorted by binlog position.  Each record
    says "at binlog position X, the schema of this table was Y, following
    DDL Z".

    When :meth:`schema_at` is called with a binlog position the tracker
    returns the latest schema that was active *at or before* that
    position -- this is exactly what the event transformer needs to
    decode positional row images from the middle of the log.
    """

    def __init__(
        self,
        config: SchemaStorageConfig,
        store: Optional[SchemaHistoryStore] = None,
        schema_provider: Optional[Callable[[str, str], Optional[TableSchema]]] = None,
    ) -> None:
        self._config = config
        self._store = store or FileSchemaHistoryStore(config.file_path)
        self._schema_provider = schema_provider
        self._lock = threading.RLock()
        self._history: Dict[Tuple[str, str], List[_SchemaEntry]] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public read API
    # ------------------------------------------------------------------
    def schema_at(
        self,
        database: str,
        table: str,
        position: Optional[BinlogPosition] = None,
    ) -> Optional[TableSchema]:
        """Return the table schema active at ``position`` (or latest)."""
        with self._lock:
            entries = self._history.get((database, table))
            if not entries:
                # Never seen this table - try the external provider.
                if self._schema_provider is not None:
                    schema = self._schema_provider(database, table)
                    if schema is not None:
                        self._put(
                            database,
                            table,
                            position or BinlogPosition("", 0),
                            schema,
                            ddl=None,
                        )
                    return schema
                return None
            if position is None:
                latest = entries[-1]
                return latest.schema
            # binary search for the largest entry <= position
            lo, hi = 0, len(entries) - 1
            ans = 0
            while lo <= hi:
                mid = (lo + hi) // 2
                if entries[mid].position <= position:
                    ans = mid
                    lo = mid + 1
                else:
                    hi = mid - 1
            return entries[ans].schema

    def current_schema_version(self, database: str, table: str) -> int:
        schema = self.schema_at(database, table)
        return schema.version if schema is not None else 0

    # ------------------------------------------------------------------
    # Public write API
    # ------------------------------------------------------------------
    def register_snapshot_schema(
        self,
        schema: TableSchema,
        snapshot_position: BinlogPosition,
    ) -> None:
        """Install a schema captured during the initial snapshot.

        The snapshot position is exactly the consistent-read position the
        snapshot was taken at; that is where this schema "starts" from
        the tracker's point of view.
        """
        with self._lock:
            self._put(
                schema.database,
                schema.table,
                snapshot_position,
                schema,
                ddl=None,
            )
            self._persist()

    def apply_ddl(
        self,
        database: str,
        table: str,
        ddl: str,
        position: BinlogPosition,
    ) -> Optional[TableSchema]:
        """Process an incoming DDL and return the new schema (or None for DROP)."""
        with self._lock:
            old = self.schema_at(database, table, position)
            new_schema = apply_ddl_to_schema(ddl, old, database, table)
            self._put(database, table, position, new_schema, ddl=ddl)
            self._persist()
            return new_schema

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _load(self) -> None:
        records = self._store.load()
        for rec in records:
            key = (rec["database"], rec["table"])
            pos = BinlogPosition.from_dict(rec["position"])
            schema = None
            if rec.get("schema") is not None:
                schema = TableSchema.from_dict(rec["schema"])
            self._history.setdefault(key, []).append(
                _SchemaEntry(position=pos, schema=schema, ddl=rec.get("ddl"))
            )
        for entries in self._history.values():
            entries.sort(key=lambda e: e.position)

    def _persist(self) -> None:
        out: List[Dict[str, Any]] = []
        for (db, tbl), entries in self._history.items():
            for e in entries:
                out.append(
                    {
                        "database": db,
                        "table": tbl,
                        "position": e.position.to_dict(),
                        "schema": e.schema.to_dict() if e.schema is not None else None,
                        "ddl": e.ddl,
                    }
                )
        self._store.save(out)

    def _put(
        self,
        database: str,
        table: str,
        position: BinlogPosition,
        schema: Optional[TableSchema],
        ddl: Optional[str],
    ) -> None:
        key = (database, table)
        entries = self._history.setdefault(key, [])
        if entries and entries[-1].position >= position:
            # Out-of-order arrival (shouldn't normally happen) - still
            # insert in sorted order so schema_at() keeps working.
            for i, e in enumerate(entries):
                if e.position > position:
                    entries.insert(i, _SchemaEntry(position, schema, ddl))
                    return
            entries.append(_SchemaEntry(position, schema, ddl))
        else:
            entries.append(_SchemaEntry(position, schema, ddl))

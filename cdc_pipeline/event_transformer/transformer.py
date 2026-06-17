"""Core event transformation logic."""

from __future__ import annotations

from typing import Any, Callable, List, Optional, TYPE_CHECKING

from ..log_parser.base import (
    BinlogPosition,
    RawDDLChange,
    RawRowChange,
    RawTransactionBegin,
    RawTransactionCommit,
)
from ..models.event import (
    ChangeType,
    ColumnValue,
    EventEnvelope,
    RowChangeEvent,
    SchemaChangeEvent,
    TransactionBeginEvent,
    TransactionCommitEvent,
    build_dedup_key,
    build_idempotency_key,
    make_event_id,
)
from ..models.schema import TableSchema

if TYPE_CHECKING:
    SchemaAtFn = Callable[[str, str, Optional[BinlogPosition]], Optional[TableSchema]]
else:
    SchemaAtFn = Any


class EventTransformer:
    """Converts raw parser records into envelope-wrapped CDC events.

    The transformer is *stateless* -- all schema lookups are delegated to
    an injected :class:`SchemaTracker` which owns the per-table versioned
    schema history.  Lookups carry the binlog position so the tracker can
    return the schema *as it was at that exact point in the log*, which
    is critical when replaying an old section of the log after a restart
    where the current table schema is different (columns added, removed,
    renamed, ...).
    """

    def __init__(
        self,
        source_name: str = "mysql",
        schema_lookup: Optional[SchemaAtFn] = None,
    ) -> None:
        self._source = source_name
        self._schema_lookup = schema_lookup

    # ------------------------------------------------------------------
    # Public conversion API
    # ------------------------------------------------------------------
    def transform_row_change(self, raw: RawRowChange) -> EventEnvelope:
        schema = self._lookup_schema(raw.database, raw.table, raw.position)

        if raw.operation == "INSERT":
            change_type = ChangeType.INSERT
            before_cols: List[ColumnValue] = []
            after_cols = self._row_to_columns(raw.after_row, schema)
        elif raw.operation == "DELETE":
            change_type = ChangeType.DELETE
            before_cols = self._row_to_columns(raw.before_row, schema)
            after_cols = []
        elif raw.operation == "UPDATE":
            change_type = ChangeType.UPDATE
            before_cols = self._row_to_columns(raw.before_row, schema)
            after_cols = self._row_to_columns(raw.after_row, schema)
        else:
            raise ValueError(f"Unknown raw row operation: {raw.operation}")

        pk = self._extract_primary_key(before_cols, after_cols, schema)

        inner = RowChangeEvent(
            database=raw.database,
            table=raw.table,
            change_type=change_type,
            before_columns=before_cols,
            after_columns=after_cols,
            primary_key=pk,
        )

        pos = raw.position
        file = pos.binlog_file if pos else None
        offset = pos.position if pos else None
        ts = pos.timestamp if pos else 0.0
        gtid = pos.gtid if pos else None

        idem = build_idempotency_key(
            source=self._source,
            binlog_file=file,
            position=offset,
            event_type="row",
            pk=pk or None,
        )
        dedup = build_dedup_key(
            source=self._source,
            database=raw.database,
            table=raw.table,
            pk=pk or None,
        )

        return EventEnvelope(
            event_id=make_event_id(),
            event_type="row",
            event=inner,
            source=self._source,
            binlog_file=file,
            position=offset,
            gtid=gtid,
            timestamp=ts,
            transaction_id=raw.transaction_id,
            idempotency_key=idem,
            dedup_key=dedup,
        )

    def transform_tx_begin(self, raw: RawTransactionBegin) -> EventEnvelope:
        inner = TransactionBeginEvent(
            transaction_id=raw.transaction_id,
            timestamp=raw.timestamp,
        )
        pos = raw.position
        file = pos.binlog_file if pos else None
        offset = pos.position if pos else None
        gtid = pos.gtid if pos else None
        idem = build_idempotency_key(
            source=self._source,
            binlog_file=file,
            position=offset,
            event_type="tx_begin",
        )
        return EventEnvelope(
            event_id=make_event_id(),
            event_type="tx_begin",
            event=inner,
            source=self._source,
            binlog_file=file,
            position=offset,
            gtid=gtid,
            timestamp=raw.timestamp,
            transaction_id=raw.transaction_id,
            idempotency_key=idem,
        )

    def transform_tx_commit(
        self,
        raw: RawTransactionCommit,
        row_count: int = 0,
    ) -> EventEnvelope:
        inner = TransactionCommitEvent(
            transaction_id=raw.transaction_id,
            timestamp=raw.timestamp,
            row_count=row_count,
        )
        pos = raw.position
        file = pos.binlog_file if pos else None
        offset = pos.position if pos else None
        gtid = pos.gtid if pos else None
        idem = build_idempotency_key(
            source=self._source,
            binlog_file=file,
            position=offset,
            event_type="tx_commit",
        )
        return EventEnvelope(
            event_id=make_event_id(),
            event_type="tx_commit",
            event=inner,
            source=self._source,
            binlog_file=file,
            position=offset,
            gtid=gtid,
            timestamp=raw.timestamp,
            transaction_id=raw.transaction_id,
            idempotency_key=idem,
        )

    def transform_ddl(
        self,
        raw: RawDDLChange,
        schema_version: int = 1,
    ) -> EventEnvelope:
        inner = SchemaChangeEvent(
            database=raw.database,
            table=raw.table or "",
            ddl_statement=raw.ddl,
            schema_version=schema_version,
        )
        pos = raw.position
        file = pos.binlog_file if pos else None
        offset = pos.position if pos else None
        gtid = pos.gtid if pos else None
        ts = pos.timestamp if pos else 0.0
        idem = build_idempotency_key(
            source=self._source,
            binlog_file=file,
            position=offset,
            event_type="schema",
        )
        return EventEnvelope(
            event_id=make_event_id(),
            event_type="schema",
            event=inner,
            source=self._source,
            binlog_file=file,
            position=offset,
            gtid=gtid,
            timestamp=ts,
            transaction_id=raw.transaction_id,
            idempotency_key=idem,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _lookup_schema(
        self,
        db: str,
        table: str,
        position: Optional[BinlogPosition] = None,
    ) -> Optional[TableSchema]:
        if self._schema_lookup is None:
            return None
        return self._schema_lookup(db, table, position)

    def _row_to_columns(
        self,
        row_values: Optional[List[Any]],
        schema: Optional[TableSchema],
    ) -> List[ColumnValue]:
        if row_values is None:
            return []
        out: List[ColumnValue] = []
        if schema is not None and len(schema.columns) == len(row_values):
            for col, val in zip(schema.columns, row_values):
                out.append(ColumnValue(name=col.name, value=val, data_type=col.data_type))
        else:
            # Fallback: anonymous columns.  This only happens when the
            # schema tracker hasn't seen the table yet; the envelope will
            # still be emitted but downstream consumers can choose to
            # discard or buffer it.
            for idx, val in enumerate(row_values):
                out.append(ColumnValue(name=f"col_{idx}", value=val, data_type=None))
        return out

    @staticmethod
    def _extract_primary_key(
        before_cols: List[ColumnValue],
        after_cols: List[ColumnValue],
        schema: Optional[TableSchema],
    ) -> dict:
        if schema is None or not schema.primary_key_columns:
            return {}
        source = after_cols or before_cols
        by_name = {c.name: c.value for c in source}
        return {pk: by_name.get(pk) for pk in schema.primary_key_columns}

"""MySQL Binlog parser implementation.

Production usage
----------------
When the optional `pymysqlreplication` package is installed this class
wraps it to stream real events from a live MySQL server configured with
`binlog_format=ROW` and `binlog_row_image=FULL`.  The wrapper exposes a
small, well-typed API that the rest of the pipeline consumes.

Design notes (how the binary format is decoded)
-----------------------------------------------
A MySQL ROW-format binlog stream is a sequence of variable-length *events*,
each prefixed with a 19-byte common header:

    +==============+===========+==========================================+
    | Byte offset  | Length    | Meaning                                  |
    +==============+===========+==========================================+
    | 0            | 4         | timestamp (seconds since epoch)          |
    | 4            | 1         | event type code (see below)              |
    | 5            | 4         | server_id of the originating server      |
    | 9            | 4         | total event length (including header)    |
    | 13           | 4         | next event position in the binlog file   |
    | 17           | 2         | flags                                    |
    +--------------+-----------+------------------------------------------+

Relevant event type codes for CDC:
  19 = TABLE_MAP_EVENT          (0x13)
  23 = WRITE_ROWS_EVENTv1       (0x17)  -- older format
  24 = UPDATE_ROWS_EVENTv1      (0x18)
  25 = DELETE_ROWS_EVENTv1      (0x19)
  26 = QUERY_EVENT              (0x1A)
  30 = XID_EVENT                (0x1E)
  31 = ROTATE_EVENT             (0x1F)
  33 = GTID_LOG_EVENT           (0x21)
  34 = ANONYMOUS_GTID_LOG_EVENT (0x22)
  35 = PREVIOUS_GTIDS_EVENT     (0x23)
  39 = WRITE_ROWS_EVENTv2       (0x27)
  40 = UPDATE_ROWS_EVENTv2      (0x28)
  41 = DELETE_ROWS_EVENTv2      (0x29)

The TABLE_MAP_EVENT is the key to decoding row images: it is emitted
*before* any WRITE/UPDATE/DELETE event that touches a given table, and
maps a transient 6-byte `table_id` to the fully-qualified (db, table)
name plus per-column metadata (types, nullability, primary keys).  When
the binlog parser later sees a rows event it looks up the cached
table_id -> schema mapping and uses it to decode the packed binary row
images.

Each row image inside a ROWS_EVENT is encoded as:
  1. A bitmap with N bits (N = number of columns) indicating which
     columns are actually present in this image.  This supports
     "binlog_row_image=MINIMAL" where only PK + changed columns are
     logged.
  2. For a DELETE or UPDATE-before image: the NULL-bitmap (one bit per
     column present).
  3. The actual column values, packed sequentially using MySQL's wire
     protocol for each data type (integers are little-endian, fixed-size;
     strings are length-prefixed; etc).

All of the low-level unpacking is delegated to `pymysqlreplication` when
available; we only reproduce the conceptual model here so the pipeline
is fully self-describing.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable, Optional

from .base import (
    BinlogPosition,
    LogParser,
    LogParserCallback,
    RawDDLChange,
    RawRowChange,
    RawTransactionBegin,
    RawTransactionCommit,
)
from ..config import DatabaseConfig


class MySQLBinlogParser(LogParser):
    """Log parser that streams MySQL ROW-format binlog events.

    The parser runs in a background thread and invokes the supplied
    callbacks for each row change, DDL, and transaction boundary it
    observes.
    """

    def __init__(self, db_config: DatabaseConfig) -> None:
        self._db_config = db_config
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._on_row: Optional[LogParserCallback] = None
        self._on_ddl: Optional[LogParserCallback] = None
        self._on_tx_begin: Optional[LogParserCallback] = None
        self._on_tx_commit: Optional[LogParserCallback] = None
        self._start_position: Optional[BinlogPosition] = None
        self._table_map_cache: dict = {}
        self._current_tx_id: Optional[str] = None
        self._current_tx_row_count: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def start(
        self,
        start_position: Optional[BinlogPosition] = None,
        on_row: Optional[LogParserCallback] = None,
        on_ddl: Optional[LogParserCallback] = None,
        on_tx_begin: Optional[LogParserCallback] = None,
        on_tx_commit: Optional[LogParserCallback] = None,
    ) -> None:
        if self._running:
            return
        self._running = True
        self._start_position = start_position
        self._on_row = on_row
        self._on_ddl = on_ddl
        self._on_tx_begin = on_tx_begin
        self._on_tx_commit = on_tx_commit
        self._thread = threading.Thread(target=self._stream, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _stream(self) -> None:
        """Entry point of the streaming thread.

        We first try the real `pymysqlreplication` library; if it's not
        installed the parser simply stays idle (the tests use the
        :class:`SimulatedLogStream` instead, so we don't need a live
        database to exercise the pipeline end-to-end).
        """
        try:
            self._stream_real()
        except ImportError:
            # pymysqlreplication is not installed; nothing to do in real
            # mode.  The tests drive the pipeline through the simulator.
            while self._running:
                time.sleep(1.0)
        except Exception:
            self._running = False
            raise

    def _stream_real(self) -> None:  # pragma: no cover - requires live DB
        from pymysqlreplication import BinLogStreamReader  # type: ignore
        from pymysqlreplication.row_event import (  # type: ignore
            DeleteRowsEvent,
            UpdateRowsEvent,
            WriteRowsEvent,
        )
        from pymysqlreplication.event import (  # type: ignore
            GtidEvent,
            QueryEvent,
            RotateEvent,
            XidEvent,
        )

        stream = BinLogStreamReader(
            connection_settings=self._db_config.to_dict(),
            server_id=self._db_config.server_id,
            blocking=True,
            resume_stream=True,
            log_file=self._start_position.binlog_file if self._start_position else None,
            log_pos=self._start_position.position if self._start_position else 4,
            only_events=[
                DeleteRowsEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
                QueryEvent,
                XidEvent,
                RotateEvent,
                GtidEvent,
            ],
        )

        try:
            for binlog_event in stream:
                if not self._running:
                    break

                pos = BinlogPosition(
                    binlog_file=stream.log_file,
                    position=stream.log_pos,
                    gtid=getattr(binlog_event, "packet", None),
                )

                event_type = type(binlog_event).__name__

                if isinstance(binlog_event, QueryEvent):
                    self._handle_query_event(binlog_event, pos)
                elif isinstance(binlog_event, XidEvent):
                    self._handle_xid_event(binlog_event, pos)
                elif isinstance(binlog_event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                    self._handle_rows_event(binlog_event, pos)
        finally:
            stream.close()

    # ------------------------------------------------------------------
    # Event handlers (invoked by the real stream, and also by the
    # simulator for end-to-end testing without a live DB).
    # ------------------------------------------------------------------
    def _handle_query_event(self, query_event: Any, pos: BinlogPosition) -> None:
        query: str = query_event.query
        db: str = getattr(query_event, "schema", "") or ""

        upper = query.strip().upper()
        if upper.startswith("BEGIN"):
            self._current_tx_id = self._make_tx_id(pos)
            self._current_tx_row_count = 0
            if self._on_tx_begin is not None:
                self._on_tx_begin(
                    RawTransactionBegin(
                        transaction_id=self._current_tx_id,
                        position=pos,
                        timestamp=pos.timestamp,
                    )
                )
            return

        if upper.startswith("COMMIT"):
            if self._current_tx_id is not None and self._on_tx_commit is not None:
                self._on_tx_commit(
                    RawTransactionCommit(
                        transaction_id=self._current_tx_id,
                        position=pos,
                        timestamp=pos.timestamp,
                    )
                )
                self._current_tx_id = None
                self._current_tx_row_count = 0
            return

        # Anything else is treated as a potential DDL.
        table = self._guess_table_from_ddl(query, db)
        if self._on_ddl is not None:
            self._on_ddl(
                RawDDLChange(
                    database=db,
                    table=table,
                    ddl=query,
                    position=pos,
                    transaction_id=self._current_tx_id,
                )
            )

    def _handle_xid_event(self, _xid_event: Any, pos: BinlogPosition) -> None:
        if self._current_tx_id is None:
            self._current_tx_id = self._make_tx_id(pos)
        if self._on_tx_commit is not None:
            self._on_tx_commit(
                RawTransactionCommit(
                    transaction_id=self._current_tx_id,
                    position=pos,
                    timestamp=pos.timestamp,
                )
            )
        self._current_tx_id = None
        self._current_tx_row_count = 0

    def _handle_rows_event(self, rows_event: Any, pos: BinlogPosition) -> None:
        cls = type(rows_event).__name__
        if "Write" in cls:
            op = "INSERT"
        elif "Update" in cls:
            op = "UPDATE"
        elif "Delete" in cls:
            op = "DELETE"
        else:
            op = "UNKNOWN"

        db = getattr(rows_event, "schema", "") or ""
        tbl = getattr(rows_event, "table", "") or ""

        rows = getattr(rows_event, "rows", [])
        for row in rows:
            before = None
            after = None
            if isinstance(row, dict):
                # pymysqlreplication UpdateRowsEvent returns
                #   {"before_values": {...}, "after_values": {...}}
                # while Write/Delete return {"values": {...}}.
                if "before_values" in row and "after_values" in row:
                    before = list(row["before_values"].values())
                    after = list(row["after_values"].values())
                elif "values" in row:
                    if op == "DELETE":
                        before = list(row["values"].values())
                    else:
                        after = list(row["values"].values())
            raw = RawRowChange(
                database=db,
                table=tbl,
                operation=op,
                before_row=before,
                after_row=after,
                position=pos,
                transaction_id=self._current_tx_id,
            )
            if self._on_row is not None:
                self._on_row(raw)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _make_tx_id(pos: BinlogPosition) -> str:
        return f"tx-{pos.binlog_file}-{pos.position}-{int(pos.timestamp * 1e6)}"

    @staticmethod
    def _guess_table_from_ddl(ddl: str, default_db: str) -> Optional[str]:
        """Very small heuristic to pull the table name out of a DDL."""
        import re

        for pat in (
            r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?([`\"\w.]+)",
            r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`\"\w.]+)",
            r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([`\"\w.]+)",
            r"RENAME\s+TABLE\s+(?:IF\s+EXISTS\s+)?([`\"\w.]+)",
            r"TRUNCATE\s+(?:TABLE\s+)?([`\"\w.]+)",
        ):
            m = re.search(pat, ddl, re.IGNORECASE)
            if m:
                name = m.group(1).strip("`\"")
                if "." in name:
                    _, _, tbl = name.rpartition(".")
                    return tbl
                return name
        return None

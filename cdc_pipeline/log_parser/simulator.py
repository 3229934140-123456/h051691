"""In-memory binlog simulator - used for tests and the demo.

The simulator feeds a synthetic, fully-deterministic event stream through
the rest of the CDC pipeline exactly as if a real MySQL server were
producing it.  This lets us exercise every part of the system -- offset
tracking, schema evolution, transaction batching, idempotent delivery --
without needing a live database.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, List, Optional

from .base import (
    BinlogPosition,
    LogParser,
    LogParserCallback,
    RawDDLChange,
    RawRowChange,
    RawTransactionBegin,
    RawTransactionCommit,
)
from ..models.schema import TableSchema


@dataclass
class SimulatedEvent:
    """An event queued up in the simulator's timeline."""

    delay: float
    payload: Any


class SimulatedLogStream(LogParser):
    """A deterministic log-stream used by tests and the demo entry point.

    Usage::

        stream = SimulatedLogStream()
        stream.set_schema(db, table, schema)
        stream.begin_tx()
        stream.insert(db, table, after_row_values)
        stream.update(db, table, before, after)
        stream.delete(db, table, before)
        stream.commit_tx()
        stream.ddl(db, table, "ALTER TABLE ...")
        stream.start(...)
    """

    def __init__(self, events_per_second: float = 100.0) -> None:
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._queue: List[SimulatedEvent] = []
        self._current_file = "mysql-bin-changelog.000001"
        self._current_pos = 4
        self._current_gtid = None
        self._current_tx_id: Optional[str] = None
        self._schemas: dict = {}
        self._on_row: Optional[LogParserCallback] = None
        self._on_ddl: Optional[LogParserCallback] = None
        self._on_tx_begin: Optional[LogParserCallback] = None
        self._on_tx_commit: Optional[LogParserCallback] = None
        self._start_position: Optional[BinlogPosition] = None
        self._interval = 1.0 / max(events_per_second, 1.0)

    # ------------------------------------------------------------------
    # Simulator API - queue synthetic events
    # ------------------------------------------------------------------
    def set_schema(self, database: str, table: str, schema: TableSchema) -> None:
        self._schemas[(database, table)] = schema

    def begin_tx(self, delay: float = 0.0) -> None:
        self._advance_pos()
        tx_id = self._tx_id()
        self._current_tx_id = tx_id
        self._queue.append(
            SimulatedEvent(
                delay=delay,
                payload=RawTransactionBegin(
                    transaction_id=tx_id,
                    position=self._make_pos(),
                    timestamp=time.time(),
                ),
            )
        )

    def commit_tx(self, delay: float = 0.0) -> None:
        if self._current_tx_id is None:
            self.begin_tx()
        self._advance_pos()
        self._queue.append(
            SimulatedEvent(
                delay=delay,
                payload=RawTransactionCommit(
                    transaction_id=self._current_tx_id,
                    position=self._make_pos(),
                    timestamp=time.time(),
                ),
            )
        )
        self._current_tx_id = None

    def insert(
        self,
        database: str,
        table: str,
        after: List[Any],
        delay: float = 0.0,
    ) -> None:
        self._advance_pos()
        self._queue.append(
            SimulatedEvent(
                delay=delay,
                payload=RawRowChange(
                    database=database,
                    table=table,
                    operation="INSERT",
                    before_row=None,
                    after_row=list(after),
                    position=self._make_pos(),
                    transaction_id=self._current_tx_id,
                ),
            )
        )

    def update(
        self,
        database: str,
        table: str,
        before: List[Any],
        after: List[Any],
        delay: float = 0.0,
    ) -> None:
        self._advance_pos()
        self._queue.append(
            SimulatedEvent(
                delay=delay,
                payload=RawRowChange(
                    database=database,
                    table=table,
                    operation="UPDATE",
                    before_row=list(before),
                    after_row=list(after),
                    position=self._make_pos(),
                    transaction_id=self._current_tx_id,
                ),
            )
        )

    def delete(
        self,
        database: str,
        table: str,
        before: List[Any],
        delay: float = 0.0,
    ) -> None:
        self._advance_pos()
        self._queue.append(
            SimulatedEvent(
                delay=delay,
                payload=RawRowChange(
                    database=database,
                    table=table,
                    operation="DELETE",
                    before_row=list(before),
                    after_row=None,
                    position=self._make_pos(),
                    transaction_id=self._current_tx_id,
                ),
            )
        )

    def ddl(
        self,
        database: str,
        table: Optional[str],
        ddl_stmt: str,
        delay: float = 0.0,
    ) -> None:
        self._advance_pos()
        self._queue.append(
            SimulatedEvent(
                delay=delay,
                payload=RawDDLChange(
                    database=database,
                    table=table,
                    ddl=ddl_stmt,
                    position=self._make_pos(),
                    transaction_id=self._current_tx_id,
                ),
            )
        )

    def rotate(self, new_file: str, delay: float = 0.0) -> None:
        self._current_file = new_file
        self._current_pos = 4

    # ------------------------------------------------------------------
    # LogParser contract
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
        self._thread = threading.Thread(target=self._replay, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Replay loop
    # ------------------------------------------------------------------
    def _replay(self) -> None:
        idx = 0
        if self._start_position is not None:
            idx = self._find_event_index(self._start_position)
        while self._running:
            if idx >= len(self._queue):
                # drained - idle until new events arrive or stop() is called
                time.sleep(self._interval)
                continue
            evt = self._queue[idx]
            idx += 1
            if evt.delay > 0:
                time.sleep(evt.delay)
            self._dispatch(evt.payload)
            time.sleep(self._interval)

    def _find_event_index(self, pos: BinlogPosition) -> int:
        target = pos.as_tuple()
        for i, ev in enumerate(self._queue):
            payload_pos = getattr(ev.payload, "position", None)
            if payload_pos is not None and payload_pos.as_tuple() >= target:
                return i
        return len(self._queue)

    def _dispatch(self, payload: Any) -> None:
        if isinstance(payload, RawRowChange) and self._on_row is not None:
            self._on_row(payload)
        elif isinstance(payload, RawDDLChange) and self._on_ddl is not None:
            self._on_ddl(payload)
        elif isinstance(payload, RawTransactionBegin) and self._on_tx_begin is not None:
            self._on_tx_begin(payload)
        elif isinstance(payload, RawTransactionCommit) and self._on_tx_commit is not None:
            self._on_tx_commit(payload)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _advance_pos(self) -> None:
        self._current_pos += 128

    def _make_pos(self) -> BinlogPosition:
        return BinlogPosition(
            binlog_file=self._current_file,
            position=self._current_pos,
            gtid=self._current_gtid,
            timestamp=time.time(),
        )

    def _tx_id(self) -> str:
        return f"tx-{self._current_file}-{self._current_pos}-{int(time.time() * 1e6)}"

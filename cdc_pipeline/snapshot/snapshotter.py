"""Full snapshot implementation."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from ..config import PipelineConfig
from ..log_parser.base import BinlogPosition
from ..models.event import (
    ColumnValue,
    EventEnvelope,
    SnapshotEvent,
    build_idempotency_key,
    make_event_id,
)
from ..models.schema import TableSchema


_LOG = logging.getLogger("cdc.snapshot")


@dataclass
class SnapshotChunk:
    """A chunk of rows read from a single table during the snapshot."""

    schema: TableSchema
    rows: List[List[Any]]


class SnapshotDataProvider(ABC):
    """Abstract source of snapshot rows + schema metadata.

    A real implementation would talk to the source database and:
      * list all tables that match the include/exclude filters,
      * fetch their current schema from INFORMATION_SCHEMA,
      * execute a consistent-read ``SELECT *`` per table in chunks.
    """

    @abstractmethod
    def list_tables(self, config: PipelineConfig) -> List[Tuple[str, str]]:
        """Return the list of ``(database, table)`` pairs to snapshot."""
        raise NotImplementedError

    @abstractmethod
    def fetch_schema(self, database: str, table: str) -> TableSchema:
        raise NotImplementedError

    @abstractmethod
    def fetch_rows(
        self,
        database: str,
        table: str,
        schema: TableSchema,
        chunk_size: int,
    ) -> Iterable[List[List[Any]]]:
        """Yield chunks of rows (each row is a list of column values in
        column ordinal order)."""
        raise NotImplementedError

    @abstractmethod
    def acquire_consistent_snapshot_position(self) -> BinlogPosition:
        """Step 2 of the algorithm: return the consistent snapshot position.

        In MySQL this is obtained by calling ``SHOW MASTER STATUS`` inside
        a ``REPEATABLE READ WITH CONSISTENT SNAPSHOT`` transaction.
        """
        raise NotImplementedError


class InMemorySnapshotProvider(SnapshotDataProvider):
    """A snapshot provider that serves rows from an in-memory dictionary.

    Used by tests and the demo entry point.
    """

    def __init__(self) -> None:
        self._tables: Dict[Tuple[str, str], Tuple[TableSchema, List[List[Any]]]] = {}
        self._snapshot_position = BinlogPosition(
            binlog_file="mysql-bin-changelog.000001",
            position=1234,
            timestamp=time.time(),
        )

    # ------------------------------------------------------------------
    # Builder API (used by tests to populate the fake dataset)
    # ------------------------------------------------------------------
    def add_table(self, schema: TableSchema, rows: List[List[Any]]) -> None:
        self._tables[(schema.database, schema.table)] = (schema, rows)

    def set_snapshot_position(self, pos: BinlogPosition) -> None:
        self._snapshot_position = pos

    # ------------------------------------------------------------------
    # SnapshotDataProvider contract
    # ------------------------------------------------------------------
    def list_tables(self, config: PipelineConfig) -> List[Tuple[str, str]]:
        return [
            (db, tbl)
            for (db, tbl) in self._tables.keys()
            if config.should_include_table(db, tbl)
        ]

    def fetch_schema(self, database: str, table: str) -> TableSchema:
        schema, _ = self._tables[(database, table)]
        return schema

    def fetch_rows(
        self,
        database: str,
        table: str,
        schema: TableSchema,
        chunk_size: int,
    ) -> Iterable[List[List[Any]]]:
        _, rows = self._tables[(database, table)]
        for i in range(0, len(rows), chunk_size):
            yield rows[i : i + chunk_size]

    def acquire_consistent_snapshot_position(self) -> BinlogPosition:
        return self._snapshot_position


class Snapshotter:
    """Orchestrates the full snapshot phase.

    The orchestration follows the 5-step algorithm described in the
    module docstring.  Steps 3 (row delivery) and 5 (switch to
    incremental) are performed collaboratively with the caller: the
    snapshotter yields snapshot rows through a callback and returns the
    consistent snapshot position; the caller is then responsible for
    starting the incremental binlog stream from that exact position.
    """

    def __init__(
        self,
        config: PipelineConfig,
        provider: SnapshotDataProvider,
        source_name: str = "mysql",
    ) -> None:
        self._config = config
        self._provider = provider
        self._source = source_name

    def run(
        self,
        on_row: Callable[[EventEnvelope], None],
        on_schema: Callable[[TableSchema, BinlogPosition], None],
    ) -> BinlogPosition:
        """Execute the full snapshot.

        Parameters
        ----------
        on_row:
            Called once per snapshot row with the envelope-wrapped
            :class:`SnapshotEvent`.  The caller forwards these to the
            delivery manager, which batches and delivers them.
        on_schema:
            Called once per table with the captured schema and the
            snapshot position so the schema tracker can install it.

        Returns
        -------
        The *consistent snapshot position* -- the exact binlog position
        that the incremental stream must start from after the snapshot
        completes so the overlap is correctly deduplicated.
        """
        _LOG.info("Starting initial full snapshot")
        snap_pos = self._provider.acquire_consistent_snapshot_position()
        _LOG.info(
            "Consistent snapshot position acquired: %s:%d",
            snap_pos.binlog_file,
            snap_pos.position,
        )

        tables = self._provider.list_tables(self._config)
        _LOG.info("Snapshotting %d table(s)", len(tables))

        for db, tbl in tables:
            schema = self._provider.fetch_schema(db, tbl)
            on_schema(schema, snap_pos)
            _LOG.info(
                "  -> %s.%s (%d column(s), pk=%s)",
                db,
                tbl,
                len(schema.columns),
                schema.primary_key_columns,
            )

            total = 0
            for chunk in self._provider.fetch_rows(
                db, tbl, schema, self._config.snapshot_chunk_size
            ):
                envelopes: List[EventEnvelope] = []
                for row_values in chunk:
                    total += 1
                    columns = [
                        ColumnValue(name=c.name, value=v, data_type=c.data_type)
                        for c, v in zip(schema.columns, row_values)
                    ]
                    pk = schema.primary_key_values_from_row(
                        {c.name: c.value for c in columns}
                    )
                    inner = SnapshotEvent(
                        database=db,
                        table=tbl,
                        columns=columns,
                        primary_key=pk,
                    )
                    idem = build_idempotency_key(
                        source=self._source,
                        binlog_file=snap_pos.binlog_file,
                        position=snap_pos.position,
                        event_type="snapshot",
                        pk=pk,
                    )
                    env = EventEnvelope(
                        event_id=make_event_id(),
                        event_type="snapshot",
                        event=inner,
                        source=self._source,
                        binlog_file=snap_pos.binlog_file,
                        position=snap_pos.position,
                        gtid=snap_pos.gtid,
                        timestamp=snap_pos.timestamp,
                        transaction_id=None,
                        idempotency_key=idem,
                    )
                    on_row(env)
            _LOG.info("     %d row(s) snapshotted", total)

        _LOG.info("Snapshot complete. Incremental stream will resume at %s:%d",
                  snap_pos.binlog_file, snap_pos.position)
        return snap_pos

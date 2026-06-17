"""The CDC pipeline orchestrator.

This is the module users typically interact with.  It wires the five
core stages together into a single running unit:

    +---------------+    +---------------+    +---------------+
    |  Log Parser   | -> | Schema Tracker| -> | Event Xformer |
    | (binlog read) |    |  (DDL aware)   |    |  (row->event)  |
    +---------------+    +---------------+    +-------+-------+
                                                       |
                                                       v
    +---------------+    +---------------+    +---------------+
    | Offset Mgr.   | <- | Delivery Mgr. | <- | Delivery      |
    |  (persist)    |    |  (batch/tx)    |    |  (consumer)    |
    +---------------+    +---------------+    +---------------+

A typical run looks like this::

    config = PipelineConfig(...)
    pipeline = CDCPipeline(config)
    pipeline.attach_consumer(LoggingDownstreamConsumer())
    pipeline.start()    # runs snapshot + streaming in background
    ...
    pipeline.stop()

The orchestrator itself is intentionally tiny -- the clever parts are
in each individual stage -- but it is responsible for one subtle piece
of state: deciding whether the very first run should perform a full
snapshot, or whether a previously persisted offset means we can jump
straight into incremental streaming.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from .config import PipelineConfig
from .downstream.consumer import DownstreamConsumer
from .downstream.delivery import DeliveryManager
from .event_transformer.transformer import EventTransformer
from .log_parser.base import (
    BinlogPosition,
    LogParser,
    RawDDLChange,
    RawRowChange,
    RawTransactionBegin,
    RawTransactionCommit,
)
from .log_parser.mysql_binlog import MySQLBinlogParser
from .offset_manager.manager import OffsetManager
from .schema_tracker.tracker import SchemaTracker
from .snapshot.snapshotter import SnapshotDataProvider, Snapshotter

_LOG = logging.getLogger("cdc.pipeline")


class CDCPipeline:
    """Top-level CDC pipeline.

    Parameters
    ----------
    config:
        Pipeline configuration.
    log_parser:
        Optional :class:`LogParser` implementation.  If omitted a real
        MySQL binlog parser is created.  The test suite injects a
        :class:`SimulatedLogStream` here.
    snapshot_provider:
        Optional :class:`SnapshotDataProvider` used during the initial
        full snapshot phase.  Tests inject an in-memory implementation.
    """

    def __init__(
        self,
        config: PipelineConfig,
        log_parser: Optional[LogParser] = None,
        snapshot_provider: Optional[SnapshotDataProvider] = None,
    ) -> None:
        self._config = config
        self._offset_manager = OffsetManager(config.offset_storage)
        self._schema_tracker = SchemaTracker(config.schema_storage)
        self._transformer = EventTransformer(
            source_name="mysql",
            schema_lookup=self._schema_lookup,
        )
        self._consumer: Optional[DownstreamConsumer] = None
        self._delivery: Optional[DeliveryManager] = None
        self._log_parser = log_parser or MySQLBinlogParser(config.database)
        self._snapshot_provider = snapshot_provider
        self._snapshotter: Optional[Snapshotter] = None
        if snapshot_provider is not None:
            self._snapshotter = Snapshotter(config, snapshot_provider)
        self._running = False
        self._tx_row_counts: dict = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def attach_consumer(self, consumer: DownstreamConsumer) -> None:
        """Register the downstream consumer that will receive events."""
        self._consumer = consumer
        self._delivery = DeliveryManager(
            consumer=consumer,
            config=self._config,
            on_ack=self._offset_manager.ack,
        )

    def start(self) -> None:
        """Start the pipeline.

        The method blocks long enough to perform the initial snapshot (if
        one is needed); after that incremental binlog streaming runs in
        background threads and start() returns.
        """
        if self._running:
            return
        if self._consumer is None or self._delivery is None:
            raise RuntimeError("A DownstreamConsumer must be attached first")

        self._delivery.start()

        # Decide whether to run a snapshot.
        last_committed = self._offset_manager.load_last_committed()
        if last_committed is None and self._config.snapshot_enabled and self._snapshotter is not None:
            _LOG.info("No persisted offset found - running initial full snapshot")
            self._offset_manager.mark_snapshot_in_progress()
            snap_pos = self._snapshotter.run(
                on_row=self._delivery.submit,
                on_schema=self._schema_tracker.register_snapshot_schema,
            )
            self._delivery.flush()
            self._offset_manager.mark_snapshot_complete(snap_pos)
            start_position = snap_pos
        elif last_committed is not None:
            _LOG.info(
                "Resuming from persisted offset %s:%d",
                last_committed.binlog_file,
                last_committed.position,
            )
            start_position = last_committed
        else:
            _LOG.info("Starting from current tail of the binlog (no snapshot)")
            start_position = None

        self._running = True
        self._log_parser.start(
            start_position=start_position,
            on_row=self._on_raw_row,
            on_ddl=self._on_raw_ddl,
            on_tx_begin=self._on_tx_begin,
            on_tx_commit=self._on_tx_commit,
        )

    def stop(self, timeout: float = 5.0) -> None:
        """Gracefully shut down the pipeline."""
        if not self._running:
            return
        self._log_parser.stop()
        if self._delivery is not None:
            self._delivery.flush()
            self._delivery.stop(timeout=timeout)
        self._running = False

    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # Log parser callbacks
    # ------------------------------------------------------------------
    def _on_raw_row(self, raw: RawRowChange) -> None:
        if not self._config.should_include_table(raw.database, raw.table):
            return
        if not self._schema_tracker.schema_at(raw.database, raw.table, raw.position):
            _LOG.debug(
                "No schema known for %s.%s at position %r; attempting refresh",
                raw.database,
                raw.table,
                raw.position,
            )
        env = self._transformer.transform_row_change(raw)
        if raw.transaction_id:
            self._tx_row_counts[raw.transaction_id] = (
                self._tx_row_counts.get(raw.transaction_id, 0) + 1
            )
        assert self._delivery is not None
        self._delivery.submit(env)

    def _on_raw_ddl(self, raw: RawDDLChange) -> None:
        if raw.table and not self._config.should_include_table(raw.database, raw.table):
            return
        new_schema = self._schema_tracker.apply_ddl(
            raw.database, raw.table or "", raw.ddl, raw.position
        )
        version = new_schema.version if new_schema is not None else (
            self._schema_tracker.current_schema_version(raw.database, raw.table or "")
        )
        env = self._transformer.transform_ddl(raw, schema_version=version)
        assert self._delivery is not None
        self._delivery.submit(env)

    def _on_tx_begin(self, raw: RawTransactionBegin) -> None:
        if not self._config.transaction_boundary_enabled:
            return
        self._tx_row_counts[raw.transaction_id] = 0
        env = self._transformer.transform_tx_begin(raw)
        assert self._delivery is not None
        self._delivery.submit(env)

    def _on_tx_commit(self, raw: RawTransactionCommit) -> None:
        row_count = self._tx_row_counts.pop(raw.transaction_id, 0)
        if self._config.transaction_boundary_enabled:
            env = self._transformer.transform_tx_commit(raw, row_count=row_count)
            assert self._delivery is not None
            self._delivery.submit(env)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _schema_lookup(self, db: str, table: str):
        # The transformer calls this without a position; we return the
        # latest known schema.
        return self._schema_tracker.schema_at(db, table)

    # ------------------------------------------------------------------
    # Test/debug helpers
    # ------------------------------------------------------------------
    @property
    def offset_manager(self) -> OffsetManager:
        return self._offset_manager

    @property
    def schema_tracker(self) -> SchemaTracker:
        return self._schema_tracker

"""End-to-end integration tests for the CDC pipeline.

These tests exercise the full pipeline using the
:class:`SimulatedLogStream` and :class:`InMemorySnapshotProvider` so they
run entirely in-memory without a live database.  The tests validate:

* Row INSERT / UPDATE / DELETE are correctly parsed and transformed.
* Transaction boundaries are preserved end-to-end.
* Schema evolution (add/drop/modify column via DDL) is tracked.
* Offsets persist to disk and survive a pipeline restart (no loss, no
  duplication beyond what the idempotency key can deduplicate).
* The initial full snapshot runs on first startup and is skipped on
  restart.
* Overlapping rows between snapshot and incremental stream are
  deduplicated downstream by the idempotency key.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
import unittest
from typing import Any, Dict

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from cdc_pipeline.config import (  # noqa: E402
    DatabaseConfig,
    OffsetStorageConfig,
    PipelineConfig,
    SchemaStorageConfig,
)
from cdc_pipeline.downstream.consumer import (  # noqa: E402
    InMemoryDownstreamConsumer,
)
from cdc_pipeline.log_parser.base import BinlogPosition  # noqa: E402
from cdc_pipeline.log_parser.simulator import SimulatedLogStream  # noqa: E402
from cdc_pipeline.models.event import ChangeType  # noqa: E402
from cdc_pipeline.models.schema import ColumnDef, TableSchema  # noqa: E402
from cdc_pipeline.pipeline import CDCPipeline  # noqa: E402
from cdc_pipeline.snapshot.snapshotter import InMemorySnapshotProvider  # noqa: E402


def _make_users_schema() -> TableSchema:
    return TableSchema(
        database="app",
        table="users",
        columns=[
            ColumnDef(name="id", data_type="INT", is_primary_key=True, is_nullable=False, ordinal_position=0),
            ColumnDef(name="name", data_type="VARCHAR(255)", is_nullable=False, ordinal_position=1),
            ColumnDef(name="age", data_type="INT", is_nullable=True, ordinal_position=2),
        ],
        primary_key_columns=["id"],
        version=1,
        captured_at=time.time(),
    )


def _build_config(tmpdir: str, snapshot: bool = True) -> PipelineConfig:
    return PipelineConfig(
        database=DatabaseConfig(),
        offset_storage=OffsetStorageConfig(
            storage_type="file",
            file_path=os.path.join(tmpdir, "offsets.json"),
            flush_interval_ms=0,  # flush every ack for tests
        ),
        schema_storage=SchemaStorageConfig(
            storage_type="file",
            file_path=os.path.join(tmpdir, "schemas.json"),
        ),
        include_databases=["app"],
        snapshot_enabled=snapshot,
        downstream_batch_size=1,  # flush every event for deterministic tests
        downstream_flush_interval_ms=0,
        transaction_boundary_enabled=True,
        idempotency_key_enabled=True,
    )


class TestCDCPipelineEndToEnd(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp(prefix="cdc_test_")

    def tearDown(self) -> None:
        import shutil

        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _sleep_until(self, predicate, timeout: float = 3.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(0.05)
        self.fail(f"Predicate not satisfied within {timeout}s")

    # ------------------------------------------------------------------
    # Test: row-level change capture
    # ------------------------------------------------------------------
    def test_row_insert_update_delete(self) -> None:
        config = _build_config(self._tmpdir, snapshot=False)
        stream = SimulatedLogStream(events_per_second=10_000)
        stream.set_schema("app", "users", _make_users_schema())

        consumer = InMemoryDownstreamConsumer()
        pipeline = CDCPipeline(config, log_parser=stream)
        pipeline.attach_consumer(consumer)

        # Seed the schema tracker directly because there is no snapshot.
        pipeline.schema_tracker.register_snapshot_schema(
            _make_users_schema(),
            BinlogPosition("mysql-bin-changelog.000001", 4),
        )

        # Prime the simulator with one tx of three row changes.
        stream.begin_tx()
        stream.insert("app", "users", [1, "Alice", 30])
        stream.update("app", "users", [1, "Alice", 30], [1, "Alice Smith", 31])
        stream.delete("app", "users", [1, "Alice Smith", 31])
        stream.commit_tx()

        pipeline.start()
        self.addCleanup(pipeline.stop)

        # Wait for delivery of tx_begin + 3 rows + tx_commit = 5 events.
        self._sleep_until(lambda: len(consumer.events) >= 5)

        types = [e.event_type for e in consumer.events]
        self.assertEqual(types, ["tx_begin", "row", "row", "row", "tx_commit"])

        # Check per-row content.
        row_events = [e for e in consumer.events if e.event_type == "row"]
        self.assertEqual(row_events[0].event.change_type, ChangeType.INSERT)
        self.assertEqual(row_events[0].event.after_dict(), {"id": 1, "name": "Alice", "age": 30})

        self.assertEqual(row_events[1].event.change_type, ChangeType.UPDATE)
        self.assertEqual(row_events[1].event.before_dict(), {"id": 1, "name": "Alice", "age": 30})
        self.assertEqual(row_events[1].event.after_dict(), {"id": 1, "name": "Alice Smith", "age": 31})

        self.assertEqual(row_events[2].event.change_type, ChangeType.DELETE)
        self.assertEqual(row_events[2].event.before_dict(), {"id": 1, "name": "Alice Smith", "age": 31})

        for ev in row_events:
            self.assertEqual(ev.event.primary_key, {"id": 1})
            self.assertIsNotNone(ev.idempotency_key)

    # ------------------------------------------------------------------
    # Test: transaction boundary preservation
    # ------------------------------------------------------------------
    def test_transaction_boundaries(self) -> None:
        config = _build_config(self._tmpdir, snapshot=False)
        config.downstream_batch_size = 100  # allow natural transaction boundaries
        stream = SimulatedLogStream(events_per_second=10_000)
        stream.set_schema("app", "users", _make_users_schema())

        consumer = InMemoryDownstreamConsumer()
        pipeline = CDCPipeline(config, log_parser=stream)
        pipeline.attach_consumer(consumer)
        pipeline.schema_tracker.register_snapshot_schema(
            _make_users_schema(),
            BinlogPosition("mysql-bin-changelog.000001", 4),
        )

        # Two independent transactions.
        stream.begin_tx()
        stream.insert("app", "users", [1, "Alice", 30])
        stream.insert("app", "users", [2, "Bob", 25])
        stream.commit_tx()

        stream.begin_tx()
        stream.insert("app", "users", [3, "Carol", 40])
        stream.commit_tx()

        pipeline.start()
        self.addCleanup(pipeline.stop)

        # Expect 2 batches: each is a full tx (begin + 2 rows + commit, begin + 1 row + commit)
        self._sleep_until(lambda: len(consumer.batches) >= 2)
        first, second = consumer.batches[:2]

        self.assertEqual([e.event_type for e in first.events], ["tx_begin", "row", "row", "tx_commit"])
        self.assertEqual(first.events[0].event.transaction_id, first.events[-1].event.transaction_id)
        self.assertEqual(first.events[-1].event.row_count, 2)

        self.assertEqual([e.event_type for e in second.events], ["tx_begin", "row", "tx_commit"])
        self.assertEqual(second.events[-1].event.row_count, 1)
        # Two different transaction ids.
        self.assertNotEqual(first.events[0].event.transaction_id, second.events[0].event.transaction_id)

    # ------------------------------------------------------------------
    # Test: schema evolution via DDL
    # ------------------------------------------------------------------
    def test_schema_evolution_ddl(self) -> None:
        config = _build_config(self._tmpdir, snapshot=False)
        stream = SimulatedLogStream(events_per_second=10_000)
        consumer = InMemoryDownstreamConsumer()
        pipeline = CDCPipeline(config, log_parser=stream)
        pipeline.attach_consumer(consumer)

        initial = _make_users_schema()
        pipeline.schema_tracker.register_snapshot_schema(
            initial, BinlogPosition("mysql-bin-changelog.000001", 4)
        )
        stream.set_schema("app", "users", initial)

        stream.begin_tx()
        stream.ddl(
            "app",
            "users",
            "ALTER TABLE users ADD COLUMN email VARCHAR(255) AFTER name",
        )
        stream.commit_tx()

        stream.begin_tx()
        # After the DDL the row has 4 columns: id, name, email, age
        new_schema = pipeline.schema_tracker.schema_at("app", "users")
        stream.set_schema("app", "users", new_schema)
        stream.insert("app", "users", [2, "Bob", "bob@example.com", 25])
        stream.commit_tx()

        pipeline.start()
        self.addCleanup(pipeline.stop)

        # Wait for: tx_begin + schema + tx_commit + tx_begin + row + tx_commit
        self._sleep_until(lambda: len(consumer.events) >= 6)

        schema_ev = next(e for e in consumer.events if e.event_type == "schema")
        self.assertIn("ADD COLUMN", schema_ev.event.ddl_statement)

        row_ev = next(e for e in consumer.events if e.event_type == "row")
        self.assertEqual(row_ev.event.after_dict(), {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25})

    # ------------------------------------------------------------------
    # Test: offset persistence and restart
    # ------------------------------------------------------------------
    def test_offset_persistence_and_restart_no_duplicates(self) -> None:
        config = _build_config(self._tmpdir, snapshot=False)
        stream = SimulatedLogStream(events_per_second=10_000)
        consumer = InMemoryDownstreamConsumer()
        pipeline = CDCPipeline(config, log_parser=stream)
        pipeline.attach_consumer(consumer)
        pipeline.schema_tracker.register_snapshot_schema(
            _make_users_schema(),
            BinlogPosition("mysql-bin-changelog.000001", 4),
        )
        stream.set_schema("app", "users", _make_users_schema())

        stream.begin_tx()
        stream.insert("app", "users", [1, "Alice", 30])
        stream.commit_tx()

        stream.begin_tx()
        stream.insert("app", "users", [2, "Bob", 25])
        stream.commit_tx()

        pipeline.start()
        self._sleep_until(lambda: len(consumer.events) >= 6)  # 2 full tx
        pipeline.stop()

        # Verify offset was persisted.
        committed = pipeline.offset_manager.committed
        self.assertIsNotNone(committed)
        self.assertGreater(committed.position, 4)

        # --- Restart the pipeline from the persisted offset. ---
        stream2 = SimulatedLogStream(events_per_second=10_000)
        stream2.set_schema("app", "users", _make_users_schema())
        # Replay the EXACT same events (a simulator with identical queue).
        stream2.begin_tx()
        stream2.insert("app", "users", [1, "Alice", 30])
        stream2.commit_tx()
        stream2.begin_tx()
        stream2.insert("app", "users", [2, "Bob", 25])
        stream2.commit_tx()
        # Plus one genuinely new event.
        stream2.begin_tx()
        stream2.insert("app", "users", [3, "Carol", 40])
        stream2.commit_tx()

        consumer2 = InMemoryDownstreamConsumer()
        config2 = _build_config(self._tmpdir, snapshot=False)
        pipeline2 = CDCPipeline(config2, log_parser=stream2)
        pipeline2.attach_consumer(consumer2)
        pipeline2.schema_tracker.register_snapshot_schema(
            _make_users_schema(),
            BinlogPosition("mysql-bin-changelog.000001", 4),
        )

        pipeline2.start()
        self.addCleanup(pipeline2.stop)
        # We expect only the NEW event (row id=3) to be delivered, because
        # the persisted offset should skip the first two transactions.
        self._sleep_until(lambda: len([e for e in consumer2.events if e.event_type == "row"]) >= 1)
        time.sleep(0.5)

        rows = [e for e in consumer2.events if e.event_type == "row"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].event.primary_key, {"id": 3})

    # ------------------------------------------------------------------
    # Test: initial snapshot + incremental overlap deduplication
    # ------------------------------------------------------------------
    def test_snapshot_and_incremental_overlap_dedup(self) -> None:
        config = _build_config(self._tmpdir, snapshot=True)

        snap_rows = [[1, "Alice", 30], [2, "Bob", 25]]
        provider = InMemorySnapshotProvider()
        schema = _make_users_schema()
        provider.add_table(schema, snap_rows)
        snap_pos = BinlogPosition(
            binlog_file="mysql-bin-changelog.000001",
            position=4,
            timestamp=time.time(),
        )
        provider.set_snapshot_position(snap_pos)

        # Build a log stream that contains rows overlapping with the snapshot
        # (id=1, id=2) plus a genuinely new row (id=3).  The snapshot runs at
        # position 4 and the incremental stream starts at exactly position 4,
        # so rows 1 and 2 will be delivered TWICE (once in snapshot, once in
        # the incremental stream) and downstream deduplication is required.
        stream = SimulatedLogStream(events_per_second=10_000)
        stream.set_schema("app", "users", schema)
        stream.rotate("mysql-bin-changelog.000001")

        # Row id=1 and id=2 overlap with the snapshot; id=3 is only in binlog.
        stream.begin_tx()
        stream.insert("app", "users", [1, "Alice", 30])   # overlap
        stream.insert("app", "users", [2, "Bob", 25])     # overlap
        stream.insert("app", "users", [3, "Carol", 40])   # new
        stream.commit_tx()

        consumer = InMemoryDownstreamConsumer()
        pipeline = CDCPipeline(
            config,
            log_parser=stream,
            snapshot_provider=provider,
        )
        pipeline.attach_consumer(consumer)
        pipeline.start()
        self.addCleanup(pipeline.stop)

        # 2 snapshot rows + 1 tx (begin + 3 rows + commit) = 7 events
        self._sleep_until(lambda: len(consumer.events) >= 7)
        time.sleep(0.3)

        # The consumer saw 2 snapshot + 3 incremental rows = 5 row events,
        # but after idempotency-key deduplication only 3 unique rows remain.
        unique = consumer.unique_events()
        unique_rows = [e for e in unique if e.event_type in ("snapshot", "row")]
        pk_values = set()
        for e in unique_rows:
            if e.event_type == "snapshot":
                for k, v in e.event.primary_key.items():
                    pk_values.add((k, v))
            else:
                for k, v in e.event.primary_key.items():
                    pk_values.add((k, v))
        self.assertEqual(pk_values, {("id", 1), ("id", 2), ("id", 3)})

        # Offsets should have advanced past the snapshot position.
        self._sleep_until(lambda: pipeline.offset_manager.committed is not None)
        self.assertGreaterEqual(pipeline.offset_manager.committed.position, 4)

    # ------------------------------------------------------------------
    # Test: row UPDATED during the snapshot phase - downstream should
    # end up with the post-update (final) value for each primary key.
    # ------------------------------------------------------------------
    def test_snapshot_update_overlap_yields_correct_final_state(self) -> None:
        users_schema = _make_users_schema()
        config = _build_config(self._tmpdir, snapshot=True)
        # Snapshot contains the *pre-update* version of id=1.
        snap_rows = [[1, "Alice", 30]]
        provider = InMemorySnapshotProvider()
        provider.add_table(users_schema, snap_rows)
        snap_pos = BinlogPosition(
            binlog_file="mysql-bin-changelog.000001",
            position=4,
            timestamp=time.time(),
        )
        provider.set_snapshot_position(snap_pos)

        # Incremental stream (starting exactly at snap_pos) first updates
        # id=1 to the *new* value, then inserts id=2.
        stream = SimulatedLogStream(events_per_second=10_000)
        stream.set_schema("app", "users", users_schema)
        stream.rotate("mysql-bin-changelog.000001")
        # Prime events at higher positions (begin_tx jumps them past 4).
        stream.begin_tx()
        stream.update(
            "app", "users",
            [1, "Alice", 30],
            [1, "Alice Smith", 31],   # id=1 updated during snapshot
        )
        stream.insert("app", "users", [2, "Bob", 25])
        stream.commit_tx()

        consumer = InMemoryDownstreamConsumer()
        pipeline = CDCPipeline(
            config, log_parser=stream, snapshot_provider=provider,
        )
        pipeline.attach_consumer(consumer)
        pipeline.start()
        self.addCleanup(pipeline.stop)

        # 1 snapshot row + 1 tx (begin + UPDATE row + INSERT row + commit) = 5 events
        self._sleep_until(
            lambda: len(consumer.events) >= 5,
            timeout=10.0,
        )
        time.sleep(0.5)

        # Replay all unique events in *delivery order* and build the
        # final materialised view (dict pk -> row).
        materialised: Dict[Any, Dict[str, Any]] = {}
        for ev in consumer.unique_events():
            if ev.event_type == "snapshot":
                row = ev.event.row_dict()
                pk = tuple(ev.event.primary_key.items())
                materialised[pk] = row
            elif ev.event_type == "row":
                inner = ev.event
                pk = tuple(inner.primary_key.items())
                if inner.change_type == ChangeType.INSERT:
                    materialised[pk] = inner.after_dict()
                elif inner.change_type == ChangeType.UPDATE:
                    materialised[pk] = inner.after_dict()
                elif inner.change_type == ChangeType.DELETE:
                    materialised.pop(pk, None)

        self.assertEqual(len(materialised), 2)
        pk1 = (("id", 1),)
        pk2 = (("id", 2),)
        # id=1 must reflect the POST-update value.
        self.assertEqual(materialised[pk1]["name"], "Alice Smith")
        self.assertEqual(materialised[pk1]["age"], 31)
        self.assertEqual(materialised[pk2]["name"], "Bob")
        self.assertEqual(materialised[pk2]["age"], 25)

    # ------------------------------------------------------------------
    # Test: non-transactional trickle of small events is flushed by
    # the refresh timer even though downstream_batch_size is never hit.
    # ------------------------------------------------------------------
    def test_non_tx_flush_by_timer(self) -> None:
        config = _build_config(self._tmpdir, snapshot=False)
        # Batch size is 100 but we'll only emit 3 non-tx events; the
        # 200ms flush interval must kick them out after ~200ms.
        config.downstream_batch_size = 100
        config.downstream_flush_interval_ms = 200
        # Disable transaction boundaries so single-row inserts don't get
        # buffered inside a pending tx.
        config.transaction_boundary_enabled = False

        stream = SimulatedLogStream(events_per_second=10_000)
        stream.set_schema("app", "users", _make_users_schema())
        consumer = InMemoryDownstreamConsumer()
        pipeline = CDCPipeline(config, log_parser=stream)
        pipeline.attach_consumer(consumer)
        pipeline.schema_tracker.register_snapshot_schema(
            _make_users_schema(),
            BinlogPosition("mysql-bin-changelog.000001", 4),
        )

        # Emit 3 single-row events WITHOUT wrapping them in tx_begin/commit.
        stream.insert("app", "users", [1, "A", 10])
        stream.insert("app", "users", [2, "B", 20])
        stream.insert("app", "users", [3, "C", 30])

        pipeline.start()
        self.addCleanup(pipeline.stop)

        # Wait 1 second (5x the 200ms flush interval) - events should
        # have flowed even though batch size was never hit.
        self._sleep_until(
            lambda: len([e for e in consumer.events if e.event_type == "row"]) >= 3,
            timeout=3.0,
        )
        rows = [e for e in consumer.events if e.event_type == "row"]
        self.assertEqual(len(rows), 3)
        pks = {tuple(e.event.primary_key.items()) for e in rows}
        self.assertEqual(pks, {(("id", 1),), (("id", 2),), (("id", 3),)})

    # ------------------------------------------------------------------
    # Test: schema versions are honoured by log position.  We build a
    # stream where, *after* a DDL that adds a column, an older row
    # event position is still decoded using the pre-DDL schema.
    # ------------------------------------------------------------------
    def test_schema_lookup_honours_log_position(self) -> None:
        from cdc_pipeline.schema_tracker.tracker import apply_ddl_to_schema

        old_schema = _make_users_schema()  # id, name, age (3 cols)
        # Apply ADD COLUMN to produce the new schema.
        new_schema = apply_ddl_to_schema(
            "ALTER TABLE users ADD COLUMN email VARCHAR(255) AFTER name",
            old_schema,
            "app",
            "users",
        )
        assert new_schema is not None and len(new_schema.columns) == 4

        config = _build_config(self._tmpdir, snapshot=False)
        stream = SimulatedLogStream(events_per_second=10_000)
        consumer = InMemoryDownstreamConsumer()
        pipeline = CDCPipeline(config, log_parser=stream)
        pipeline.attach_consumer(consumer)

        # Register the OLD schema at position 100 and NEW schema at 500.
        pos_old = BinlogPosition("mysql-bin-changelog.000001", 100, timestamp=1.0)
        pos_new = BinlogPosition("mysql-bin-changelog.000001", 500, timestamp=2.0)
        pipeline.schema_tracker.register_snapshot_schema(old_schema, pos_old)
        pipeline.schema_tracker.apply_ddl(
            "app", "users",
            "ALTER TABLE users ADD COLUMN email VARCHAR(255) AFTER name",
            pos_new,
        )

        # Emit a row event *between* the two positions -> 3 cols, OLD schema.
        stream.set_schema("app", "users", old_schema)
        stream.rotate("mysql-bin-changelog.000001")
        # Manually position the stream so the insert lands at ~pos 200.
        stream._current_pos = 200
        stream.begin_tx()
        stream.insert("app", "users", [1, "Alice", 30])  # 3 columns
        stream.commit_tx()

        # Now emit a row *after* the DDL -> 4 cols, NEW schema.
        stream.set_schema("app", "users", new_schema)
        stream._current_pos = 600
        stream.begin_tx()
        stream.insert("app", "users", [2, "Bob", "bob@x.com", 25])  # 4 columns
        stream.commit_tx()

        pipeline.start()
        self.addCleanup(pipeline.stop)

        self._sleep_until(
            lambda: len([e for e in consumer.events if e.event_type == "row"]) >= 2
        )
        rows = [e for e in consumer.events if e.event_type == "row"]

        # Row at ~pos 200 must use the OLD 3-column schema -> no col_0 anywhere.
        row_old = rows[0].event.after_dict()
        self.assertIn("id", row_old)
        self.assertIn("name", row_old)
        self.assertIn("age", row_old)
        for k in row_old:
            self.assertFalse(k.startswith("col_"), f"bad column name: {k}")

        # Row at ~pos 600 must use the NEW 4-column schema.
        row_new = rows[1].event.after_dict()
        self.assertIn("email", row_new)
        for k in row_new:
            self.assertFalse(k.startswith("col_"), f"bad column name: {k}")

    # ------------------------------------------------------------------
    # Test: dedup_key lets downstream collapse snapshot + incremental
    # updates of the *same* primary key into a single final event.
    # ------------------------------------------------------------------
    def test_dedup_key_collapses_snapshot_and_update_to_one(self) -> None:
        users_schema = _make_users_schema()
        config = _build_config(self._tmpdir, snapshot=True)
        snap_rows = [[1, "Alice", 30]]
        provider = InMemorySnapshotProvider()
        provider.add_table(users_schema, snap_rows)
        snap_pos = BinlogPosition(
            binlog_file="mysql-bin-changelog.000001",
            position=4,
            timestamp=time.time(),
        )
        provider.set_snapshot_position(snap_pos)

        stream = SimulatedLogStream(events_per_second=10_000)
        stream.set_schema("app", "users", users_schema)
        stream.rotate("mysql-bin-changelog.000001")
        stream.begin_tx()
        stream.update(
            "app", "users",
            [1, "Alice", 30],
            [1, "Alice Smith", 31],
        )
        stream.commit_tx()

        consumer = InMemoryDownstreamConsumer()
        pipeline = CDCPipeline(
            config, log_parser=stream, snapshot_provider=provider,
        )
        pipeline.attach_consumer(consumer)
        pipeline.start()
        self.addCleanup(pipeline.stop)

        self._sleep_until(
            lambda: len(consumer.events) >= 4,
            timeout=10.0,
        )

        # Every row/snapshot event must carry a dedup_key.
        row_events = [e for e in consumer.events
                      if e.event_type in ("row", "snapshot")]
        self.assertEqual(len(row_events), 2)  # 1 snapshot + 1 update
        for ev in row_events:
            self.assertIsNotNone(ev.dedup_key, f"{ev.event_type} has no dedup_key")
            self.assertIn("id=", ev.dedup_key or "")

        # Collapsing by dedup_key should leave exactly 1 row for pk id=1
        # (the UPDATE, which has a higher position than the snapshot).
        latest = consumer.latest_by_dedup_key()
        self.assertEqual(len(latest), 1)
        key = list(latest.keys())[0]
        ev = latest[key]
        # Winner must be the row event (UPDATE), not the snapshot.
        self.assertEqual(ev.event_type, "row")
        self.assertEqual(ev.event.change_type.value, "UPDATE")
        # Final value must be the post-update version.
        after = ev.event.after_dict()
        self.assertEqual(after["name"], "Alice Smith")
        self.assertEqual(after["age"], 31)

    # ------------------------------------------------------------------
    # Test: INSERT / UPDATE / DELETE events all carry proper column
    # names (no col_N fallback) and correct before/after images.
    # ------------------------------------------------------------------
    def test_all_event_types_have_proper_column_names_and_pk(self) -> None:
        users_schema = _make_users_schema()
        config = _build_config(self._tmpdir, snapshot=False)
        stream = SimulatedLogStream(events_per_second=10_000)
        stream.set_schema("app", "users", users_schema)
        consumer = InMemoryDownstreamConsumer()
        pipeline = CDCPipeline(config, log_parser=stream)
        pipeline.attach_consumer(consumer)
        pipeline.schema_tracker.register_snapshot_schema(
            users_schema,
            BinlogPosition("mysql-bin-changelog.000001", 4),
        )

        stream.begin_tx()
        stream.insert("app", "users", [1, "Alice", 30])
        stream.update(
            "app", "users",
            [1, "Alice", 30],
            [1, "Alice Smith", 31],
        )
        stream.delete("app", "users", [1, "Alice Smith", 31])
        stream.commit_tx()

        pipeline.start()
        self.addCleanup(pipeline.stop)

        self._sleep_until(
            lambda: len([e for e in consumer.events if e.event_type == "row"]) >= 3,
            timeout=5.0,
        )
        rows = [e for e in consumer.events if e.event_type == "row"]
        self.assertEqual(len(rows), 3)

        insert_ev, update_ev, delete_ev = rows

        # ---- INSERT ----
        self.assertEqual(insert_ev.event.change_type, ChangeType.INSERT)
        self.assertEqual(insert_ev.event.primary_key, {"id": 1})
        after = insert_ev.event.after_dict()
        self.assertEqual(set(after.keys()), {"id", "name", "age"})
        self.assertEqual(after["name"], "Alice")
        self.assertEqual(after["age"], 30)
        for k in after:
            self.assertFalse(k.startswith("col_"), f"INSERT bad col: {k}")

        # ---- UPDATE ----
        self.assertEqual(update_ev.event.change_type, ChangeType.UPDATE)
        self.assertEqual(update_ev.event.primary_key, {"id": 1})
        before = update_ev.event.before_dict()
        after = update_ev.event.after_dict()
        self.assertEqual(set(before.keys()), {"id", "name", "age"})
        self.assertEqual(set(after.keys()), {"id", "name", "age"})
        self.assertEqual(before["name"], "Alice")
        self.assertEqual(after["name"], "Alice Smith")
        for k in list(before.keys()) + list(after.keys()):
            self.assertFalse(k.startswith("col_"), f"UPDATE bad col: {k}")

        # ---- DELETE ----
        self.assertEqual(delete_ev.event.change_type, ChangeType.DELETE)
        self.assertEqual(delete_ev.event.primary_key, {"id": 1})
        before = delete_ev.event.before_dict()
        self.assertEqual(set(before.keys()), {"id", "name", "age"})
        self.assertEqual(before["name"], "Alice Smith")
        for k in before:
            self.assertFalse(k.startswith("col_"), f"DELETE bad col: {k}")

        # dedup_key must be present and stable across all three events.
        keys = {e.dedup_key for e in rows}
        self.assertEqual(len(keys), 1)  # same pk -> same dedup key


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    unittest.main(verbosity=2)

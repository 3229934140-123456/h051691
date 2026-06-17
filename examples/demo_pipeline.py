"""Demo entry point for the CDC pipeline.

Run this script to see a fully end-to-end walk-through of the pipeline
without needing a live MySQL database.  It:

1. Builds an in-memory dataset with two tables (``app.users`` and
   ``app.orders``).
2. Starts the CDC pipeline which runs the full snapshot.
3. Feeds a synthetic binlog stream containing INSERT/UPDATE/DELETE
   events, a DDL that adds a column, and multiple transactions.
4. Pretty-prints every delivered event so you can see:
   * transaction boundaries,
   * before/after row images,
   * schema change events,
   * idempotency keys,
   * the offset advancing monotonically.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import time

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
from cdc_pipeline.downstream.consumer import LoggingDownstreamConsumer  # noqa: E402
from cdc_pipeline.log_parser.base import BinlogPosition  # noqa: E402
from cdc_pipeline.log_parser.simulator import SimulatedLogStream  # noqa: E402
from cdc_pipeline.models.schema import ColumnDef, TableSchema  # noqa: E402
from cdc_pipeline.pipeline import CDCPipeline  # noqa: E402
from cdc_pipeline.snapshot.snapshotter import InMemorySnapshotProvider  # noqa: E402


def _build_schema() -> tuple:
    users = TableSchema(
        database="app",
        table="users",
        columns=[
            ColumnDef(name="id", data_type="INT", is_primary_key=True, is_nullable=False, ordinal_position=0),
            ColumnDef(name="name", data_type="VARCHAR(255)", is_nullable=False, ordinal_position=1),
            ColumnDef(name="email", data_type="VARCHAR(255)", is_nullable=True, ordinal_position=2),
            ColumnDef(name="age", data_type="INT", is_nullable=True, ordinal_position=3),
        ],
        primary_key_columns=["id"],
        version=1,
        captured_at=time.time(),
    )
    orders = TableSchema(
        database="app",
        table="orders",
        columns=[
            ColumnDef(name="id", data_type="BIGINT", is_primary_key=True, is_nullable=False, ordinal_position=0),
            ColumnDef(name="user_id", data_type="INT", is_nullable=False, ordinal_position=1),
            ColumnDef(name="amount", data_type="DECIMAL(10,2)", is_nullable=False, ordinal_position=2),
            ColumnDef(name="status", data_type="VARCHAR(32)", is_nullable=False, ordinal_position=3),
        ],
        primary_key_columns=["id"],
        version=1,
        captured_at=time.time(),
    )
    return users, orders


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    tmpdir = tempfile.mkdtemp(prefix="cdc_demo_")
    print(f"Storing offsets / schemas under: {tmpdir}")

    users_schema, orders_schema = _build_schema()

    # --- Snapshot data. --------------------------------------------------
    provider = InMemorySnapshotProvider()
    provider.add_table(
        users_schema,
        [
            [1, "Alice", "alice@example.com", 30],
            [2, "Bob", "bob@example.com", 25],
            [3, "Carol", "carol@example.com", 40],
        ],
    )
    provider.add_table(
        orders_schema,
        [
            [1001, 1, "49.99", "PAID"],
            [1002, 2, "19.99", "PENDING"],
        ],
    )
    snap_pos = BinlogPosition(
        binlog_file="mysql-bin-changelog.000001",
        position=1024,
        timestamp=time.time(),
    )
    provider.set_snapshot_position(snap_pos)

    # --- Synthetic binlog stream. ---------------------------------------
    stream = SimulatedLogStream(events_per_second=2.0)
    stream.rotate("mysql-bin-changelog.000001")
    stream.set_schema("app", "users", users_schema)
    stream.set_schema("app", "orders", orders_schema)

    # Transaction 1: insert a new user + their first order.
    stream.begin_tx()
    stream.insert("app", "users", [4, "Dave", "dave@example.com", 35])
    stream.insert("app", "orders", [1003, 4, "99.99", "PAID"])
    stream.commit_tx(delay=0.5)

    # Transaction 2: DDL to add a column, then an insert using the new schema.
    stream.begin_tx(delay=1.0)
    stream.ddl(
        "app",
        "users",
        "ALTER TABLE app.users ADD COLUMN country VARCHAR(64) DEFAULT 'US' AFTER email",
    )
    stream.commit_tx(delay=0.5)

    # Transaction 3: UPDATE and DELETE interleaved.
    stream.begin_tx(delay=1.0)
    stream.update(
        "app", "users",
        [2, "Bob", "bob@example.com", "US", 25],
        [2, "Bob Smith", "bob.smith@example.com", "US", 26],
    )
    stream.delete("app", "orders", [1002, 2, "19.99", "PENDING"])
    stream.commit_tx(delay=0.5)

    # Transaction 4: final INSERT.
    stream.begin_tx(delay=1.0)
    stream.insert("app", "users", [5, "Eve", "eve@example.com", "UK", 28])
    stream.commit_tx(delay=0.5)

    # --- Assemble and run the pipeline. ---------------------------------
    config = PipelineConfig(
        database=DatabaseConfig(),
        offset_storage=OffsetStorageConfig(
            storage_type="file",
            file_path=os.path.join(tmpdir, "offsets.json"),
            flush_interval_ms=500,
        ),
        schema_storage=SchemaStorageConfig(
            storage_type="file",
            file_path=os.path.join(tmpdir, "schemas.json"),
        ),
        include_databases=["app"],
        snapshot_enabled=True,
        downstream_batch_size=16,
        downstream_flush_interval_ms=250,
        transaction_boundary_enabled=True,
        idempotency_key_enabled=True,
    )

    consumer = LoggingDownstreamConsumer()
    pipeline = CDCPipeline(config, log_parser=stream, snapshot_provider=provider)
    pipeline.attach_consumer(consumer)

    print("\n===== STARTING CDC PIPELINE =====")
    pipeline.start()
    try:
        time.sleep(10.0)
    finally:
        pipeline.stop()
    print("===== PIPELINE STOPPED =====\n")

    committed = pipeline.offset_manager.committed
    if committed is not None:
        print(
            f"Final committed offset: {committed.binlog_file} @ {committed.position}"
        )
    else:
        print("No offset was committed.")


if __name__ == "__main__":
    main()

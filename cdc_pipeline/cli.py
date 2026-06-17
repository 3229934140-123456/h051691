"""Command-line entry point for the CDC pipeline.

Usage::

    # Run with a YAML configuration file
    python -m cdc_pipeline.cli --config config.example.yaml

    # Run the built-in simulated demo (no config required)
    python -m cdc_pipeline.cli --demo

    # Exit cleanly with Ctrl+C
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from typing import Any, Dict, Optional

from . import __version__
from .config import (
    DatabaseConfig,
    OffsetStorageConfig,
    PipelineConfig,
    SchemaStorageConfig,
)
from .downstream import DeliveryStats
from .downstream.consumer import DownstreamConsumer
from .log_parser.base import BinlogPosition
from .log_parser.mysql_binlog import MySQLBinlogParser
from .log_parser.simulator import SimulatedLogStream
from .models.schema import ColumnDef, TableSchema
from .pipeline import CDCPipeline
from .sinks import build_sink_from_config
from .snapshot.snapshotter import InMemorySnapshotProvider


def _load_yaml(path: str) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "PyYAML is required to load YAML configs.  Install it with `pip install pyyaml`"
        ) from exc
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _build_config_from_yaml(cfg: Dict[str, Any]) -> PipelineConfig:
    db = DatabaseConfig(**(cfg.get("database") or {}))
    snap = cfg.get("snapshot") or {}
    dlv = cfg.get("delivery") or {}
    off = cfg.get("offset_storage") or {}
    sch = cfg.get("schema_storage") or {}

    return PipelineConfig(
        database=db,
        offset_storage=OffsetStorageConfig(
            storage_type=off.get("type", "file"),
            file_path=off.get("path", "./data/cdc_offsets.json"),
            flush_interval_ms=int(off.get("flush_interval_ms", 1000)),
        ),
        schema_storage=SchemaStorageConfig(
            storage_type=sch.get("type", "file"),
            file_path=sch.get("path", "./data/cdc_schema_history.json"),
        ),
        include_tables=list(cfg.get("include_tables") or []),
        exclude_tables=list(cfg.get("exclude_tables") or []),
        include_databases=list(cfg.get("include_databases") or []),
        exclude_databases=list(cfg.get("exclude_databases") or []),
        snapshot_enabled=bool(snap.get("enabled", True)),
        snapshot_lock_timeout_seconds=int(snap.get("lock_timeout_seconds", 10)),
        snapshot_chunk_size=int(snap.get("chunk_size", 1000)),
        downstream_batch_size=int(dlv.get("batch_size", 100)),
        downstream_flush_interval_ms=int(dlv.get("flush_interval_ms", 500)),
        transaction_boundary_enabled=bool(dlv.get("transaction_boundary_enabled", True)),
        idempotency_key_enabled=bool(dlv.get("idempotency_key_enabled", True)),
    )


def _build_demo_source(
    provider: InMemorySnapshotProvider,
    stream: SimulatedLogStream,
    snap_pos: BinlogPosition,
) -> None:
    """Populate a synthetic dataset + incremental timeline for --demo mode."""
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
    provider.add_table(
        users,
        [
            [1, "Alice", "alice@example.com", 30],
            [2, "Bob", "bob@example.com", 25],
            [3, "Carol", "carol@example.com", 40],
        ],
    )
    provider.add_table(
        orders,
        [
            [1001, 1, "49.99", "PAID"],
            [1002, 2, "19.99", "PENDING"],
        ],
    )
    provider.set_snapshot_position(snap_pos)

    stream.set_schema("app", "users", users)
    stream.set_schema("app", "orders", orders)
    stream.rotate("mysql-bin-changelog.000001")

    # Build an incremental timeline that gradually feeds new events.
    stream.begin_tx(delay=0.0)
    stream.insert("app", "users", [4, "Dave", "dave@example.com", 35])
    stream.insert("app", "orders", [1003, 4, "99.99", "PAID"])
    stream.commit_tx(delay=0.8)

    stream.begin_tx(delay=1.2)
    stream.ddl(
        "app",
        "users",
        "ALTER TABLE app.users ADD COLUMN country VARCHAR(64) DEFAULT 'US' AFTER email",
    )
    stream.commit_tx(delay=0.5)

    # After the DDL the schema tracker holds 4 columns + new country.
    new_users = TableSchema(
        database="app",
        table="users",
        columns=[
            ColumnDef(name="id", data_type="INT", is_primary_key=True, is_nullable=False, ordinal_position=0),
            ColumnDef(name="name", data_type="VARCHAR(255)", is_nullable=False, ordinal_position=1),
            ColumnDef(name="email", data_type="VARCHAR(255)", is_nullable=True, ordinal_position=2),
            ColumnDef(name="country", data_type="VARCHAR(64)", is_nullable=True, ordinal_position=3),
            ColumnDef(name="age", data_type="INT", is_nullable=True, ordinal_position=4),
        ],
        primary_key_columns=["id"],
        version=2,
        captured_at=time.time(),
    )
    stream.set_schema("app", "users", new_users)

    stream.begin_tx(delay=1.0)
    stream.update(
        "app", "users",
        [2, "Bob", "bob@example.com", "US", 25],
        [2, "Bob Smith", "bob.smith@example.com", "US", 26],
    )
    stream.delete("app", "orders", [1002, 2, "19.99", "PENDING"])
    stream.commit_tx(delay=0.6)

    stream.begin_tx(delay=1.0)
    stream.insert("app", "users", [5, "Eve", "eve@example.com", "UK", 28])
    stream.commit_tx(delay=0.5)

    # Keep the stream alive with a few more events.
    stream.begin_tx(delay=1.2)
    stream.insert("app", "orders", [1004, 5, "29.99", "PENDING"])
    stream.commit_tx(delay=0.5)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print_progress(stats: DeliveryStats) -> None:
    """Render a compact status line without spamming the terminal."""
    file = stats.last_delivered_file or "-"
    pos = stats.last_delivered_position or 0
    line = (
        f"\r[CDC] "
        f"delivered={stats.events_delivered:<8} "
        f"submitted={stats.events_submitted:<8} "
        f"batches_ok={stats.batches_delivered:<5} "
        f"retries={stats.batches_retried:<4} "
        f"pending_tx={stats.pending_transactions:<3} "
        f"pending_buf={stats.pending_non_tx_events:<4} "
        f"pos={file}:{pos:<10} "
    )
    sys.stderr.write(line)
    sys.stderr.flush()


def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="cdc-pipeline",
        description="Capture row-level changes from a database and stream them to a downstream sink.",
    )
    p.add_argument("--config", type=str, default=None, help="Path to a YAML config file.")
    p.add_argument("--demo", action="store_true", help="Run the built-in in-memory demo source + snapshot.")
    p.add_argument("--sink", choices=["log", "stdout", "memory", "jsonl", "webhook"],
                   default=None, help="Override the sink type set in config.")
    p.add_argument("--sink-path", type=str, default=None, help="For jsonl sink, the output file path.")
    p.add_argument("--sink-url", type=str, default=None, help="For webhook sink, the target URL.")
    p.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging.")
    p.add_argument("--version", action="version", version=f"cdc-pipeline {__version__}")
    return p.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    args = _parse_args(argv)

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    # ---- Resolve configuration ------------------------------------------
    if args.config:
        if not os.path.exists(args.config):
            print(f"Config file not found: {args.config}", file=sys.stderr)
            return 2
        cfg = _load_yaml(args.config)
        config = _build_config_from_yaml(cfg)
        sink_cfg = dict(cfg.get("sink") or {})
    elif args.demo:
        config = PipelineConfig(
            offset_storage=OffsetStorageConfig(
                storage_type="file",
                file_path="./data/demo_offsets.json",
                flush_interval_ms=500,
            ),
            schema_storage=SchemaStorageConfig(
                storage_type="file",
                file_path="./data/demo_schemas.json",
            ),
            include_databases=["app"],
            snapshot_enabled=True,
            downstream_batch_size=8,
            downstream_flush_interval_ms=200,
            transaction_boundary_enabled=True,
        )
        sink_cfg = {"type": "stdout"}
    else:
        print("Either --config <path> or --demo is required. Use --help for usage.", file=sys.stderr)
        return 2

    # ---- Sink override flags --------------------------------------------
    if args.sink:
        sink_cfg["type"] = args.sink
    if args.sink_path:
        sink_cfg["path"] = args.sink_path
    if args.sink_url:
        sink_cfg["url"] = args.sink_url

    sink: DownstreamConsumer = build_sink_from_config(sink_cfg)

    # ---- Build the pipeline ---------------------------------------------
    parser: Any = None
    snapshot_provider: Optional[InMemorySnapshotProvider] = None

    source_kind = "mysql"
    if args.config:
        source_kind = cfg.get("source", "mysql")

    if source_kind == "simulated" or args.demo:
        snap_pos = BinlogPosition(
            binlog_file="mysql-bin-changelog.000001", position=4, timestamp=time.time()
        )
        snapshot_provider = InMemorySnapshotProvider()
        parser = SimulatedLogStream(events_per_second=5.0 if args.demo else 100.0)
        if args.demo:
            _build_demo_source(snapshot_provider, parser, snap_pos)
    else:
        parser = MySQLBinlogParser(config.database)

    pipeline = CDCPipeline(
        config,
        log_parser=parser,
        snapshot_provider=snapshot_provider,
    )

    progress_counter = {"n": 0}

    def on_progress(stats: DeliveryStats) -> None:
        # Refresh terminal every ~5 metric updates, avoid busy redrawing.
        progress_counter["n"] += 1
        if progress_counter["n"] % 3 == 0:
            _print_progress(stats)

    pipeline.attach_consumer(sink, on_progress=on_progress)

    # ---- Lifecycle ------------------------------------------------------
    stop_event = threading.Event()

    def _handle_sigint(*_: Any) -> None:
        print("\n[CDC] Received SIGINT, shutting down gracefully...", file=sys.stderr)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    pipeline.start()
    try:
        # Wait until we're told to stop.  Also keep refreshing stats so the
        # terminal line updates even if no new events arrive for a while.
        while not stop_event.is_set():
            stats = pipeline.delivery_stats()
            if stats is not None:
                _print_progress(stats)
            if not pipeline.is_running():
                break
            stop_event.wait(0.5)
    finally:
        pipeline.stop(timeout=10)
        stats = pipeline.delivery_stats()
        if stats is not None:
            _print_progress(stats)
        print("\n[CDC] Pipeline stopped.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

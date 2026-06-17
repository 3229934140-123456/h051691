"""JSONL file sink - writes each event as one JSON line."""

from __future__ import annotations

import json
import os
import tempfile
from typing import IO, List, Optional

from ..downstream.consumer import DeliveryBatch, DownstreamConsumer
from ..models.event import EventEnvelope


class JsonlFileSink(DownstreamConsumer):
    """Write every delivered event as a single JSON line to ``file_path``.

    *   Each :class:`DeliveryBatch` is written atomically via
        ``write-to-tmp + os.replace`` so downstream readers never see a
        partial batch.
    *   Transactional boundaries are preserved: a full
        ``[tx_begin, rows, tx_commit]`` sequence is always written in a
        single batch flush and therefore always appears contiguously in
        the output.
    *   When ``dedupe_index_path`` is supplied the sink persists the set
        of already-seen ``idempotency_key`` values there.  On restart
        any event whose key is already in the index is skipped on
        write, so replay of already-confirmed offsets does not produce
        duplicate lines.
    *   Passing ``file_path="-"`` writes to ``sys.stdout`` line by line
        (useful for CLI demos) without any dedup index.
    """

    def __init__(
        self,
        file_path: str,
        dedupe_index_path: Optional[str] = None,
        append: bool = True,
        flush_every_batch: bool = True,
    ) -> None:
        self._file_path = file_path
        self._dedupe_index_path = dedupe_index_path
        self._append = append
        self._flush_every_batch = flush_every_batch
        self._seen_keys: set = set()
        self._fp: Optional[IO] = None
        self._stdout_mode = file_path == "-"

        if self._dedupe_index_path and os.path.exists(self._dedupe_index_path):
            with open(self._dedupe_index_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self._seen_keys.add(line)

        if not self._stdout_mode:
            parent = os.path.dirname(os.path.abspath(self._file_path))
            if parent:
                os.makedirs(parent, exist_ok=True)
            mode = "a" if append and os.path.exists(self._file_path) else "w"
            self._fp = open(self._file_path, mode, encoding="utf-8")

    # ------------------------------------------------------------------
    def deliver_batch(self, batch: DeliveryBatch) -> None:
        events_to_write: List[EventEnvelope] = []
        for ev in batch.events:
            if ev.idempotency_key and ev.idempotency_key in self._seen_keys:
                # Already written on a prior run - skip.
                continue
            events_to_write.append(ev)

        if not events_to_write:
            return

        lines = [
            json.dumps(ev.to_dict(), ensure_ascii=False, default=str)
            for ev in events_to_write
        ]
        block = "\n".join(lines) + "\n"

        if self._stdout_mode:
            import sys
            sys.stdout.write(block)
            sys.stdout.flush()
        else:
            self._write_atomic(block)

        # Only persist the keys AFTER the data block is durable.
        if self._dedupe_index_path is not None:
            new_keys = [ev.idempotency_key for ev in events_to_write if ev.idempotency_key]
            if new_keys:
                with open(self._dedupe_index_path, "a", encoding="utf-8") as f:
                    for k in new_keys:
                        f.write(k + "\n")
                        self._seen_keys.add(k)

    # ------------------------------------------------------------------
    def _write_atomic(self, block: str) -> None:
        assert self._fp is not None
        if self._flush_every_batch:
            # Atomic per-batch: use tmp file + rename.
            fp_dir = os.path.dirname(os.path.abspath(self._file_path))
            self._fp.close()
            fd, tmp_path = tempfile.mkstemp(
                prefix=".cdc_jsonl_", suffix=".tmp", dir=fp_dir
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as tmp_fp:
                    # Append to existing content if in append mode.
                    if self._append and os.path.exists(self._file_path):
                        with open(self._file_path, "r", encoding="utf-8") as cur:
                            tmp_fp.write(cur.read())
                    tmp_fp.write(block)
                    tmp_fp.flush()
                    os.fsync(tmp_fp.fileno())
                os.replace(tmp_path, self._file_path)
            finally:
                if os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass
            # Re-open for next batch.
            self._fp = open(self._file_path, "a", encoding="utf-8")
        else:
            self._fp.write(block)
            self._fp.flush()

    def close(self) -> None:
        if self._fp is not None:
            try:
                self._fp.close()
            finally:
                self._fp = None

    def __del__(self):
        self.close()

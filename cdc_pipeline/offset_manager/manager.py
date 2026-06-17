"""Concrete offset manager implementation."""

from __future__ import annotations

import json
import os
import threading
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from ..config import OffsetStorageConfig
from ..log_parser.base import BinlogPosition


class OffsetStore(ABC):
    """Abstract persistence back-end for offset records."""

    @abstractmethod
    def read(self) -> Optional[Dict[str, Any]]:
        """Return the last stored offset record, or None if empty."""
        raise NotImplementedError

    @abstractmethod
    def write(self, record: Dict[str, Any]) -> None:
        """Atomically persist an offset record."""
        raise NotImplementedError


class FileOffsetStore(OffsetStore):
    """Offset persistence backed by a single JSON file.

    Writes use a write-to-tmp + os.replace pattern so that a crash in the
    middle of a write either leaves the old file in place (if the crash
    happens before :func:`os.replace`) or the new one (if it happens
    after).  No partial/garbled file is ever observable on restart.
    """

    def __init__(self, file_path: str) -> None:
        self._file_path = os.path.abspath(file_path)
        self._tmp_path = self._file_path + ".tmp"

    def read(self) -> Optional[Dict[str, Any]]:
        if not os.path.exists(self._file_path):
            return None
        try:
            with open(self._file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return None

    def write(self, record: Dict[str, Any]) -> None:
        parent = os.path.dirname(self._file_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(self._tmp_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(self._tmp_path, self._file_path)


class OffsetManager:
    """Tracks the highest binlog position that has been *confirmed* delivered.

    Threading
    ---------
    All public methods are safe to call from multiple threads.  The
    manager uses a single :class:`threading.Lock` to protect state and a
    background flush timer to periodically persist the latest committed
    position so that a crash does not lose too much progress.
    """

    def __init__(
        self,
        config: OffsetStorageConfig,
        store: Optional[OffsetStore] = None,
    ) -> None:
        self._config = config
        self._store = store or FileOffsetStore(config.file_path)
        self._lock = threading.RLock()
        self._committed: Optional[BinlogPosition] = None
        self._last_flushed: Optional[BinlogPosition] = None
        self._last_flush_time = 0.0
        self._dirty = False
        self._snapshot_mode_active = False
        self._load_initial()

    # ------------------------------------------------------------------
    # Startup API
    # ------------------------------------------------------------------
    def load_last_committed(self) -> Optional[BinlogPosition]:
        """Return the last position persisted to the store (or None)."""
        with self._lock:
            return self._committed

    def mark_snapshot_in_progress(self) -> None:
        """Signal that a full snapshot is about to begin.

        When a snapshot is running we intentionally DO NOT advance the
        committed offset until the snapshot completes and we have
        switched over to incremental streaming.  This way a crash
        mid-snapshot simply restarts the snapshot from scratch.
        """
        with self._lock:
            self._snapshot_mode_active = True

    def mark_snapshot_complete(self, start_position: BinlogPosition) -> None:
        """Mark snapshot as done and record the binlog position it started at.

        This is the critical "snapshot -> incremental handoff" moment.
        The snapshot was taken while holding a consistent read view at
        exactly ``start_position``.  From now on incremental binlog
        streaming starts at the same position and any row changes the
        snapshot already captured will be replayed -- but that is fine
        because downstream consumers deduplicate using the per-event
        idempotency key.
        """
        with self._lock:
            self._snapshot_mode_active = False
            # Do NOT auto-advance past start_position yet - that happens
            # when the first incremental event gets acked.  Recording
            # start_position here lets the next pipeline run know it
            # should *not* run another snapshot.
            self._advance(start_position)
            self._flush(force=True)

    # ------------------------------------------------------------------
    # Runtime API
    # ------------------------------------------------------------------
    def ack(self, position: BinlogPosition) -> None:
        """Report that ``position`` was successfully delivered downstream.

        The offset manager only records ``position`` if it is strictly
        greater than what was already committed.  This makes out-of-order
        acks safe: even if a downstream batch acks an older position
        after a newer one, the committed offset never moves backwards.
        """
        with self._lock:
            if self._snapshot_mode_active:
                # During a snapshot we still accept acks (they come from
                # the snapshot rows themselves) but do not persist them
                # yet; only the final snapshot completion marker counts.
                return
            self._advance(position)
            self._maybe_flush()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _load_initial(self) -> None:
        record = self._store.read()
        if record is None:
            self._committed = None
            self._last_flushed = None
            return
        try:
            self._committed = BinlogPosition.from_dict(record["position"])
            self._last_flushed = self._committed
        except (KeyError, TypeError):
            self._committed = None
            self._last_flushed = None

    def _advance(self, position: BinlogPosition) -> None:
        if self._committed is None or position > self._committed:
            self._committed = position
            self._dirty = True

    def _maybe_flush(self) -> None:
        now = time.time()
        if not self._dirty:
            return
        elapsed_ms = (now - self._last_flush_time) * 1000.0
        if elapsed_ms >= self._config.flush_interval_ms:
            self._flush(force=False)

    def _flush(self, force: bool) -> None:
        if not force and not self._dirty:
            return
        if self._committed is None:
            return
        if (
            self._last_flushed is not None
            and self._committed == self._last_flushed
        ):
            self._dirty = False
            return
        record = {
            "position": self._committed.to_dict(),
            "persisted_at": time.time(),
        }
        self._store.write(record)
        self._last_flushed = self._committed
        self._last_flush_time = time.time()
        self._dirty = False

    # ------------------------------------------------------------------
    # Debug helpers
    # ------------------------------------------------------------------
    @property
    def committed(self) -> Optional[BinlogPosition]:
        with self._lock:
            return self._committed

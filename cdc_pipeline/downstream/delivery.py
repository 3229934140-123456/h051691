"""Delivery manager - batches events and drives the downstream consumer.

The delivery manager sits between the transformer and the consumer.  It
receives individual event envelopes and:

* When transaction boundaries are enabled it buffers row events that
  share a ``transaction_id`` until the matching ``tx_commit`` arrives,
  at which point the entire ``[tx_begin, rows..., tx_commit]`` sequence
  is handed to the consumer as a single atomic batch.

* When a batch reaches the configured size or time budget, or when a
  transaction commits, it invokes :meth:`DownstreamConsumer.deliver_batch`
  and, on success, acks the maximum position in the batch back to the
  offset manager.

Transaction buffering
---------------------
Databases emit binlog events for a single transaction spread over many
kilobytes of log.  The pipeline therefore cannot know that a transaction
is "done" until it sees the XID/COMMIT marker.  To preserve transaction
boundaries for the downstream consumer we therefore:

1. Keep a ``pending_transactions`` dict of ``transaction_id -> list of
   events``.
2. When a ``tx_begin`` arrives we allocate an entry and record the
   begin envelope.
3. When row or schema events arrive with a ``transaction_id`` we append
   them to that entry instead of delivering them immediately.
4. When the matching ``tx_commit`` arrives we finalise the entry: append
   the commit envelope, call the consumer, and -- on success -- remove
   the entry and ack the max position.

If a transaction stays open for a very long time the pipeline will hold
its events in memory.  This is bounded by the DB server itself (MySQL
has ``innodb_lock_wait_timeout`` and friends) but we also expose a
``max_transaction_events`` safety cap that, when hit, forces an early
partial flush (and loses transaction atomicity for that one tx only).
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from ..config import PipelineConfig
from ..log_parser.base import BinlogPosition
from ..models.event import EventEnvelope
from .consumer import DeliveryBatch, DownstreamConsumer

_LOG = logging.getLogger("cdc.delivery")


@dataclass
class TransactionalBatch:
    events: List[EventEnvelope] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)

    def append(self, ev: EventEnvelope) -> None:
        self.events.append(ev)


class DeliveryManager:
    """Collects individual event envelopes and delivers them in batches."""

    def __init__(
        self,
        consumer: DownstreamConsumer,
        config: PipelineConfig,
        on_ack: Optional[Callable[[BinlogPosition], None]] = None,
    ) -> None:
        self._consumer = consumer
        self._config = config
        self._on_ack = on_ack
        self._lock = threading.RLock()
        self._pending_transactions: Dict[str, TransactionalBatch] = {}
        self._non_tx_buffer: List[EventEnvelope] = []
        self._buffer_full_at: Optional[float] = None
        self._delivery_thread: Optional[threading.Thread] = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._delivery_thread = threading.Thread(
            target=self._delivery_loop, name="cdc-delivery", daemon=True
        )
        self._delivery_thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._running = False
        if self._delivery_thread is not None:
            self._delivery_thread.join(timeout=timeout)
            self._delivery_thread = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def submit(self, envelope: EventEnvelope) -> None:
        """Hand an envelope to the manager for eventual delivery."""
        with self._lock:
            tx_id = envelope.transaction_id
            if tx_id and self._config.transaction_boundary_enabled:
                self._submit_transactional(tx_id, envelope)
            else:
                self._non_tx_buffer.append(envelope)
                self._maybe_mark_buffer()

    def deliver_snapshot_batch(self, envelopes: List[EventEnvelope]) -> None:
        """Synchronously deliver a batch coming from the initial snapshot.

        Snapshot rows are not associated with any binlog transaction and
        should be flushed as quickly as possible to the consumer so that
        the initial bulk-load finishes in a reasonable time.
        """
        if not envelopes:
            return
        batch = DeliveryBatch(events=list(envelopes), source="snapshot")
        self._deliver_with_retry(batch)

    def flush(self) -> None:
        """Force any buffered non-transactional events to be delivered now."""
        with self._lock:
            if self._non_tx_buffer:
                self._flush_non_tx_locked()

    # ------------------------------------------------------------------
    # Transactional buffering
    # ------------------------------------------------------------------
    def _submit_transactional(self, tx_id: str, ev: EventEnvelope) -> None:
        if ev.event_type == "tx_begin":
            self._pending_transactions[tx_id] = TransactionalBatch(events=[ev])
            return

        batch = self._pending_transactions.get(tx_id)
        if batch is None:
            # We missed the BEGIN (e.g. we joined mid-transaction).  Fall
            # back to non-transactional delivery for these events.
            self._non_tx_buffer.append(ev)
            self._maybe_mark_buffer()
            return

        if ev.event_type == "tx_commit":
            batch.append(ev)
            self._deliver_transaction_locked(tx_id, batch)
            return

        batch.append(ev)
        # Safety cap: if a transaction grows too large, flush what we
        # have and demote the rest to non-transactional delivery.
        if len(batch.events) > 10_000:
            _LOG.warning(
                "Transaction %s exceeded safety cap (%d events); "
                "flushing partial batch and falling back to non-tx mode.",
                tx_id,
                len(batch.events),
            )
            self._deliver_transaction_locked(tx_id, batch)

    def _deliver_transaction_locked(self, tx_id: str, batch: TransactionalBatch) -> None:
        self._pending_transactions.pop(tx_id, None)
        delivery = DeliveryBatch(events=list(batch.events), source="incremental")
        self._deliver_with_retry(delivery)

    # ------------------------------------------------------------------
    # Non-transactional buffering
    # ------------------------------------------------------------------
    def _maybe_mark_buffer(self) -> None:
        if (
            self._buffer_full_at is None
            and len(self._non_tx_buffer) >= self._config.downstream_batch_size
        ):
            self._buffer_full_at = time.time()

    def _delivery_loop(self) -> None:
        flush_interval = self._config.downstream_flush_interval_ms / 1000.0
        while self._running:
            try:
                time.sleep(min(flush_interval, 0.1))
                with self._lock:
                    now = time.time()
                    over_size = self._buffer_full_at is not None
                    over_time = (
                        self._non_tx_buffer
                        and (self._buffer_full_at or now) - now >= flush_interval
                    )
                    if over_size or over_time or (self._non_tx_buffer and flush_interval == 0):
                        self._flush_non_tx_locked()
            except Exception:
                _LOG.exception("Delivery loop error")

    def _flush_non_tx_locked(self) -> None:
        if not self._non_tx_buffer:
            return
        events = self._non_tx_buffer
        self._non_tx_buffer = []
        self._buffer_full_at = None
        delivery = DeliveryBatch(events=events, source="incremental")
        self._deliver_with_retry(delivery)

    # ------------------------------------------------------------------
    # Core delivery + retry
    # ------------------------------------------------------------------
    def _deliver_with_retry(self, batch: DeliveryBatch) -> None:
        """Call the consumer with simple linear backoff on transient errors.

        The pipeline intentionally *blocks* on a failing downstream:
        offsets must not advance until the consumer confirms success, and
        we would rather slow down than silently lose data.
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                self._consumer.deliver_batch(batch)
                break
            except Exception:
                sleep_s = min(2 ** attempt, 30)
                _LOG.exception(
                    "Downstream delivery failed on attempt %d; retrying in %.1fs",
                    attempt,
                    sleep_s,
                )
                time.sleep(sleep_s)
                if not self._running:
                    raise

        # Success -> ack the highest position in the batch.
        max_pos = batch.max_position()
        if max_pos is not None and self._on_ack is not None:
            self._on_ack(max_pos)

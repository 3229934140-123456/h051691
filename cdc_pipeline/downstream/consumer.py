"""Downstream consumer interface + reference implementations."""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..models.event import EventEnvelope

_LOG = logging.getLogger("cdc.downstream")


@dataclass
class DeliveryBatch:
    """A single batch of events handed to a downstream consumer.

    Attributes
    ----------
    events:
        The ordered list of event envelopes to deliver.  When transaction
        boundaries are enabled the batch is either:
          * a full transaction ``[tx_begin, ...rows..., tx_commit]``, or
          * a non-transactional batch of events assembled by size/time.
    source:
        "snapshot" if these rows came from the initial snapshot phase,
        "incremental" otherwise.  Consumers sometimes need to tell the
        two apart (e.g. to switch from bulk-load mode to live mode).
    """

    events: List[EventEnvelope]
    source: str = "incremental"

    def max_position(self):
        """Return the largest binlog position present in this batch.

        The offset manager is acked with this value after a successful
        delivery: it is guaranteed to be greater than or equal to every
        individual event position in the batch.
        """
        from ..log_parser.base import BinlogPosition

        best: Optional[BinlogPosition] = None
        for ev in self.events:
            if ev.binlog_file is None:
                continue
            pos = BinlogPosition(
                binlog_file=ev.binlog_file,
                position=ev.position or 0,
                gtid=ev.gtid,
                timestamp=ev.timestamp,
            )
            if best is None or pos > best:
                best = pos
        return best


class DownstreamConsumer(ABC):
    """Abstract downstream consumer.

    A real implementation might publish to Kafka, write to a target
    database via JDBC, fan out to web-hooks, etc.
    """

    @abstractmethod
    def deliver_batch(self, batch: DeliveryBatch) -> None:
        """Deliver a batch of events to the downstream system.

        The contract: if this method returns normally, the batch is
        considered durably received and the offset manager will be
        advanced.  If the method raises an exception the delivery will
        be retried (backoff policy is up to the caller).
        """
        raise NotImplementedError


class InMemoryDownstreamConsumer(DownstreamConsumer):
    """Stores delivered batches in-memory; used by the test suite."""

    def __init__(self) -> None:
        self.batches: List[DeliveryBatch] = []
        self.events: List[EventEnvelope] = []
        self.seen_keys: set = set()
        self.duplicates: List[EventEnvelope] = []

    def deliver_batch(self, batch: DeliveryBatch) -> None:
        self.batches.append(batch)
        for ev in batch.events:
            self.events.append(ev)
            if ev.idempotency_key and ev.idempotency_key in self.seen_keys:
                self.duplicates.append(ev)
            elif ev.idempotency_key:
                self.seen_keys.add(ev.idempotency_key)

    def unique_events(self) -> List[EventEnvelope]:
        seen: set = set()
        out: List[EventEnvelope] = []
        for ev in self.events:
            if ev.idempotency_key and ev.idempotency_key in seen:
                continue
            if ev.idempotency_key:
                seen.add(ev.idempotency_key)
            out.append(ev)
        return out

    def latest_by_dedup_key(self) -> Dict[str, EventEnvelope]:
        """Return the *latest* event per primary-key dedup key.

        Useful for collapsing snapshot + incremental overlap into a single
        final version of each row.  Events without a dedup_key (e.g.
        transaction boundaries, DDL) are skipped.
        """
        result: Dict[str, EventEnvelope] = {}
        for ev in self.events:
            if not ev.dedup_key:
                continue
            existing = result.get(ev.dedup_key)
            if existing is None:
                result[ev.dedup_key] = ev
                continue
            # Keep the one with the higher position (later in log).
            pos_new = (ev.binlog_file or "", ev.position or 0)
            pos_old = (existing.binlog_file or "", existing.position or 0)
            if pos_new > pos_old:
                result[ev.dedup_key] = ev
        return result


class LoggingDownstreamConsumer(DownstreamConsumer):
    """Pretty-prints every delivered batch at INFO level."""

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self._logger = logger or _LOG

    def deliver_batch(self, batch: DeliveryBatch) -> None:
        self._logger.info(
            "Delivering %d event(s) (%s source)",
            len(batch.events),
            batch.source,
        )
        for ev in batch.events:
            self._logger.info(
                "  [%s] id=%s source=%s file=%s pos=%s tx=%s idem=%s payload=%s",
                ev.event_type,
                ev.event_id[:12],
                ev.source,
                ev.binlog_file,
                ev.position,
                ev.transaction_id,
                ev.idempotency_key,
                json.dumps(ev.to_dict().get("event", {}), ensure_ascii=False, default=str),
            )

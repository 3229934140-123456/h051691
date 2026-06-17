"""Downstream delivery module.

This module owns the *last mile* of the CDC pipeline: taking
envelope-wrapped events from the transformer and handing them to a
downstream consumer with the following guarantees:

1. **Transactional boundaries.**  When the pipeline is configured to
   honour transaction boundaries, all row events that belong to a single
   upstream database transaction are delivered together inside a single
   call to :meth:`DownstreamConsumer.deliver_batch`, flanked by a
   ``tx_begin`` and a ``tx_commit`` envelope.  The consumer therefore
   sees the upstream transaction *as an atomic unit* even though the
   individual row events may have arrived over several seconds of
   binlog streaming.

2. **At-least-once delivery.**  Events are only *acknowledged* back to
   the :class:`OffsetManager` *after*
   :meth:`DownstreamConsumer.deliver_batch` has returned successfully.
   A crash between a successful delivery and the subsequent offset
   flush causes those events to be redelivered on restart, which is
   exactly the "at-least-once" contract.

3. **Idempotent replay.**  Every event envelope carries a deterministic
   ``idempotency_key`` computed from the upstream log position +
   event type + primary key.  Downstream consumers can use this key
   (e.g. as a ``UNIQUE`` column in a target table, or as the message key
   in Kafka) to make replayed events a strict no-op, closing the loop
   from "at-least-once" to "effectively-exactly-once".

The module ships with two out-of-the-box consumers:

* :class:`LoggingDownstreamConsumer` simply pretty-prints every batch.
* :class:`InMemoryDownstreamConsumer` stores all deliveries in a list
  and is used heavily by the unit tests.
"""

from .consumer import (
    DownstreamConsumer,
    InMemoryDownstreamConsumer,
    LoggingDownstreamConsumer,
)
from .delivery import DeliveryManager, TransactionalBatch

__all__ = [
    "DownstreamConsumer",
    "InMemoryDownstreamConsumer",
    "LoggingDownstreamConsumer",
    "DeliveryManager",
    "TransactionalBatch",
]

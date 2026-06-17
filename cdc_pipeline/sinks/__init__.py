"""Pluggable downstream sinks.

This module extends the pipeline with production-grade sinks beyond the
simple in-memory / logging consumers used in tests:

* :class:`JsonlFileSink`  - writes every event as one JSON line to a
  file on disk (or to ``stdout``) with atomic ``tmp + replace`` rotation
  per batch.  Because batches are written as a whole, a transaction's
  ``[tx_begin, rows, tx_commit]`` sequence always appears together and
  in order -- even across a crash / restart.

* :class:`HttpWebhookSink` - POSTs each batch to an HTTP endpoint as a
  JSON array.  Network errors and 5xx responses trigger exponential
  backoff; 4xx responses (bad request) are considered a programmer
  error and are re-raised after logging.

Both sinks use the per-event ``idempotency_key`` to advertise
deduplication information (via a request header for the webhook, and
via an on-disk index for the JSONL sink) so that restart replay does
not produce duplicate downstream effects.
"""

from .jsonl_sink import JsonlFileSink
from .webhook_sink import HttpWebhookSink
from .factory import build_sink_from_config

__all__ = ["JsonlFileSink", "HttpWebhookSink", "build_sink_from_config"]

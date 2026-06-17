"""HTTP webhook sink - POSTs batches to an HTTP endpoint."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional
from urllib import error, request

from ..downstream.consumer import DeliveryBatch, DownstreamConsumer

_LOG = logging.getLogger("cdc.sinks.webhook")


class HttpWebhookSink(DownstreamConsumer):
    """POST each :class:`DeliveryBatch` to ``url`` as a JSON array.

    HTTP contract
    -------------
    Request body (single batch)::

        {
            "batch_id": "<uuid>",
            "source":   "snapshot" | "incremental",
            "event_count": N,
            "max_binlog_file": "...",
            "max_binlog_position": 12345,
            "transaction_id": "<optional tx id if this is a full-tx batch>",
            "events": [ ... EventEnvelope.to_dict() ... ]
        }

    Request headers:
        ``Content-Type: application/json``
        ``X-CDC-Idempotency-Key: <sha256(sorted event idempotency keys)>``
        ``X-CDC-Retry-Attempt: <n>``

    The webhook server MUST respond with:
      * 2xx on success (any 2xx code is treated as durable receipt).
      * 4xx on a request the server will never accept (bad payload etc.)
        - the sink raises an exception so operators can fix the config.
      * 5xx on transient failure - the sink retries with exponential
        backoff up to ``max_retries``, then raises.

    Transaction batches are always POSTed as a *single* HTTP request so
    the downstream service observes ``tx_begin``, all row changes, and
    ``tx_commit`` together with ``transaction_id`` populated in the
    request envelope.
    """

    def __init__(
        self,
        url: str,
        timeout_seconds: float = 10.0,
        max_retries: int = 10,
        initial_backoff_seconds: float = 0.5,
        max_backoff_seconds: float = 30.0,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self._url = url
        self._timeout = timeout_seconds
        self._max_retries = max_retries
        self._init_backoff = initial_backoff_seconds
        self._max_backoff = max_backoff_seconds
        self._extra_headers = dict(extra_headers or {})
        self._delivery_attempts = 0
        self._delivery_failures = 0

    # ------------------------------------------------------------------
    @property
    def metrics(self) -> Dict[str, int]:
        return {
            "delivery_attempts": self._delivery_attempts,
            "delivery_failures": self._delivery_failures,
        }

    # ------------------------------------------------------------------
    def deliver_batch(self, batch: DeliveryBatch) -> None:
        payload = self._build_payload(batch)
        batch_key = self._batch_idempotency_key(batch)
        attempt = 0
        last_exc: Optional[Exception] = None

        while True:
            attempt += 1
            self._delivery_attempts += 1
            try:
                self._post_once(payload, batch_key, attempt)
                return
            except _PermanentHttpError as exc:
                # 4xx - re-raise without retry (programmer error)
                _LOG.error("Permanent webhook error (HTTP %s): %s", exc.status_code, exc)
                raise
            except Exception as exc:  # transient: network error, 5xx, timeout
                last_exc = exc
                self._delivery_failures += 1
                if attempt > self._max_retries:
                    _LOG.error(
                        "Webhook delivery failed after %d attempts; giving up",
                        attempt,
                    )
                    raise
                backoff = min(self._init_backoff * (2 ** (attempt - 1)), self._max_backoff)
                _LOG.warning(
                    "Webhook delivery attempt %d/%d failed (%s); retrying in %.1fs",
                    attempt,
                    self._max_retries,
                    exc,
                    backoff,
                )
                time.sleep(backoff)

    # ------------------------------------------------------------------
    def _build_payload(self, batch: DeliveryBatch) -> Dict[str, Any]:
        events = [ev.to_dict() for ev in batch.events]
        max_pos = batch.max_position()
        tx_ids = {e.transaction_id for e in batch.events if e.transaction_id}
        return {
            "batch_id": self._batch_id(batch),
            "source": batch.source,
            "event_count": len(batch.events),
            "max_binlog_file": max_pos.binlog_file if max_pos else None,
            "max_binlog_position": max_pos.position if max_pos else None,
            "transaction_id": next(iter(tx_ids)) if len(tx_ids) == 1 else None,
            "events": events,
        }

    def _post_once(self, payload: Dict, batch_key: str, attempt: int) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-CDC-Idempotency-Key": batch_key,
            "X-CDC-Retry-Attempt": str(attempt),
        }
        headers.update(self._extra_headers)
        req = request.Request(self._url, data=body, headers=headers, method="POST")
        try:
            with request.urlopen(req, timeout=self._timeout) as resp:
                status = resp.status
                if 400 <= status < 500:
                    raise _PermanentHttpError(status, resp.read(4096).decode("utf-8", "replace"))
                if 500 <= status < 600:
                    raise error.HTTPError(
                        self._url, status, "Server error", resp.headers, None
                    )
        except _PermanentHttpError:
            raise
        except error.HTTPError as exc:
            if 400 <= exc.code < 500:
                raise _PermanentHttpError(exc.code, str(exc)) from exc
            raise  # treat 5xx as transient


    @staticmethod
    def _batch_id(batch: DeliveryBatch) -> str:
        import hashlib
        keys = sorted(e.event_id for e in batch.events)
        return hashlib.sha256("|".join(keys).encode()).hexdigest()

    @staticmethod
    def _batch_idempotency_key(batch: DeliveryBatch) -> str:
        import hashlib
        keys = sorted(e.idempotency_key or "" for e in batch.events)
        return hashlib.sha256("|".join(keys).encode()).hexdigest()


class _PermanentHttpError(Exception):
    def __init__(self, status_code: int, body: str) -> None:
        super().__init__(f"HTTP {status_code}: {body[:200]}")
        self.status_code = status_code
        self.body = body

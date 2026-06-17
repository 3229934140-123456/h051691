"""Sink factory - constructs sinks from a configuration dict."""

from __future__ import annotations

from typing import Any, Dict

from ..downstream.consumer import (
    DownstreamConsumer,
    InMemoryDownstreamConsumer,
    LoggingDownstreamConsumer,
)
from .jsonl_sink import JsonlFileSink
from .webhook_sink import HttpWebhookSink


def build_sink_from_config(sink_config: Dict[str, Any]) -> DownstreamConsumer:
    """Construct a :class:`DownstreamConsumer` from a config dict.

    Supported types (select via ``type`` key):

    * ``"memory"``   -> :class:`InMemoryDownstreamConsumer`
    * ``"log"``      -> :class:`LoggingDownstreamConsumer`
    * ``"jsonl"``    -> :class:`JsonlFileSink`  (requires ``path``)
    * ``"webhook"``  -> :class:`HttpWebhookSink` (requires ``url``)
    * ``"stdout"``   -> :class:`JsonlFileSink` writing to stdout
    """
    sink_type = sink_config.get("type", "log").lower()

    if sink_type == "memory":
        return InMemoryDownstreamConsumer()
    if sink_type == "log":
        return LoggingDownstreamConsumer()
    if sink_type == "stdout":
        return JsonlFileSink(file_path="-")
    if sink_type == "jsonl":
        path = sink_config["path"]
        index_path = sink_config.get("dedupe_index_path")
        append = sink_config.get("append", True)
        return JsonlFileSink(
            file_path=path,
            dedupe_index_path=index_path,
            append=append,
        )
    if sink_type == "webhook":
        return HttpWebhookSink(
            url=sink_config["url"],
            timeout_seconds=float(sink_config.get("timeout_seconds", 10.0)),
            max_retries=int(sink_config.get("max_retries", 10)),
            initial_backoff_seconds=float(sink_config.get("initial_backoff_seconds", 0.5)),
            max_backoff_seconds=float(sink_config.get("max_backoff_seconds", 30.0)),
            extra_headers=dict(sink_config.get("extra_headers", {})),
        )
    raise ValueError(f"Unknown sink type: {sink_type!r}")

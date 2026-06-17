"""Offset management module - guarantees "no loss, no duplication".

The offset manager is the CDC pipeline's bookkeeping heart.  It answers
the question: *"which binlog position have we successfully delivered so
that, after a crash, we can resume exactly from there without replaying
already-seen events and without missing any?"*

Guarantees
----------
* Offsets only ever move FORWARD (monotonic).  Calling :meth:`ack` with a
  position earlier than the already-committed one is a no-op.  This
  protects the pipeline against out-of-order acks from downstream.
* Offsets are only persisted AFTER the downstream stage has confirmed
  successful delivery.  The pipeline therefore delivers at-least-once by
  construction -- a crash between a successful downstream write and the
  subsequent :meth:`ack`/flush simply causes a small amount of replay on
  restart, which the downstream stage deduplicates using the
  idempotency key in every event.
* On startup the manager reads the last persisted offset from disk (or
  wherever) and returns it so the log parser can seek to it.  If no
  offset has ever been committed the manager returns ``None``, which
  tells the parser to either run a full snapshot or start at the
  current tail of the log depending on the pipeline configuration.
"""

from .manager import OffsetManager, FileOffsetStore

__all__ = ["OffsetManager", "FileOffsetStore"]

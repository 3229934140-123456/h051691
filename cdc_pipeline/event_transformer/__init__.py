"""Event transformer module.

The log parser emits *raw* row changes that carry only *positional* column
values -- i.e. ``after_row = [1, "alice", 30]``.  The transformer is the
stage that turns these into fully-described :class:`RowChangeEvent`
objects whose column lists carry the column names, data types, and
primary-key information looked up in the :class:`SchemaTracker`.

The transformer also produces :class:`EventEnvelope` wrappers that carry
the binlog position, the transaction id, and the deterministically-built
idempotency key used by the downstream stage to de-duplicate replays.
"""

from .transformer import EventTransformer

__all__ = ["EventTransformer"]

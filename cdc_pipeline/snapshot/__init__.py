"""Initial full snapshot module.

The first time a CDC pipeline is started against a database that already
contains data it must first capture a *full snapshot* of every row of
every tracked table.  The snapshot needs to be *consistent*: every row
must appear as it existed at a single logical point in time, and that
point in time must be exactly the binlog position at which incremental
binlog streaming will later resume.

The industry-standard algorithm (used by Debezium, Maxwell, Canal and
others) works like this:

1. Open a REPEATABLE READ transaction with ``WITH CONSISTENT SNAPSHOT``
   so that all subsequent ``SELECT``s in that transaction read from the
   same MVCC point-in-time view.
2. Inside that transaction, run ``SHOW MASTER STATUS`` and record the
   current (file, position) pair.  This is the *snapshot position*.
3. Still inside the same transaction, walk every tracked table with
   ``SELECT * FROM table`` and stream the resulting rows into snapshot
   events that are delivered to the downstream consumer.  Each row is
   tagged with the *same* snapshot position and carries an idempotency
   key built from the primary key.
4. After the last table has been read, close the transaction.
5. Switch over to incremental binlog streaming starting at the exact
   (file, position) recorded in step 2.

The overlap problem
-------------------
Because step 2 is executed *before* step 3, any row change that happens
between steps 2 and 5 will be captured *twice*: once inside the snapshot
SELECT and once as an incremental binlog event.  This is *intentional*.
Every event -- whether snapshot or incremental -- carries a
deterministic idempotency key, so the downstream consumer can trivially
deduplicate the overlap by keying on that value.  This eliminates the
need to hold an exclusive global table lock for the entire snapshot,
which would be untenable on a live production system.

The snapshot module below provides an abstract interface and a reference
in-memory implementation used by the demo and test suite.
"""

from .snapshotter import Snapshotter, InMemorySnapshotProvider

__all__ = ["Snapshotter", "InMemorySnapshotProvider"]

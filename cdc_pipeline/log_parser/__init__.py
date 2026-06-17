"""Log parser module - reads binary database transaction logs.

This module implements the first stage of the CDC pipeline: converting the
raw binary stream produced by the source database (MySQL Binlog in ROW
format, or PostgreSQL WAL) into typed "raw change records" that downstream
stages can reason about.

Key concepts
------------
MySQL Binlog (ROW format) records changes as:
  1. TABLE_MAP_EVENT        -> maps (table_id) -> (db, table, columns, PK)
  2. WRITE_ROWS_EVENTv2     -> one or more INSERTed rows, referencing a table_id
  3. UPDATE_ROWS_EVENTv2    -> (before_row, after_row) pairs
  4. DELETE_ROWS_EVENTv2    -> rows that were removed
  5. QUERY_EVENT            -> carries BEGIN / COMMIT / ROLLBACK / raw DDL
  6. XID_EVENT              -> marks transaction commit (InnoDB)
  7. ROTATE_EVENT           -> signals binlog file rotation
  8. GTID_LOG_EVENT         -> Global Transaction Identifier (if GTID mode on)

Because the binary format is stable we do not need the full MySQL protocol
stack to demonstrate the CDC pipeline; this module provides both a
*production-capable* adapter (driven by the real `pymysqlreplication` lib
when available) and a *self-contained in-memory simulator* that is used by
the unit tests and by the demo main entry point so the pipeline can be
exercised without a live database.
"""

from .base import (
    RawRowChange,
    RawDDLChange,
    RawTransactionBegin,
    RawTransactionCommit,
    BinlogPosition,
    LogParser,
    LogParserCallback,
)
from .mysql_binlog import MySQLBinlogParser
from .simulator import SimulatedLogStream

__all__ = [
    "RawRowChange",
    "RawDDLChange",
    "RawTransactionBegin",
    "RawTransactionCommit",
    "BinlogPosition",
    "LogParser",
    "LogParserCallback",
    "MySQLBinlogParser",
    "SimulatedLogStream",
]

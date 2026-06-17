"""CDC event data models.

The event hierarchy captures different kinds of changes flowing through the
pipeline:
  - RowChangeEvent       : a single row INSERT/UPDATE/DELETE
  - TransactionBeginEvent: marks the start of a database transaction
  - TransactionCommitEvent: marks the successful commit of a transaction
  - SchemaChangeEvent    : a DDL that modifies table structure
  - SnapshotEvent        : a row emitted during the initial snapshot phase

All events are wrapped in EventEnvelope which carries the offset, source
metadata, and an idempotency key used for downstream deduplication.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional


class ChangeType(Enum):
    """Row-level operation type."""

    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    READ = "READ"


@dataclass
class ColumnValue:
    """A single column's before/after value together with type info."""

    name: str
    value: Any
    data_type: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "value": self.value, "data_type": self.data_type}


@dataclass
class RowChangeEvent:
    """A single row-level INSERT / UPDATE / DELETE."""

    database: str
    table: str
    change_type: ChangeType
    before_columns: List[ColumnValue] = field(default_factory=list)
    after_columns: List[ColumnValue] = field(default_factory=list)
    primary_key: Dict[str, Any] = field(default_factory=dict)

    def before_dict(self) -> Dict[str, Any]:
        return {c.name: c.value for c in self.before_columns}

    def after_dict(self) -> Dict[str, Any]:
        return {c.name: c.value for c in self.after_columns}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "database": self.database,
            "table": self.table,
            "change_type": self.change_type.value,
            "before_columns": [c.to_dict() for c in self.before_columns],
            "after_columns": [c.to_dict() for c in self.after_columns],
            "primary_key": self.primary_key,
        }


@dataclass
class TransactionBeginEvent:
    """Marks the start of a logical database transaction."""

    transaction_id: str
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp,
        }


@dataclass
class TransactionCommitEvent:
    """Marks the successful commit of a transaction."""

    transaction_id: str
    timestamp: float
    row_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp,
            "row_count": self.row_count,
        }


@dataclass
class SchemaChangeEvent:
    """A DDL change that alters a table's schema."""

    database: str
    table: str
    ddl_statement: str
    schema_version: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "database": self.database,
            "table": self.table,
            "ddl_statement": self.ddl_statement,
            "schema_version": self.schema_version,
        }


@dataclass
class SnapshotEvent:
    """A row emitted during the initial full snapshot phase."""

    database: str
    table: str
    columns: List[ColumnValue] = field(default_factory=list)
    primary_key: Dict[str, Any] = field(default_factory=dict)

    def row_dict(self) -> Dict[str, Any]:
        return {c.name: c.value for c in self.columns}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "database": self.database,
            "table": self.table,
            "columns": [c.to_dict() for c in self.columns],
            "primary_key": self.primary_key,
        }


@dataclass
class EventEnvelope:
    """Envelope wrapping any CDC event with source, offset and delivery metadata.

    Attributes:
        event_id      : globally unique id for the event, used for dedup.
        event_type    : one of 'row' | 'tx_begin' | 'tx_commit' | 'schema' | 'snapshot'.
        event         : the typed inner event payload.
        source        : 'mysql' | 'postgres' | ...
        binlog_file / position / gtid : capture position in the upstream log.
        timestamp     : when the event was observed (seconds since epoch).
        transaction_id: id of the enclosing transaction (if any).
        idempotency_key: deterministic, stable key for downstream de-duplication.
                         Built from (source, binlog pos, event type, pk).
    """

    event_id: str
    event_type: str
    event: Any
    source: str
    binlog_file: Optional[str] = None
    position: Optional[int] = None
    gtid: Optional[str] = None
    timestamp: float = 0.0
    transaction_id: Optional[str] = None
    idempotency_key: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        inner = self.event.to_dict() if hasattr(self.event, "to_dict") else self.event
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event": inner,
            "source": self.source,
            "binlog_file": self.binlog_file,
            "position": self.position,
            "gtid": self.gtid,
            "timestamp": self.timestamp,
            "transaction_id": self.transaction_id,
            "idempotency_key": self.idempotency_key,
        }


def make_event_id() -> str:
    return uuid.uuid4().hex


def build_idempotency_key(
    source: str,
    binlog_file: Optional[str],
    position: Optional[int],
    event_type: str,
    pk: Optional[Dict[str, Any]] = None,
) -> str:
    """Build a stable, deterministic idempotency key.

    The same upstream log position + event type + pk always produces the same
    key so downstream systems can safely re-apply the event any number of
    times and observe the same outcome.
    """
    pk_str = ""
    if pk:
        pk_str = "|".join(f"{k}={pk[k]}" for k in sorted(pk.keys()))
    return f"{source}:{binlog_file or ''}:{position or 0}:{event_type}:{pk_str}"

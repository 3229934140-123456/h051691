"""Schema-related data models."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


@dataclass
class ColumnDef:
    """Definition of a single column in a table schema."""

    name: str
    data_type: str
    is_primary_key: bool = False
    is_nullable: bool = True
    default_value: Optional[Any] = None
    character_set: Optional[str] = None
    collation: Optional[str] = None
    column_type_extra: Optional[str] = None
    ordinal_position: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnDef":
        return cls(**data)


@dataclass
class TableSchema:
    """Complete schema definition for a single table."""

    database: str
    table: str
    columns: List[ColumnDef]
    primary_key_columns: List[str] = field(default_factory=list)
    version: int = 1
    captured_at: float = 0.0

    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.table}"

    def get_column(self, name: str) -> Optional[ColumnDef]:
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def primary_key_values_from_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Extract primary key values from a row dict."""
        return {pk: row.get(pk) for pk in self.primary_key_columns}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "database": self.database,
            "table": self.table,
            "columns": [c.to_dict() for c in self.columns],
            "primary_key_columns": self.primary_key_columns,
            "version": self.version,
            "captured_at": self.captured_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableSchema":
        columns = [ColumnDef.from_dict(c) for c in data.get("columns", [])]
        return cls(
            database=data["database"],
            table=data["table"],
            columns=columns,
            primary_key_columns=data.get("primary_key_columns", []),
            version=data.get("version", 1),
            captured_at=data.get("captured_at", 0.0),
        )

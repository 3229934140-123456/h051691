"""Configuration classes for the CDC pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class DatabaseConfig:
    """Source database connection configuration."""

    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = ""
    server_id: int = 1001
    connect_timeout: int = 10
    read_timeout: int = 30

    def to_dict(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "server_id": self.server_id,
            "connect_timeout": self.connect_timeout,
            "read_timeout": self.read_timeout,
        }


@dataclass
class OffsetStorageConfig:
    """Offset persistence configuration."""

    storage_type: str = "file"
    file_path: str = "./cdc_offsets.json"
    flush_interval_ms: int = 1000


@dataclass
class SchemaStorageConfig:
    """Schema history storage configuration."""

    storage_type: str = "file"
    file_path: str = "./cdc_schema_history.json"


@dataclass
class PipelineConfig:
    """Top-level CDC pipeline configuration."""

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    offset_storage: OffsetStorageConfig = field(default_factory=OffsetStorageConfig)
    schema_storage: SchemaStorageConfig = field(default_factory=SchemaStorageConfig)

    include_tables: List[str] = field(default_factory=list)
    exclude_tables: List[str] = field(default_factory=list)
    include_databases: List[str] = field(default_factory=list)
    exclude_databases: List[str] = field(default_factory=list)

    snapshot_enabled: bool = True
    snapshot_lock_timeout_seconds: int = 10
    snapshot_chunk_size: int = 1000

    downstream_batch_size: int = 100
    downstream_flush_interval_ms: int = 500

    transaction_boundary_enabled: bool = True
    idempotency_key_enabled: bool = True

    def should_include_table(self, db: str, table: str) -> bool:
        """Determine whether a given db.table should be captured."""
        full_name = f"{db}.{table}"
        if self.exclude_tables and full_name in self.exclude_tables:
            return False
        if self.exclude_databases and db in self.exclude_databases:
            return False
        if self.include_databases and db not in self.include_databases:
            return False
        if self.include_tables and full_name not in self.include_tables:
            return False
        return True

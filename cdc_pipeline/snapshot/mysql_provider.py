"""MySQL-backed snapshot data provider.

Talks to a real MySQL server over ``pymysql`` and performs a consistent
read snapshot using the standard Debezium algorithm:

1. Open a connection and set isolation level to ``REPEATABLE READ``.
2. Execute ``START TRANSACTION WITH CONSISTENT SNAPSHOT``.
3. Immediately run ``SHOW MASTER STATUS`` to capture the binlog position.
4. List and snapshot each table via ``SELECT *`` -- all reads see the
   same point-in-time view because they happen inside the consistent
   transaction.
5. Close the transaction (and connection).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Iterable, List, Tuple

from ..config import DatabaseConfig, PipelineConfig
from ..log_parser.base import BinlogPosition
from ..models.schema import ColumnDef, TableSchema
from .snapshotter import SnapshotDataProvider


_LOG = logging.getLogger("cdc.snapshot.mysql")


class MySQLSnapshotProvider(SnapshotDataProvider):
    """Snapshot data provider backed by a real MySQL server.

    The provider maintains a single transaction across the whole snapshot
    so every table is read from the same consistent point in time.
    Call :meth:`close` after the snapshot is done to release the
    connection.
    """

    def __init__(self, db_config: DatabaseConfig) -> None:
        self._db = db_config
        self._conn: Any = None
        self._snap_position: BinlogPosition | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        """Release the underlying MySQL connection."""
        if self._conn is not None:
            try:
                self._conn.rollback()
            except Exception:
                pass
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def __enter__(self) -> "MySQLSnapshotProvider":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_connection(self) -> Any:
        if self._conn is None:
            try:
                import pymysql  # type: ignore
            except ImportError as e:
                raise ImportError(
                    "pymysql is required for MySQL snapshot provider. "
                    "Install it with `pip install pymysql`"
                ) from e
            self._conn = pymysql.connect(
                host=self._db.host,
                port=self._db.port,
                user=self._db.user,
                password=self._db.password,
                database=self._db.database or None,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=self._db.connect_timeout,
                read_timeout=self._db.read_timeout,
            )
        return self._conn

    def _execute(self, sql: str, params: tuple = ()) -> Any:
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params)
        return cursor

    # ------------------------------------------------------------------
    # SnapshotDataProvider contract
    # ------------------------------------------------------------------
    def acquire_consistent_snapshot_position(self) -> BinlogPosition:
        conn = self._get_connection()
        with conn.cursor() as cur:
            cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            cur.execute("SHOW MASTER STATUS")
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(
                    "SHOW MASTER STATUS returned no row. Is binary logging enabled?"
                )
            # DictCursor returns dict with column names as keys
            if isinstance(row, dict):
                log_file = row.get("File") or row.get("file")
                log_pos = row.get("Position") or row.get("position")
            else:
                log_file = row[0]
                log_pos = row[1]

        pos = BinlogPosition(
            binlog_file=log_file,
            position=int(log_pos),
            timestamp=time.time(),
        )
        self._snap_position = pos
        _LOG.info(
            "Consistent snapshot established at %s:%d",
            pos.binlog_file,
            pos.position,
        )
        return pos

    def list_tables(self, config: PipelineConfig) -> List[Tuple[str, str]]:
        out: List[Tuple[str, str]] = []
        sql = (
            "SELECT TABLE_SCHEMA, TABLE_NAME "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE'"
        )
        params: list = []
        if config.include_databases:
            placeholders = ",".join(["%s"] * len(config.include_databases))
            sql += f" AND TABLE_SCHEMA IN ({placeholders})"
            params.extend(config.include_databases)

        with self._execute(sql, tuple(params)) as cur:
            rows = cur.fetchall()

        for row in rows:
            if isinstance(row, dict):
                db = row.get("TABLE_SCHEMA") or row.get("table_schema")
                tbl = row.get("TABLE_NAME") or row.get("table_name")
            else:
                db, tbl = row[0], row[1]
            if config.should_include_table(str(db), str(tbl)):
                out.append((str(db), str(tbl)))
        return out

    def fetch_schema(self, database: str, table: str) -> TableSchema:
        # Column info
        cols_sql = (
            "SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, IS_NULLABLE, "
            "COLUMN_KEY, COLUMN_DEFAULT "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION"
        )
        with self._execute(cols_sql, (database, table)) as cur:
            col_rows = cur.fetchall()

        pk_cols: List[str] = []
        columns: List[ColumnDef] = []

        for row in col_rows:
            if isinstance(row, dict):
                name = str(row.get("COLUMN_NAME") or row.get("column_name"))
                ordinal = int(row.get("ORDINAL_POSITION") or row.get("ordinal_position") or 0) - 1
                data_type = str(row.get("DATA_TYPE") or row.get("data_type"))
                is_nullable = str(row.get("IS_NULLABLE") or row.get("is_nullable") or "").upper() == "YES"
                column_key = str(row.get("COLUMN_KEY") or row.get("column_key") or "")
            else:
                name = str(row[0])
                ordinal = int(row[1]) - 1
                data_type = str(row[2])
                is_nullable = str(row[3]).upper() == "YES"
                column_key = str(row[4])

            is_pk = column_key == "PRI"
            if is_pk:
                pk_cols.append(name)
            columns.append(ColumnDef(
                name=name,
                data_type=data_type,
                is_primary_key=is_pk,
                is_nullable=is_nullable,
                ordinal_position=ordinal,
            ))

        return TableSchema(
            database=database,
            table=table,
            version=1,
            captured_at=time.time(),
            columns=columns,
            primary_key_columns=pk_cols,
        )

    def fetch_rows(
        self,
        database: str,
        table: str,
        schema: TableSchema,
        chunk_size: int,
    ) -> Iterable[List[List[Any]]]:
        """Yield chunks of rows in column ordinal order.

        Uses the consistent snapshot transaction so reads are repeatable.
        """
        col_names = ",".join(f"`{c.name}`" for c in schema.columns)
        sql = f"SELECT {col_names} FROM `{database}`.`{table}`"

        # If there's a primary key, use it for ordering and cursor-like
        # pagination.  Otherwise fall back to LIMIT/OFFSET which can be
        # slow for large tables but is correct.
        pk = schema.primary_key_columns
        if pk:
            pk_expr = ",".join(f"`{c}`" for c in pk)
            sql += f" ORDER BY {pk_expr}"
        sql += " LIMIT %s"

        offset = 0
        while True:
            chunk_sql = sql + " OFFSET %s"
            with self._execute(chunk_sql, (chunk_size, offset)) as cur:
                rows = cur.fetchall()

            if not rows:
                break

            chunk: List[List[Any]] = []
            for row in rows:
                if isinstance(row, dict):
                    row_vals = [row.get(c.name) for c in schema.columns]
                else:
                    row_vals = list(row)
                chunk.append(row_vals)
            yield chunk

            if len(chunk) < chunk_size:
                break
            offset += chunk_size

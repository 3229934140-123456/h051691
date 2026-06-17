"""Schema tracking module.

The schema tracker has two responsibilities:

1. It holds a versioned history of every (db, table) schema the pipeline
   has ever observed.  When the event transformer needs to decode a row
   image from the binlog it asks the tracker for the schema *as it was
   at that binlog position* -- NOT the latest version, because the
   pipeline may be replaying an old section of the log after a restart,
   and a column that was added last week did not exist last month.

2. It monitors the incoming DDL stream (CREATE / ALTER / DROP / RENAME
   TABLE) and keeps its versioned history up to date.  Every DDL bumps
   the table's schema version by one and persists the new entry to the
   schema history store so that on restart the full evolution of each
   table is available without re-running a snapshot.

This dual nature is what makes schema-on-read CDC work correctly: the
binlog stores only *positional* row images; the CDC pipeline must
reconstruct the column names by replaying DDLs up to the exact log
position of the row event being decoded.
"""

from .tracker import SchemaTracker, FileSchemaHistoryStore, apply_ddl_to_schema

__all__ = ["SchemaTracker", "FileSchemaHistoryStore", "apply_ddl_to_schema"]

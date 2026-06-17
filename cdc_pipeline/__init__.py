"""Change Data Capture (CDC) Pipeline.

A modular CDC system that captures row-level changes from database
transaction logs (e.g. MySQL Binlog, PostgreSQL WAL), converts them into
structured events, and delivers them to downstream consumers with
guarantees of at-least-once delivery, transactional boundaries, and
seamless schema evolution handling.
"""

from .pipeline import CDCPipeline
from .config import PipelineConfig

__all__ = ["CDCPipeline", "PipelineConfig"]
__version__ = "0.1.0"

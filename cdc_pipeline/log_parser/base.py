"""Abstract base classes for the log parser stage."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from functools import total_ordering
from typing import Any, Callable, Dict, List, Optional, Tuple


@total_ordering
@dataclass
class BinlogPosition:
    """A single position in the upstream transaction log.

    For MySQL this is (binlog_file, offset); for PostgreSQL the equivalent
    is (timeline, LSN).  We keep the fields generic enough to model either.

    The position is monotonically increasing: for any two events the one
    with the greater (file, position) tuple was observed later in the log.
    """

    binlog_file: str
    position: int
    gtid: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    def as_tuple(self) -> Tuple[str, int]:
        return (self.binlog_file, self.position)

    def __lt__(self, other: "BinlogPosition") -> bool:
        if not isinstance(other, BinlogPosition):
            return NotImplemented
        return self.as_tuple() < other.as_tuple()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BinlogPosition):
            return NotImplemented
        return self.as_tuple() == other.as_tuple()

    def __hash__(self) -> int:
        return hash(self.as_tuple())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "binlog_file": self.binlog_file,
            "position": self.position,
            "gtid": self.gtid,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BinlogPosition":
        return cls(
            binlog_file=data["binlog_file"],
            position=data["position"],
            gtid=data.get("gtid"),
            timestamp=data.get("timestamp", time.time()),
        )


@dataclass
class RawRowChange:
    """A single INSERT / UPDATE / DELETE row change as parsed from the log.

    This is the *raw* parser output: the row images are still plain
    python lists in column ordinal order.  Downstream stages enrich them
    with column names via the schema tracker before they become events.
    """

    database: str
    table: str
    operation: str
    before_row: Optional[List[Any]] = None
    after_row: Optional[List[Any]] = None
    position: Optional[BinlogPosition] = None
    transaction_id: Optional[str] = None


@dataclass
class RawDDLChange:
    """A DDL statement parsed from a QUERY_EVENT."""

    database: str
    table: Optional[str]
    ddl: str
    position: Optional[BinlogPosition] = None
    transaction_id: Optional[str] = None


@dataclass
class RawTransactionBegin:
    transaction_id: str
    position: Optional[BinlogPosition] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class RawTransactionCommit:
    transaction_id: str
    position: Optional[BinlogPosition] = None
    timestamp: float = field(default_factory=time.time)


LogParserCallback = Callable[[Any], None]


class LogParser(ABC):
    """Abstract base for log parsers."""

    @abstractmethod
    def start(
        self,
        start_position: Optional[BinlogPosition] = None,
        on_row: Optional[LogParserCallback] = None,
        on_ddl: Optional[LogParserCallback] = None,
        on_tx_begin: Optional[LogParserCallback] = None,
        on_tx_commit: Optional[LogParserCallback] = None,
    ) -> None:
        """Start consuming the log stream.

        Parameters
        ----------
        start_position : where to resume from. None means "current tail".
        on_row         : invoked for every parsed row change.
        on_ddl         : invoked for every parsed DDL.
        on_tx_begin    : invoked at logical transaction start.
        on_tx_commit   : invoked at logical transaction commit.
        """
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def is_running(self) -> bool:
        raise NotImplementedError

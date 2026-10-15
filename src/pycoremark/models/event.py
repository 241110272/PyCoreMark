"""Cluster event, retry policy, and validation models.

TODO(Member 1, Member 5): Add exponential backoff jitter configurations and schema validation rules.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class ClusterEventType(str, Enum):
    """Categorization of asynchronous cluster events."""

    NODE_REGISTERED = "NODE_REGISTERED"
    NODE_HEARTBEAT = "NODE_HEARTBEAT"
    NODE_FAILED = "NODE_FAILED"
    NODE_RECOVERED = "NODE_RECOVERED"
    TASK_SCHEDULED = "TASK_SCHEDULED"
    TASK_STARTED = "TASK_STARTED"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"
    EXPERIMENT_STARTED = "EXPERIMENT_STARTED"
    EXPERIMENT_COMPLETED = "EXPERIMENT_COMPLETED"


@dataclass
class ClusterEvent:
    """Event representation emitted across broker topics."""

    event_id: str
    event_type: ClusterEventType
    source_node_id: str
    timestamp: datetime = field(default_factory=_utc_now)
    payload: Dict[str, Any] = field(default_factory=dict)
    logical_clock: int = 0


@dataclass
class RetryPolicy:
    """Strategy configuration governing task retry execution."""

    max_retries: int = 3
    initial_backoff_seconds: float = 0.5
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 10.0
    retry_on_exceptions: tuple[type[Exception], ...] = (Exception,)


@dataclass
class ValidationResult:
    """Outcome report for configuration, benchmark, or data validation."""

    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

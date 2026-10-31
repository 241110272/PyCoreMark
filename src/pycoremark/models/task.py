"""Task, execution attempt, and result models.

TODO(Member 3): Add fine-grained task checkpointing and serialization schemas.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TaskStatus(str, Enum):
    """Execution status of a distributed task."""

    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    DEAD_LETTERED = "DEAD_LETTERED"


@dataclass
class TaskAttempt:
    """Record of a single execution attempt for a task."""

    attempt_number: int
    worker_id: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    error_message: Optional[str] = None
    trace_id: Optional[str] = None


@dataclass
class Task:
    """Unit of computational workload scheduled across the cluster."""

    task_id: str
    workload_name: str
    payload: Dict[str, Any]
    required_cores: int = 1
    required_memory_mb: int = 512
    priority: int = 0
    status: TaskStatus = TaskStatus.PENDING
    assigned_node_id: Optional[str] = None
    max_retries: int = 3
    retry_count: int = 0
    created_at: datetime = field(default_factory=_utc_now)
    attempts: list[TaskAttempt] = field(default_factory=list)
    queued_at: Optional[datetime] = None
    experiment_id: Optional[str] = None
    trace_id: Optional[str] = None
    parent_span_id: Optional[str] = None
    logical_clock: int = 0


@dataclass
class TaskResult:
    """Output summary of an executed task."""

    task_id: str
    worker_id: str
    status: TaskStatus
    output_data: Dict[str, Any] = field(default_factory=dict)
    execution_time_seconds: float = 0.0
    error_message: Optional[str] = None
    completed_at: datetime = field(default_factory=_utc_now)
    queue_wait_seconds: float = 0.0
    attempt_number: int = 1
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None
    started_at: Optional[datetime] = None

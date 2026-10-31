"""Experiment setup and aggregated outcome models.

TODO(Member 7): Add multi-trial statistical parameters and confidence interval fields.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from pycoremark.models.workload import WorkloadConfig


class ExperimentStatus(str, Enum):
    """Execution lifecycle of an experimental run."""

    CREATED = "CREATED"
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


@dataclass
class ExperimentConfig:
    """Benchmark experiment run parameters."""

    experiment_id: str
    name: str
    workload: WorkloadConfig
    scheduler_type: str = "DYNAMIC"
    target_nodes: int = 4
    repeat_count: int = 1
    description: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.experiment_id, str) or not self.experiment_id.strip():
            raise ValueError("experiment_id must be a non-empty string")
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("name must be a non-empty string")
        for field_name in ("target_nodes", "repeat_count"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{field_name} must be a positive integer")
        if not isinstance(self.scheduler_type, str) or not self.scheduler_type.strip():
            raise ValueError("scheduler_type must be a non-empty string")
        normalized_scheduler = self.scheduler_type.strip().upper().replace("-", "_")
        if normalized_scheduler not in {
            "STATIC", "DYNAMIC", "RESOURCE_AWARE", "ROUND_ROBIN", "LEAST_LOADED"
        }:
            raise ValueError(f"Unsupported scheduler_type: {self.scheduler_type}")
        self.scheduler_type = normalized_scheduler
        if not isinstance(self.workload, WorkloadConfig):
            raise TypeError("workload must be a WorkloadConfig")
        if not isinstance(self.parameters, dict):
            raise TypeError("parameters must be a dictionary")


@dataclass
class ExperimentResult:
    """Aggregated final metrics and execution data from a completed benchmark."""

    experiment_id: str
    status: ExperimentStatus
    total_tasks: int
    successful_tasks: int
    failed_tasks: int
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    throughput_tasks_per_sec: float = 0.0
    speedup: float = 1.0
    efficiency: float = 1.0
    average_latency_seconds: float = 0.0
    communication_overhead_seconds: float = 0.0
    metrics_summary: Dict[str, Any] = field(default_factory=dict)


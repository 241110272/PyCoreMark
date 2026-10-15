"""Workload specifications and definitions.

TODO(Member 3): Add memory allocation limits and streaming data parameters.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict


class WorkloadType(str, Enum):
    """Categorization of distributed benchmark workloads."""

    CPU_BOUND = "CPU_BOUND"
    DATA_PARTITIONED = "DATA_PARTITIONED"
    MIXED = "MIXED"
    CUSTOM = "CUSTOM"


@dataclass
class WorkloadConfig:
    """Configuration driving synthetic workload execution."""

    name: str
    workload_type: WorkloadType
    total_tasks: int
    task_complexity: int = 100
    dataset_size_records: int = 1000
    partitions: int = 4
    parameters: Dict[str, Any] = field(default_factory=dict)


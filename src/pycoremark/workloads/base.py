"""Base workload abstract interface.

TODO(Member 3): Add memory footprint constraints and execution timeout hooks.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List
from pycoremark.models.task import Task
from pycoremark.models.workload import WorkloadConfig


class BaseWorkload(ABC):
    """Abstract base class representing a benchmarkable distributed workload."""

    def __init__(self, config: WorkloadConfig) -> None:
        """Initialize workload with configuration specification.

        Args:
            config: WorkloadConfig defining workload characteristics.
        """
        self.config = config

    @abstractmethod
    def generate_tasks(self) -> List[Task]:
        """Decompose workload into individual dispatchable Tasks."""
        raise NotImplementedError

    @abstractmethod
    def execute_task(self, task: Task) -> Dict[str, Any]:
        """Execute a single task payload and return computation result.

        Args:
            task: Task instance to process.

        Returns:
            Dictionary containing result data.
        """
        raise NotImplementedError


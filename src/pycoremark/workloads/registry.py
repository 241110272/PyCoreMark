"""Dynamic workload registration catalog.

TODO(Member 3): Add plugin auto-discovery mechanism using entry points or decorators.
"""

from typing import Dict, Type
from pycoremark.models.task import Task
from pycoremark.models.workload import WorkloadConfig, WorkloadType
from pycoremark.workloads.base import BaseWorkload


class WorkloadRegistry:
    """Catalog of registered workload factory classes."""

    def __init__(self) -> None:
        self._registry: Dict[str, Type[BaseWorkload]] = {}

    def register(self, name: str, workload_cls: Type[BaseWorkload]) -> None:
        """Register a workload class under an identifier name."""
        self._registry[name] = workload_cls

    def get(self, name: str) -> Type[BaseWorkload]:
        """Fetch workload class by name."""
        if name not in self._registry:
            raise KeyError(f"Workload '{name}' not found in registry.")
        return self._registry[name]

    def names(self) -> tuple[str, ...]:
        """Return workload identifiers advertised by this node."""
        return tuple(sorted(self._registry))

    def create(self, config: WorkloadConfig) -> BaseWorkload:
        """Instantiate configured workload instance."""
        cls = self.get(config.name)
        return cls(config)

    def create_for_task(self, task: Task) -> BaseWorkload:
        """Create a workload instance for a self-contained task.

        Tasks carry their input payload, so a worker does not need the full
        experiment configuration to execute them. Workloads that need extra
        per-task options can read those values from ``task.payload``.
        """
        config = WorkloadConfig(
            name=task.workload_name,
            workload_type=WorkloadType.CUSTOM,
            total_tasks=1,
            parameters=dict(task.payload),
        )
        return self.create(config)


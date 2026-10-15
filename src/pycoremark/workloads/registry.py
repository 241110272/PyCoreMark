"""Dynamic workload registration catalog.

TODO(Member 3): Add plugin auto-discovery mechanism using entry points or decorators.
"""

from typing import Dict, Type
from pycoremark.models.workload import WorkloadConfig
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

    def create(self, config: WorkloadConfig) -> BaseWorkload:
        """Instantiate configured workload instance."""
        cls = self.get(config.name)
        return cls(config)


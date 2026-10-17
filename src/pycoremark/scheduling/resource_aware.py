"""Weighted CPU/RAM/queue placement with capacity and overload guards."""

from typing import List, Optional

from pycoremark.models.node import NodeInfo
from pycoremark.models.task import Task
from pycoremark.scheduling.base import BaseScheduler


class ResourceAwareScheduler(BaseScheduler):

    def __init__(
        self,
        w_cpu: float = 0.4,
        w_ram: float = 0.4,
        w_queue: float = 0.2,
        cpu_threshold: float = 0.95,
        ram_threshold: float = 0.95,
        queue_capacity: int = 100,
        metrics_max_age_seconds: float = 30.0,
    ) -> None:
        super().__init__(queue_capacity, metrics_max_age_seconds)
        for name, value in (("w_cpu", w_cpu), ("w_ram", w_ram), ("w_queue", w_queue)):
            self._validate_number(value, name)
        # Scale first to avoid overflow with very large finite weights.
        largest = max(w_cpu, w_ram, w_queue)
        if largest == 0:
            raise ValueError("At least one weight must be positive")
        scaled = [value / largest for value in (w_cpu, w_ram, w_queue)]
        total = sum(scaled)
        self._weights = tuple(value / total for value in scaled)
        for name, value in (("cpu_threshold", cpu_threshold), ("ram_threshold", ram_threshold)):
            self._validate_number(value, name)
            if not 0 < value <= 1:
                raise ValueError(f"{name} must be in (0, 1]")
        self.cpu_threshold = cpu_threshold
        self.ram_threshold = ram_threshold

    def _score(self, task: Task, node: NodeInfo) -> Optional[float]:
        resources = node.resources
        values = (resources.total_cores, resources.available_cores,
                  resources.total_memory_mb, resources.available_memory_mb)
        try:
            for value in values:
                self._validate_number(value, "node resource")
        except ValueError:
            return None
        cores, free_cores, memory, free_memory = values
        if cores == 0 or memory == 0 or free_cores > cores or free_memory > memory:
            return None
        pending = self._pending(node)
        reserved_cores = sum(item.cores for item in pending)
        reserved_memory = sum(item.memory_mb for item in pending)
        metrics = self._metrics_for(node)
        cpu = 1 - free_cores / cores
        ram = 1 - free_memory / memory
        ram_total = memory
        if metrics is not None:
            if metrics.cpu_percent is not None:
                cpu = metrics.cpu_percent / 100
            if metrics.memory_total_mb and metrics.memory_used_mb is not None:
                ram = metrics.memory_used_mb / metrics.memory_total_mb
                ram_total = metrics.memory_total_mb
                free_memory = min(free_memory, metrics.memory_total_mb - metrics.memory_used_mb)
        if free_cores - reserved_cores < task.required_cores:
            return None
        if free_memory - reserved_memory < task.required_memory_mb:
            return None
        cpu = min(1.0, cpu + reserved_cores / cores)
        ram = min(1.0, ram + reserved_memory / ram_total)
        if cpu >= self.cpu_threshold or ram >= self.ram_threshold:
            return None
        queued, capacity = self._queue(node)
        return sum(weight * value for weight, value in zip(self._weights, (cpu, ram, queued / capacity)))

    def _select_node(self, task: Task, candidates: List[NodeInfo]) -> Optional[NodeInfo]:
        best_node = None
        best_score = float("inf")
        for node in candidates:
            score = self._score(task, node)
            if score is not None and score < best_score:
                best_node, best_score = node, score
        return best_node

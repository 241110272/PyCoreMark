"""Static task placement using Round-Robin."""

from typing import List, Optional

from pycoremark.models.node import NodeInfo
from pycoremark.models.task import Task
from pycoremark.scheduling.base import BaseScheduler


class StaticScheduler(BaseScheduler):
    def __init__(self, queue_capacity: int = 100, metrics_max_age_seconds: float = 30.0) -> None:
        super().__init__(queue_capacity, metrics_max_age_seconds)
        self._last_node_id: Optional[str] = None

    def _select_node(self, task: Task, candidates: List[NodeInfo]) -> Optional[NodeInfo]:
        """Choose the next eligible node, independent of load scores."""
        if not candidates:
            return None
        ids = [node.node_id for node in candidates]
        start = (ids.index(self._last_node_id) + 1) % len(ids) if self._last_node_id in ids else 0
        target = candidates[start]
        self._last_node_id = target.node_id
        return target

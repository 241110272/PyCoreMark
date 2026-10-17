"""Dynamic task placement using Least-Loaded."""

from typing import List, Optional

from pycoremark.models.node import NodeInfo
from pycoremark.models.task import Task
from pycoremark.scheduling.base import BaseScheduler


class DynamicScheduler(BaseScheduler):
    def _select_node(self, task: Task, candidates: List[NodeInfo]) -> Optional[NodeInfo]:
        return min(candidates, key=self._load, default=None)

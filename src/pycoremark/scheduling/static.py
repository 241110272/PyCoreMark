"""Static deterministic scheduler (Round-Robin).

TODO(Member 4): Implement consistent hashing and weighted static partitioning.
"""

from typing import List
from pycoremark.models.node import NodeInfo
from pycoremark.models.task import Task
from pycoremark.scheduling.base import BaseScheduler, SchedulingDecision


class StaticScheduler(BaseScheduler):
    """Assigns tasks to nodes using deterministic Round-Robin strategy."""

    def __init__(self) -> None:
        self._index = 0

    def schedule(
        self,
        task: Task,
        available_nodes: List[NodeInfo],
    ) -> SchedulingDecision:
        """Schedule single task using round-robin index."""
        # TODO(Member 4): Validate node state and handle empty node lists
        if not available_nodes:
            return SchedulingDecision(task_id=task.task_id, target_node_id=None, reason="No nodes available")

        target = available_nodes[self._index % len(available_nodes)]
        self._index += 1
        return SchedulingDecision(
            task_id=task.task_id,
            target_node_id=target.node_id,
            reason="Static round-robin allocation",
        )

    def schedule_batch(
        self,
        tasks: List[Task],
        available_nodes: List[NodeInfo],
    ) -> List[SchedulingDecision]:
        """Schedule task batch round-robin."""
        return [self.schedule(t, available_nodes) for t in tasks]


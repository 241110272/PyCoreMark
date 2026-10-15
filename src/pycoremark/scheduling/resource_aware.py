"""Resource-aware scheduler considering CPU cores and memory limits.

TODO(Member 4): Implement multi-dimensional bin-packing and locality-aware scoring.
"""

from typing import List
from pycoremark.models.node import NodeInfo
from pycoremark.models.task import Task
from pycoremark.scheduling.base import BaseScheduler, SchedulingDecision


class ResourceAwareScheduler(BaseScheduler):
    """Schedules tasks matching hardware resource requirements (cores & memory)."""

    def schedule(
        self,
        task: Task,
        available_nodes: List[NodeInfo],
    ) -> SchedulingDecision:
        """Find a node with sufficient remaining cores and memory capacity."""
        # TODO(Member 4): Track dynamic decrements of available_cores and memory reservations
        candidates = [
            node for node in available_nodes
            if (
                node.resources.available_cores >= task.required_cores
                and node.resources.available_memory_mb >= task.required_memory_mb
            )
        ]

        if not candidates:
            return SchedulingDecision(
                task_id=task.task_id,
                target_node_id=None,
                reason="No node satisfies required cores/memory constraints",
            )

        # Select candidate with the highest remaining cores (best fit / balance)
        best_node = max(candidates, key=lambda n: n.resources.available_cores)
        return SchedulingDecision(
            task_id=task.task_id,
            target_node_id=best_node.node_id,
            reason="Resource capacity matched",
        )

    def schedule_batch(
        self,
        tasks: List[Task],
        available_nodes: List[NodeInfo],
    ) -> List[SchedulingDecision]:
        """Schedule batch matching resources for each task."""
        return [self.schedule(t, available_nodes) for t in tasks]


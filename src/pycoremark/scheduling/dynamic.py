"""Dynamic load-aware scheduler (Least Loaded).

TODO(Member 4): Implement feedback-driven dynamic load estimation and work-stealing.
"""

from typing import Dict, List
from pycoremark.models.node import NodeInfo
from pycoremark.models.task import Task
from pycoremark.scheduling.base import BaseScheduler, SchedulingDecision


class DynamicScheduler(BaseScheduler):
    """Assigns tasks to nodes based on runtime active load or queue length."""

    def __init__(self) -> None:
        self._node_assigned_counts: Dict[str, int] = {}

    def schedule(
        self,
        task: Task,
        available_nodes: List[NodeInfo],
    ) -> SchedulingDecision:
        """Assign task to the node currently having minimum assigned tasks."""
        # TODO(Member 4): Factor in real-time heartbeat CPU metrics rather than counts alone
        if not available_nodes:
            return SchedulingDecision(task_id=task.task_id, target_node_id=None, reason="No nodes available")

        # Pick node with lowest assigned load
        best_node = min(
            available_nodes,
            key=lambda n: self._node_assigned_counts.get(n.node_id, 0),
        )
        self._node_assigned_counts[best_node.node_id] = (
            self._node_assigned_counts.get(best_node.node_id, 0) + 1
        )

        return SchedulingDecision(
            task_id=task.task_id,
            target_node_id=best_node.node_id,
            reason=f"Least loaded node (active count={self._node_assigned_counts[best_node.node_id]})",
        )

    def schedule_batch(
        self,
        tasks: List[Task],
        available_nodes: List[NodeInfo],
    ) -> List[SchedulingDecision]:
        """Schedule batch dynamically."""
        return [self.schedule(t, available_nodes) for t in tasks]


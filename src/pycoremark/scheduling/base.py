"""Scheduler abstraction and scheduling decision models.

TODO(Member 4): Add multi-objective optimization metrics and priority preemption hooks.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional
from pycoremark.models.node import NodeInfo
from pycoremark.models.task import Task


@dataclass
class SchedulingDecision:
    """Outcome of scheduling evaluation for a single task."""

    task_id: str
    target_node_id: Optional[str]
    reason: str = ""
    estimated_queue_delay_ms: float = 0.0


class BaseScheduler(ABC):
    """Abstract interface defining task placement onto cluster nodes."""

    @abstractmethod
    def schedule(
        self,
        task: Task,
        available_nodes: List[NodeInfo],
    ) -> SchedulingDecision:
        """Decide target node placement for a pending task.

        Args:
            task: Task pending allocation.
            available_nodes: List of candidate worker nodes.

        Returns:
            SchedulingDecision containing target node id.
        """
        raise NotImplementedError

    @abstractmethod
    def schedule_batch(
        self,
        tasks: List[Task],
        available_nodes: List[NodeInfo],
    ) -> List[SchedulingDecision]:
        """Schedule a batch of tasks across candidate nodes.

        Args:
            tasks: List of pending tasks.
            available_nodes: Candidate nodes.

        Returns:
            List of SchedulingDecisions.
        """
        raise NotImplementedError


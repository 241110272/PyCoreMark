"""Task execution result aggregator.

TODO(Member 7): Add partial aggregation streaming and real-time completion progress callbacks.
"""

from typing import Dict, List, Optional
from pycoremark.models.task import TaskResult


class ResultAggregator:
    """Collects and organizes incoming TaskResult objects for an active experiment."""

    def __init__(self) -> None:
        self._results: Dict[str, TaskResult] = {}

    def add_result(self, result: TaskResult) -> None:
        """Record an incoming task result."""
        self._results[result.task_id] = result

    def get_result(self, task_id: str) -> Optional[TaskResult]:
        """Fetch result for specific task."""
        return self._results.get(task_id)

    def get_all_results(self) -> List[TaskResult]:
        """Return all aggregated results."""
        return list(self._results.values())

    def get_completed_count(self) -> int:
        """Count received task results."""
        return len(self._results)

    def is_complete(self, expected_total: int) -> bool:
        """Check if all expected task results have arrived."""
        return len(self._results) >= expected_total


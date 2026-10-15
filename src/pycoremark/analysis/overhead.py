"""Communication and orchestration overhead calculation.

TODO(Member 6): Disaggregate serialization delay, network RTT, and queue waiting time.
"""

from typing import List
from pycoremark.models.task import TaskResult


class OverheadCalculator:
    """Calculates network and scheduling overhead versus raw compute time."""

    @staticmethod
    def calculate_communication_overhead(
        total_experiment_duration: float,
        task_results: List[TaskResult],
    ) -> float:
        """Estimate cluster communication and scheduling overhead time."""
        # Total cluster execution wall time minus active computation time
        total_compute_time = sum(r.execution_time_seconds for r in task_results)
        # Placeholder estimation: overhead = total elapsed wall time - (sum(compute) / workers)
        overhead = max(0.0, total_experiment_duration - (total_compute_time / max(1, len(task_results))))
        return round(overhead, 4)


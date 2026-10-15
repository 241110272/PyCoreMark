"""Performance metric calculators: Throughput, Speedup, Efficiency, Load Balance.

TODO(Member 6): Add Gustafson's Law evaluation in addition to Amdahl's Law.
"""

from typing import Dict, List


class ThroughputCalculator:
    """Calculates completed tasks per second."""

    @staticmethod
    def calculate(completed_tasks: int, total_duration_seconds: float) -> float:
        """Calculate throughput (tasks/second)."""
        if total_duration_seconds <= 0:
            return 0.0
        return completed_tasks / total_duration_seconds


class SpeedupCalculator:
    """Calculates parallel speedup: S = T_baseline / T_parallel."""

    @staticmethod
    def calculate(single_node_duration: float, cluster_duration: float) -> float:
        """Calculate speedup factor."""
        if cluster_duration <= 0 or single_node_duration <= 0:
            return 1.0
        return single_node_duration / cluster_duration


class EfficiencyCalculator:
    """Calculates parallel efficiency: E = Speedup / N_nodes."""

    @staticmethod
    def calculate(speedup: float, node_count: int) -> float:
        """Calculate efficiency ratio (typically between 0.0 and 1.0)."""
        if node_count <= 0:
            return 0.0
        return speedup / node_count


class LoadBalanceCalculator:
    """Measures task distribution variance across nodes (Gini index or coefficient of variation)."""

    @staticmethod
    def calculate_imbalance_ratio(tasks_per_node: Dict[str, int]) -> float:
        """Calculate ratio of max tasks to average tasks per node."""
        if not tasks_per_node:
            return 0.0
        counts = list(tasks_per_node.values())
        avg_tasks = sum(counts) / len(counts)
        if avg_tasks == 0:
            return 0.0
        return max(counts) / avg_tasks


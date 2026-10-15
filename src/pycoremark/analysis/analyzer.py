"""Unified performance analyzer orchestrating calculations into ExperimentResult.

TODO(Member 6): Add historical trend comparison and percentile distributions (p50, p95, p99).
"""

from typing import Dict, List
from pycoremark.analysis.metrics_calc import (
    EfficiencyCalculator,
    LoadBalanceCalculator,
    SpeedupCalculator,
    ThroughputCalculator,
)
from pycoremark.analysis.overhead import OverheadCalculator
from pycoremark.models.experiment import ExperimentConfig, ExperimentResult, ExperimentStatus
from pycoremark.models.task import TaskResult


class PerformanceAnalyzer:
    """Aggregates raw task results and cluster telemetry into benchmark metrics."""

    def __init__(
        self,
        throughput_calc: ThroughputCalculator,
        speedup_calc: SpeedupCalculator,
        efficiency_calc: EfficiencyCalculator,
        load_balance_calc: LoadBalanceCalculator,
        overhead_calc: OverheadCalculator,
    ) -> None:
        """Initialize analyzer with injected metric calculation engines."""
        self.throughput_calc = throughput_calc
        self.speedup_calc = speedup_calc
        self.efficiency_calc = efficiency_calc
        self.load_balance_calc = load_balance_calc
        self.overhead_calc = overhead_calc

    def analyze(
        self,
        config: ExperimentConfig,
        task_results: List[TaskResult],
        duration_seconds: float,
        baseline_single_node_duration: float = 0.0,
    ) -> ExperimentResult:
        """Compute aggregated metrics and package into ExperimentResult."""
        total_tasks = len(task_results)
        successful_tasks = sum(1 for r in task_results if r.status.value == "COMPLETED")
        failed_tasks = total_tasks - successful_tasks

        throughput = self.throughput_calc.calculate(successful_tasks, duration_seconds)
        speedup = self.speedup_calc.calculate(baseline_single_node_duration or duration_seconds, duration_seconds)
        efficiency = self.efficiency_calc.calculate(speedup, config.target_nodes)
        overhead = self.overhead_calc.calculate_communication_overhead(duration_seconds, task_results)

        # Compute average task latency
        avg_latency = (
            sum(r.execution_time_seconds for r in task_results) / total_tasks
            if total_tasks > 0 else 0.0
        )

        return ExperimentResult(
            experiment_id=config.experiment_id,
            status=ExperimentStatus.COMPLETED if failed_tasks == 0 else ExperimentStatus.FAILED,
            total_tasks=total_tasks,
            successful_tasks=successful_tasks,
            failed_tasks=failed_tasks,
            start_time=task_results[0].completed_at if task_results else config.workload.parameters.get("start"),
            duration_seconds=duration_seconds,
            throughput_tasks_per_sec=round(throughput, 2),
            speedup=round(speedup, 2),
            efficiency=round(efficiency, 2),
            average_latency_seconds=round(avg_latency, 4),
            communication_overhead_seconds=overhead,
        )


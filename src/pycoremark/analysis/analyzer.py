"""Unified performance analyzer orchestrating calculations into ExperimentResult.

TODO(Member 6): Add historical trend comparison and percentile distributions (p50, p95, p99).
"""

from datetime import datetime, timezone
from typing import List

from pycoremark.analysis.metrics_calc import (
    EfficiencyCalculator,
    LoadBalanceCalculator,
    SpeedupCalculator,
    ThroughputCalculator,
)
from pycoremark.analysis.overhead import OverheadCalculator
from pycoremark.models.experiment import (
    ExperimentConfig,
    ExperimentResult,
    ExperimentStatus,
)
from pycoremark.models.task import TaskResult, TaskStatus


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
        completed_results = [
            r for r in task_results if r.status == TaskStatus.COMPLETED
        ]
        successful_tasks = len(completed_results)
        failed_tasks = total_tasks - successful_tasks

        throughput = self.throughput_calc.calculate(successful_tasks, duration_seconds)
        speedup = self.speedup_calc.calculate(
            baseline_single_node_duration or duration_seconds,
            duration_seconds,
        )
        efficiency = self.efficiency_calc.calculate(speedup, config.target_nodes)
        overhead = self.overhead_calc.calculate_communication_overhead(
            duration_seconds,
            task_results,
            num_workers=config.target_nodes,
        )

        # Latency hanya dari task yang benar-benar selesai.
        if completed_results:
            avg_latency = sum(
                r.execution_time_seconds for r in completed_results
            ) / len(completed_results)
        else:
            avg_latency = 0.0

        # Rekonstruksi start_time dari task pertama yang selesai, fallback ke now.
        if completed_results:
            first = completed_results[0]
            start_time = first.completed_at
            # completed_at - execution_time = perkiraan waktu mulai task.
            from datetime import timedelta
            start_time = first.completed_at - timedelta(
                seconds=first.execution_time_seconds
            )
        else:
            start_time = datetime.now(timezone.utc)

        status = (
            ExperimentStatus.COMPLETED
            if failed_tasks == 0 and total_tasks > 0
            else ExperimentStatus.FAILED
        )

        return ExperimentResult(
            experiment_id=config.experiment_id,
            status=status,
            total_tasks=total_tasks,
            successful_tasks=successful_tasks,
            failed_tasks=failed_tasks,
            start_time=start_time,
            duration_seconds=duration_seconds,
            throughput_tasks_per_sec=round(throughput, 2),
            speedup=round(speedup, 2),
            efficiency=round(efficiency, 2),
            average_latency_seconds=round(avg_latency, 4),
            communication_overhead_seconds=overhead,
        )
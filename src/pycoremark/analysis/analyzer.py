"""Unified performance analyzer orchestrating calculations into ExperimentResult."""

from datetime import datetime, timezone
from math import ceil
from typing import List

from pycoremark.analysis.metrics_calc import (
    CPUScoreCalculator,
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
        *,
        num_workers: int | None = None,
        start_time: datetime | None = None,
    ) -> ExperimentResult:
        """Compute aggregated metrics and package into ExperimentResult."""
        total_tasks = len(task_results)
        completed_results = [
            r for r in task_results if r.status == TaskStatus.COMPLETED
        ]
        successful_tasks = len(completed_results)
        failed_tasks = total_tasks - successful_tasks

        processed_numbers = 0
        if config.workload.name == "cpu_benchmark":
            processed_numbers = sum(
                int(result.output_data.get("processed_numbers", 0))
                for result in completed_results
                if isinstance(result.output_data, dict)
            )
        cpu_score = CPUScoreCalculator.calculate(processed_numbers, duration_seconds)

        throughput = self.throughput_calc.calculate(successful_tasks, duration_seconds)
        speedup = self.speedup_calc.calculate(
            baseline_single_node_duration,
            duration_seconds,
        )
        worker_count = num_workers if num_workers is not None else config.target_nodes
        efficiency = self.efficiency_calc.calculate(speedup, worker_count)
        overhead = self.overhead_calc.calculate_communication_overhead(
            duration_seconds,
            task_results,
            num_workers=worker_count,
        )

        # Latency hanya dari task yang benar-benar selesai.
        if completed_results:
            avg_latency = sum(
                r.execution_time_seconds for r in completed_results
            ) / len(completed_results)
        else:
            avg_latency = 0.0

        latencies = sorted(r.execution_time_seconds for r in completed_results)
        queue_waits = sorted(r.queue_wait_seconds for r in completed_results)
        tasks_per_worker: dict[str, int] = {}
        for result in completed_results:
            tasks_per_worker[result.worker_id] = tasks_per_worker.get(result.worker_id, 0) + 1
        load_imbalance = self.load_balance_calc.calculate_imbalance_ratio(tasks_per_worker)

        def percentile(fraction: float) -> float:
            if not latencies:
                return 0.0
            index = max(0, min(len(latencies) - 1, ceil(fraction * len(latencies)) - 1))
            return round(latencies[index], 6)

        # Prefer the experiment wall-clock start supplied by the controller.
        if start_time is None:
            if completed_results:
                first = min(completed_results, key=lambda result: result.completed_at)
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

        metrics_summary = {
            "calculation_inputs": {
                "scheduler_type": config.scheduler_type,
                "workload": config.workload.name,
                "target_nodes": config.target_nodes,
                "repeat_count": config.repeat_count,
                "baseline_single_node_duration_seconds": baseline_single_node_duration,
                "parallel_wall_duration_seconds": duration_seconds,
                "workers_that_completed_tasks": worker_count,
                "task_load_imbalance_ratio": round(load_imbalance, 4),
            },
            "task_latency_seconds": {
                "p50": percentile(0.50),
                "p95": percentile(0.95),
                "p99": percentile(0.99),
            },
            "queue_wait_seconds": {
                "average": round(sum(queue_waits) / len(queue_waits), 6)
                if queue_waits else 0.0,
                "p95": round(
                    queue_waits[max(0, ceil(0.95 * len(queue_waits)) - 1)], 6
                ) if queue_waits else 0.0,
            },
        }
        if config.workload.name == "cpu_benchmark":
            metrics_summary["benchmark_score"] = {
                "name": "PyCoreMark CPU Points",
                "points": round(cpu_score, 2),
                "processed_numbers": processed_numbers,
                "processed_numbers_per_second": round(
                    processed_numbers / duration_seconds, 2
                ) if duration_seconds > 0 else 0.0,
                "formula": "processed_numbers / duration_seconds / 1000",
                "unit_definition": "1 point = 1,000 input integers processed per second",
            }

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
            average_latency_seconds=round(avg_latency, 6),
            communication_overhead_seconds=overhead,
            metrics_summary=metrics_summary,
        )

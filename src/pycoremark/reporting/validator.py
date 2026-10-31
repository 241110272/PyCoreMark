
"""Result and output data validator."""

import math

from pycoremark.models.event import ValidationResult
from pycoremark.models.experiment import ExperimentResult, ExperimentStatus


class ResultValidator:
    """Validate experiment metrics and task-count consistency."""

    @staticmethod
    def validate_experiment_result(result: ExperimentResult) -> ValidationResult:
        """Validate task counts, timestamps, and benchmark metrics."""
        errors = []
        warnings = []

        # Validate task counts.
        task_counts = {
            "total_tasks": result.total_tasks,
            "successful_tasks": result.successful_tasks,
            "failed_tasks": result.failed_tasks,
        }

        for name, value in task_counts.items():
            if value < 0:
                errors.append(f"Invalid {name}: value cannot be negative.")

        if all(value >= 0 for value in task_counts.values()):
            accounted_tasks = result.successful_tasks + result.failed_tasks

            if accounted_tasks > result.total_tasks:
                errors.append(
                    "Task-count mismatch: successful_tasks + failed_tasks "
                    "cannot exceed total_tasks."
                )
            elif (
                result.status == ExperimentStatus.COMPLETED
                and accounted_tasks != result.total_tasks
            ):
                errors.append(
                    "Task-count mismatch: a completed experiment must account "
                    "for every task as successful or failed."
                )
            elif accounted_tasks < result.total_tasks:
                warnings.append(
                    f"{result.total_tasks - accounted_tasks} tasks are not "
                    "yet accounted for."
                )

        if result.failed_tasks > 0:
            warnings.append(
                f"{result.failed_tasks} tasks encountered execution failure."
            )

        consistency = result.metrics_summary.get("consistency")
        if consistency is not None and consistency.get("valid") is False:
            errors.append("Distributed result does not match the sequential baseline checksum.")

        # Validate numeric metrics.
        numeric_metrics = {
            "duration": result.duration_seconds,
            "throughput": result.throughput_tasks_per_sec,
            "speedup": result.speedup,
            "efficiency": result.efficiency,
            "average latency": result.average_latency_seconds,
            "communication overhead": result.communication_overhead_seconds,
        }

        for name, value in numeric_metrics.items():
            if not math.isfinite(value):
                errors.append(f"Invalid {name}: value must be finite.")
            elif value < 0:
                errors.append(f"Invalid {name}: value cannot be negative.")

        if (
            math.isfinite(result.duration_seconds)
            and result.duration_seconds <= 0
        ):
            errors.append("Invalid duration: duration must be > 0.")

        if math.isfinite(result.speedup) and result.speedup <= 0:
            errors.append("Invalid speedup calculation: speedup must be > 0.")

        # Validate timestamp ordering when an end time exists.
        if result.end_time is not None:
            try:
                if result.end_time < result.start_time:
                    errors.append(
                        "Invalid timestamps: end_time precedes start_time."
                    )
            except TypeError:
                errors.append(
                    "Invalid timestamps: start_time and end_time "
                    "must use compatible timezone information."
                )

        return ValidationResult(
            is_valid=(len(errors) == 0),
            errors=errors,
            warnings=warnings,
            metadata={
                "total_tasks": result.total_tasks,
                "completed_tasks": result.successful_tasks,
                "permanently_failed_tasks": result.failed_tasks,
                "unfinished_tasks": max(
                    0, result.total_tasks - result.successful_tasks - result.failed_tasks
                ),
            },
        )

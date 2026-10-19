
"""Experiment comparison and historical benchmark diffing service."""

from typing import Any, Dict, List

from pycoremark.models.experiment import ExperimentResult
from pycoremark.storage.base import ExperimentRepository


class ExperimentComparisonService:
    """Compares metrics between multiple benchmark runs."""

    def __init__(self, experiment_repository: ExperimentRepository) -> None:
        self.experiment_repository = experiment_repository

    def compare(self, experiment_ids: List[str]) -> Dict[str, Any]:
        """Compare benchmark metrics and report missing experiment IDs."""
        runs: List[ExperimentResult] = []
        missing_experiments: List[str] = []

        for exp_id in experiment_ids:
            result = self.experiment_repository.get_result(exp_id)

            if result is None:
                missing_experiments.append(exp_id)
                continue

            runs.append(result)

        metrics = [
            {
                "experiment_id": result.experiment_id,
                "throughput": result.throughput_tasks_per_sec,
                "speedup": result.speedup,
                "efficiency": result.efficiency,
                "avg_latency": result.average_latency_seconds,
                "duration": result.duration_seconds,
            }
            for result in runs
        ]

        return {
            "compared_experiments": list(experiment_ids),
            "metrics": metrics,
            "missing_experiments": missing_experiments,
            "requested_count": len(experiment_ids),
            "found_count": len(runs),
        }

"""Experiment comparison and historical benchmark diffing service.

TODO(Member 7): Add multi-run statistical hypothesis testing (p-value, t-test).
"""

from typing import Any, Dict, List
from pycoremark.models.experiment import ExperimentResult
from pycoremark.storage.base import ExperimentRepository


class ExperimentComparisonService:
    """Compares metrics between multiple benchmark runs."""

    def __init__(self, experiment_repository: ExperimentRepository) -> None:
        """Initialize with experiment repository."""
        self.experiment_repository = experiment_repository

    def compare(self, experiment_ids: List[str]) -> Dict[str, Any]:
        """Generate side-by-side metric comparison table for given experiment IDs."""
        runs: List[ExperimentResult] = []
        for exp_id in experiment_ids:
            res = self.experiment_repository.get_result(exp_id)
            if res:
                runs.append(res)

        return {
            "compared_experiments": experiment_ids,
            "metrics": [
                {
                    "experiment_id": r.experiment_id,
                    "throughput": r.throughput_tasks_per_sec,
                    "speedup": r.speedup,
                    "efficiency": r.efficiency,
                    "avg_latency": r.average_latency_seconds,
                    "duration": r.duration_seconds,
                }
                for r in runs
            ],
        }


"""Report exporters for benchmark results (CSV and JSON formats).

TODO(Member 7): Add Markdown table and HTML PDF export support.
"""

from abc import ABC, abstractmethod
import csv
import io
import json
from typing import List
from pycoremark.models.experiment import ExperimentResult


class BaseExporter(ABC):
    """Abstract file/stream exporter interface."""

    @abstractmethod
    def export(self, results: List[ExperimentResult]) -> str:
        """Export list of experiment results to string format."""
        raise NotImplementedError


class JSONExporter(BaseExporter):
    """Serializes experiment results to formatted JSON string."""

    def export(self, results: List[ExperimentResult]) -> str:
        """Serialize results to JSON."""
        data = [
            {
                "experiment_id": r.experiment_id,
                "status": r.status.value,
                "total_tasks": r.total_tasks,
                "successful_tasks": r.successful_tasks,
                "failed_tasks": r.failed_tasks,
                "duration_seconds": r.duration_seconds,
                "throughput_tasks_per_sec": r.throughput_tasks_per_sec,
                "speedup": r.speedup,
                "efficiency": r.efficiency,
                "average_latency_seconds": r.average_latency_seconds,
                "communication_overhead_seconds": r.communication_overhead_seconds,
            }
            for r in results
        ]
        return json.dumps(data, indent=2)


class CSVExporter(BaseExporter):
    """Serializes experiment results to comma-separated values (CSV) string."""

    def export(self, results: List[ExperimentResult]) -> str:
        """Serialize results to CSV."""
        output = io.StringIO()
        fieldnames = [
            "experiment_id",
            "status",
            "total_tasks",
            "successful_tasks",
            "failed_tasks",
            "duration_seconds",
            "throughput_tasks_per_sec",
            "speedup",
            "efficiency",
            "average_latency_seconds",
            "communication_overhead_seconds",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow({
                "experiment_id": r.experiment_id,
                "status": r.status.value,
                "total_tasks": r.total_tasks,
                "successful_tasks": r.successful_tasks,
                "failed_tasks": r.failed_tasks,
                "duration_seconds": r.duration_seconds,
                "throughput_tasks_per_sec": r.throughput_tasks_per_sec,
                "speedup": r.speedup,
                "efficiency": r.efficiency,
                "average_latency_seconds": r.average_latency_seconds,
                "communication_overhead_seconds": r.communication_overhead_seconds,
            })
        return output.getvalue()


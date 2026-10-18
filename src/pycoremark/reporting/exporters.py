
"""Report exporters for benchmark results."""

from abc import ABC, abstractmethod
import csv
import html
import io
import json
from typing import List

from pycoremark.models.experiment import ExperimentResult


class BaseExporter(ABC):
    """Abstract file/stream exporter interface."""

    @abstractmethod
    def export(self, results: List[ExperimentResult]) -> str:
        """Export experiment results to a string."""
        raise NotImplementedError


def _result_to_dict(result: ExperimentResult) -> dict:
    """Convert an experiment result into a serializable dictionary."""
    return {
        "experiment_id": result.experiment_id,
        "status": result.status.value,
        "total_tasks": result.total_tasks,
        "successful_tasks": result.successful_tasks,
        "failed_tasks": result.failed_tasks,
        "duration_seconds": result.duration_seconds,
        "throughput_tasks_per_sec": result.throughput_tasks_per_sec,
        "speedup": result.speedup,
        "efficiency": result.efficiency,
        "average_latency_seconds": result.average_latency_seconds,
        "communication_overhead_seconds": result.communication_overhead_seconds,
    }


class JSONExporter(BaseExporter):
    """Export experiment results as formatted JSON."""

    def export(self, results: List[ExperimentResult]) -> str:
        return json.dumps(
            [_result_to_dict(result) for result in results],
            indent=2,
        )


class CSVExporter(BaseExporter):
    """Export experiment results as CSV."""

    def export(self, results: List[ExperimentResult]) -> str:
        output = io.StringIO()
        fieldnames = list(_result_to_dict.__annotations__)  # Replaced below.
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

        for result in results:
            writer.writerow(_result_to_dict(result))

        return output.getvalue()


class MarkdownExporter(BaseExporter):
    """Export experiment results as a Markdown table."""

    def export(self, results: List[ExperimentResult]) -> str:
        columns = [
            ("experiment_id", "Experiment ID"),
            ("status", "Status"),
            ("total_tasks", "Total Tasks"),
            ("successful_tasks", "Successful Tasks"),
            ("failed_tasks", "Failed Tasks"),
            ("duration_seconds", "Duration (s)"),
            ("throughput_tasks_per_sec", "Throughput (tasks/s)"),
            ("speedup", "Speedup"),
            ("efficiency", "Efficiency"),
            ("average_latency_seconds", "Average Latency (s)"),
            ("communication_overhead_seconds", "Communication Overhead (s)"),
        ]

        headers = [label for _, label in columns]
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
        ]

        for result in results:
            data = _result_to_dict(result)
            values = [
                str(data[key]).replace("|", "\\|").replace("\n", " ")
                for key, _ in columns
            ]
            lines.append("| " + " | ".join(values) + " |")

        if not results:
            lines.append("| No experiment results |" + " |" * (len(columns) - 1))

        return "\n".join(lines)


class HTMLExporter(BaseExporter):
    """Export experiment results as a standalone HTML report."""

    def export(self, results: List[ExperimentResult]) -> str:
        columns = [
            ("experiment_id", "Experiment ID"),
            ("status", "Status"),
            ("total_tasks", "Total Tasks"),
            ("successful_tasks", "Successful Tasks"),
            ("failed_tasks", "Failed Tasks"),
            ("duration_seconds", "Duration (s)"),
            ("throughput_tasks_per_sec", "Throughput (tasks/s)"),
            ("speedup", "Speedup"),
            ("efficiency", "Efficiency"),
            ("average_latency_seconds", "Average Latency (s)"),
            ("communication_overhead_seconds", "Communication Overhead (s)"),
        ]

        header_html = "".join(
            f"<th>{html.escape(label)}</th>" for _, label in columns
        )

        rows = []
        for result in results:
            data = _result_to_dict(result)
            cells = "".join(
                f"<td>{html.escape(str(data[key]))}</td>"
                for key, _ in columns
            )
            rows.append(f"<tr>{cells}</tr>")

        body_html = "\n".join(rows)
        if not rows:
            body_html = (
                f'<tr><td colspan="{len(columns)}">'
                "No experiment results available."
                "</td></tr>"
            )

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PyCoreMark Benchmark Report</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
h1 {{ margin-bottom: 8px; }}
p {{ color: #555; }}
.table-container {{ overflow-x: auto; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
th {{ background: #f2f2f2; }}
tr:nth-child(even) {{ background: #fafafa; }}
@media print {{ body {{ margin: 8mm; }} }}
</style>
</head>
<body>
<h1>PyCoreMark Benchmark Report</h1>
<p>Experiment results: {len(results)}</p>
<div class="table-container">
<table>
<thead><tr>{header_html}</tr></thead>
<tbody>{body_html}</tbody>
</table>
</div>
</body>
</html>"""

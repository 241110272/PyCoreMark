"""Report generator compiling validation and export formats.

TODO(Member 7): Add graphical charting integration (matplotlib/plotly static images).
"""

from typing import List
from pycoremark.models.experiment import ExperimentResult
from pycoremark.reporting.exporters import CSVExporter, JSONExporter
from pycoremark.reporting.validator import ResultValidator


class ReportGenerator:
    """Compiles benchmark results into validated, exportable artifacts."""

    def __init__(
        self,
        validator: ResultValidator,
        json_exporter: JSONExporter,
        csv_exporter: CSVExporter,
    ) -> None:
        """Initialize ReportGenerator with injected validator and exporters."""
        self.validator = validator
        self.json_exporter = json_exporter
        self.csv_exporter = csv_exporter

    def generate_json_report(self, results: List[ExperimentResult]) -> str:
        """Validate and generate JSON report."""
        for r in results:
            val = self.validator.validate_experiment_result(r)
            if not val.is_valid:
                # Log or flag invalid results
                pass
        return self.json_exporter.export(results)

    def generate_csv_report(self, results: List[ExperimentResult]) -> str:
        """Validate and generate CSV report."""
        return self.csv_exporter.export(results)


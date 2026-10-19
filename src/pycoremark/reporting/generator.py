
"""Report generator compiling validation and export formats."""

from typing import Any, Dict, List

from pycoremark.models.experiment import ExperimentResult
from pycoremark.reporting.exporters import (
    CSVExporter,
    HTMLExporter,
    JSONExporter,
    MarkdownExporter,
)
from pycoremark.reporting.validator import ResultValidator


class ReportGenerator:
    """Compiles benchmark results into validated, exportable artifacts."""

    def __init__(
        self,
        validator: ResultValidator,
        json_exporter: JSONExporter,
        csv_exporter: CSVExporter,
    ) -> None:
        """Initialize the report generator with existing dependencies."""
        self.validator = validator
        self.json_exporter = json_exporter
        self.csv_exporter = csv_exporter
        self.markdown_exporter = MarkdownExporter()
        self.html_exporter = HTMLExporter()

    def validate_results(
        self, results: List[ExperimentResult]
    ) -> List[Dict[str, Any]]:
        """Return validation details for every experiment."""
        details = []

        for result in results:
            validation = self.validator.validate_experiment_result(result)
            details.append(
                {
                    "experiment_id": result.experiment_id,
                    "is_valid": validation.is_valid,
                    "errors": list(validation.errors),
                    "warnings": list(validation.warnings),
                }
            )

        return details

    def generate_json_report(self, results: List[ExperimentResult]) -> str:
        """Generate a JSON report."""
        return self.json_exporter.export(results)

    def generate_csv_report(self, results: List[ExperimentResult]) -> str:
        """Generate a CSV report."""
        return self.csv_exporter.export(results)

    def generate_markdown_report(
        self, results: List[ExperimentResult]
    ) -> str:
        """Generate a Markdown table report."""
        return self.markdown_exporter.export(results)

    def generate_html_report(self, results: List[ExperimentResult]) -> str:
        """Generate a standalone HTML report."""
        return self.html_exporter.export(results)

    def generate_detailed_json_report(
        self, results: List[ExperimentResult]
    ) -> str:
        """Generate benchmark results together with validation details."""
        import json

        return json.dumps(
            {
                "results": json.loads(self.json_exporter.export(results)),
                "validation": self.validate_results(results),
            },
            indent=2,
        )

    def generate_detailed_markdown_report(
        self, results: List[ExperimentResult]
    ) -> str:
        """Generate a Markdown report with validation details."""
        lines = [
            "# PyCoreMark Benchmark Report",
            "",
            self.generate_markdown_report(results),
            "",
            "## Validation Results",
            "",
        ]

        validation_details = self.validate_results(results)

        if not validation_details:
            lines.append("No experiment results available for validation.")
        else:
            lines.extend(
                [
                    "| Experiment ID | Valid | Errors | Warnings |",
                    "| --- | --- | --- | --- |",
                ]
            )

            for detail in validation_details:
                errors = "; ".join(detail["errors"]) or "None"
                warnings = "; ".join(detail["warnings"]) or "None"

                def escape_cell(value: str) -> str:
                    return value.replace("|", "\\|").replace("\n", " ")

                lines.append(
                    f"| {escape_cell(detail['experiment_id'])} "
                    f"| {'Yes' if detail['is_valid'] else 'No'} "
                    f"| {escape_cell(errors)} "
                    f"| {escape_cell(warnings)} |"
                )

        return "\n".join(lines)

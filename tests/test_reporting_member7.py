from pycoremark.reporting.generator import ReportGenerator
from pycoremark.reporting.validator import ResultValidator

import json
from datetime import datetime, timezone

from pycoremark.models.experiment import ExperimentResult, ExperimentStatus
from pycoremark.reporting.exporters import (
    CSVExporter,
    HTMLExporter,
    JSONExporter,
    MarkdownExporter,
)


def sample_result():
    return ExperimentResult(
        experiment_id="EXP-001",
        status=ExperimentStatus.COMPLETED,
        total_tasks=100,
        successful_tasks=100,
        failed_tasks=0,
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2026, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
        duration_seconds=10.0,
        throughput_tasks_per_sec=10.0,
        speedup=2.0,
        efficiency=0.5,
        average_latency_seconds=0.1,
        communication_overhead_seconds=0.2,
    )


def test_json_exporter():
    output = JSONExporter().export([sample_result()])
    data = json.loads(output)
    assert len(data) == 1
    assert data[0]["experiment_id"] == "EXP-001"
    assert data[0]["total_tasks"] == 100


def test_csv_exporter():
    output = CSVExporter().export([sample_result()])
    assert "experiment_id" in output
    assert "EXP-001" in output
    assert "throughput_tasks_per_sec" in output


def test_markdown_exporter():
    output = MarkdownExporter().export([sample_result()])
    assert "Experiment ID" in output
    assert "EXP-001" in output
    assert "100" in output


def test_html_exporter():
    output = HTMLExporter().export([sample_result()])
    assert "<html" in output
    assert "EXP-001" in output
    assert "PyCoreMark Benchmark Report" in output


def test_exporters_handle_empty_results():
    assert json.loads(JSONExporter().export([])) == []
    assert "experiment_id" in CSVExporter().export([])
    assert "No experiment results" in MarkdownExporter().export([])
    assert "No experiment results available" in HTMLExporter().export([])

def test_report_generator_exports_all_formats():
    generator = ReportGenerator(
        validator=ResultValidator(),
        json_exporter=JSONExporter(),
        csv_exporter=CSVExporter(),
    )
    results = [sample_result()]

    assert "EXP-001" in generator.generate_json_report(results)
    assert "EXP-001" in generator.generate_csv_report(results)
    assert "EXP-001" in generator.generate_markdown_report(results)
    assert "EXP-001" in generator.generate_html_report(results)


def test_report_generator_validates_results():
    generator = ReportGenerator(
        validator=ResultValidator(),
        json_exporter=JSONExporter(),
        csv_exporter=CSVExporter(),
    )

    details = generator.validate_results([sample_result()])

    assert len(details) == 1
    assert details[0]["experiment_id"] == "EXP-001"
    assert details[0]["is_valid"] is True
    assert details[0]["errors"] == []
    assert details[0]["warnings"] == []


def test_report_generator_handles_empty_results():
    generator = ReportGenerator(
        validator=ResultValidator(),
        json_exporter=JSONExporter(),
        csv_exporter=CSVExporter(),
    )

    assert generator.validate_results([]) == []
    assert generator.generate_json_report([]) == "[]"
    assert "No experiment results" in generator.generate_markdown_report([])
    assert "No experiment results available" in generator.generate_html_report([])


def test_detailed_json_report_includes_validation():
    import json

    generator = ReportGenerator(
        validator=ResultValidator(),
        json_exporter=JSONExporter(),
        csv_exporter=CSVExporter(),
    )

    report = json.loads(
        generator.generate_detailed_json_report([sample_result()])
    )

    assert report["results"][0]["experiment_id"] == "EXP-001"
    assert report["validation"][0]["is_valid"] is True
    assert report["validation"][0]["errors"] == []


def test_detailed_markdown_report_includes_validation():
    generator = ReportGenerator(
        validator=ResultValidator(),
        json_exporter=JSONExporter(),
        csv_exporter=CSVExporter(),
    )

    report = generator.generate_detailed_markdown_report([sample_result()])

    assert "## Validation Results" in report
    assert "EXP-001" in report
    assert "| Yes |" in report

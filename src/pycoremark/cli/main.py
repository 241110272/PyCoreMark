"""Unified command line interface for PyCoreMark."""

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

from pycoremark.cli.runtime import LocalClusterRuntime
from pycoremark.analysis.scaling import benchmark_cpu_scaling
from pycoremark.common.config import load_config_from_env
from pycoremark.communication.http_transport import HTTPClusterClient
from pycoremark.storage.benchmark import benchmark_storage
from pycoremark.storage.sqlite import SQLiteExperimentRepository


def _json_default(value):
    if hasattr(value, "value"):
        return value.value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(f"Cannot encode {type(value).__name__}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pycoremark",
        description="Monitor a virtual cluster and benchmark CPU, data, and storage workloads.",
    )
    commands = parser.add_subparsers(dest="command")
    dashboard = commands.add_parser("dashboard", help="Open the interactive terminal dashboard")
    dashboard.add_argument("--coordinator", help="Connect to a remote coordinator URL")
    commands.add_parser("coordinator", help="Start the HTTP coordinator service")
    commands.add_parser("worker", help="Start a worker connected to the configured coordinator")
    commands.add_parser("web", help="Launch the Streamlit/Plotly dashboard")

    run = commands.add_parser("run", help="Run a local cluster benchmark")
    run.add_argument("--workload", choices=("cpu", "data", "mixed"), default="cpu")
    run.add_argument("--tasks", type=int, default=8)
    run.add_argument("--complexity", type=int, default=25000)
    run.add_argument("--records", type=int, default=1000)
    run.add_argument("--dataset-path", type=Path)
    run.add_argument("--output-dir", type=Path)
    run.add_argument("--result-url")
    run.add_argument("--network-timeout", type=float, default=10.0)
    run.add_argument("--nodes", type=int, default=2)
    run.add_argument("--scheduler", choices=("static", "dynamic", "resource-aware"), default="dynamic")
    run.add_argument("--repeats", type=int, default=1, help="Independent trials per experiment")
    run.add_argument("--failure-rate", type=float, default=0.0,
                     help="Local worker task failure probability [0,1]")
    run.add_argument("--delay-ms", type=float, default=0.0,
                     help="Artificial per-task delay for local network-latency studies")
    run.add_argument("--coordinator", help="Run on a remote coordinator instead of local workers")
    run.add_argument("--json", action="store_true", dest="as_json")

    storage = commands.add_parser("storage", help="Benchmark storage using disposable test files")
    storage.add_argument("--path", type=Path, default=None, help="Parent directory for the temporary test file")
    storage.add_argument("--size-mib", type=int, default=64)
    storage.add_argument("--random-ops", type=int, default=500)
    storage.add_argument("--json", action="store_true", dest="as_json")

    scaling = commands.add_parser(
        "scaling", help="Compare single-core and multi-core CPU throughput over a fixed time window"
    )
    scaling.add_argument("--duration", type=float, default=2.0, help="Minimum seconds per worker count")
    scaling.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 6, 8])
    scaling.add_argument("--tasks", type=int, default=16, help="Prime partitions per round")
    scaling.add_argument("--complexity", type=int, default=10000, help="Numbers per task")
    scaling.add_argument("--json", action="store_true", dest="as_json")

    status = commands.add_parser("status", help="Show a short-lived local cluster snapshot")
    status.add_argument("--nodes", type=int, default=2)
    status.add_argument("--coordinator", help="Read status from a remote coordinator URL")

    report = commands.add_parser("report", help="Export saved experiment results")
    report.add_argument(
        "--format",
        choices=("json", "csv", "markdown", "html", "detailed-json", "detailed-markdown"),
        default="detailed-json",
    )
    report.add_argument("--output", type=Path, help="Output file; omit to print to stdout")
    report.add_argument("--limit", type=int, default=1000)
    report.add_argument("--database", type=Path, help="Override the configured SQLite database")
    return parser


def _launch_dashboard(coordinator_url: str | None = None) -> int:
    try:
        from pycoremark.cli.tui import PyCoreMarkApp
    except ImportError as exc:
        print(
            "The interactive dashboard needs Textual. Install PyCoreMark with "
            "`python -m pip install -e .` and retry.\n"
            f"Details: {exc}",
            file=sys.stderr,
        )
        return 2
    token = load_config_from_env().registration_auth_token
    PyCoreMarkApp(coordinator_url=coordinator_url, auth_token=token).run()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    command = args.command or "dashboard"
    if command == "dashboard":
        return _launch_dashboard(getattr(args, "coordinator", None))
    if command == "coordinator":
        from pycoremark.cli.coordinator import main as coordinator_main
        coordinator_main()
        return 0
    if command == "worker":
        from pycoremark.cli.worker import main as worker_main
        worker_main()
        return 0
    if command == "web":
        from pycoremark.cli.web import main as web_main
        web_main()
        return 0
    if command == "storage":
        result = benchmark_storage(
            args.path,
            size_mib=args.size_mib,
            random_operations=args.random_ops,
        )
        if args.as_json:
            print(json.dumps(result.to_dict(), indent=2))
        else:
            print(f"Storage benchmark in {result.directory}")
            print(f"Sequential write: {result.sequential_write_mib_s:.2f} MiB/s (fsync included)")
            print(f"Sequential read:  {result.sequential_read_mib_s:.2f} MiB/s (OS cache may apply)")
            print(f"Random read:      {result.random_read_iops:.1f} IOPS")
            print(f"Random write:     {result.random_write_iops:.1f} IOPS")
            print(f"Random latency:   p50 {result.random_latency_p50_ms:.3f} ms, p95 {result.random_latency_p95_ms:.3f} ms")
            print(f"Temporary test data removed: {result.data_removed}")
        return 0
    if command == "scaling":
        report = benchmark_cpu_scaling(
            duration_seconds=args.duration,
            worker_counts=args.workers,
            task_count=args.tasks,
            task_complexity=args.complexity,
        )
        if args.as_json:
            print(json.dumps(report.to_dict(), indent=2))
        else:
            print(
                f"Consistency: {'PASS' if report.consistency_valid else 'FAIL'}; "
                f"{report.test_numbers:,} integers per round; target {args.duration:.2f}s/sample"
            )
            print("workers  seconds  rounds  tasks/s    speedup  efficiency  checksum")
            for sample in report.samples:
                print(
                    f"{sample.workers:>7}  {sample.actual_duration_seconds:>7.2f}  "
                    f"{sample.rounds_completed:>6}  {sample.throughput_tasks_per_second:>8.2f}  "
                    f"{sample.speedup:>7.2f}x  {sample.efficiency:>9.1%}  "
                    f"{sample.expected_checksum[:12]}"
                )
        return 0 if report.consistency_valid else 1
    if command == "report":
        if args.limit < 1:
            _parser().error("--limit must be positive")
        from pycoremark.cli.runtime import _default_database_path
        from pycoremark.reporting.exporters import CSVExporter, JSONExporter
        from pycoremark.reporting.generator import ReportGenerator
        from pycoremark.reporting.validator import ResultValidator
        if args.database:
            database_path = args.database
        else:
            database_url = load_config_from_env().database_url
            if database_url == "sqlite:///pycoremark.db":
                database_path = _default_database_path()
            elif database_url.startswith("sqlite:///"):
                database_path = Path(database_url.removeprefix("sqlite:///"))
            else:
                _parser().error("Reporting currently supports SQLite databases only")
        repository = SQLiteExperimentRepository(database_path)
        results = repository.list_all()[:args.limit]
        generator = ReportGenerator(
            validator=ResultValidator(),
            json_exporter=JSONExporter(),
            csv_exporter=CSVExporter(),
        )
        output = {
            "json": generator.generate_json_report,
            "csv": generator.generate_csv_report,
            "markdown": generator.generate_markdown_report,
            "html": generator.generate_html_report,
            "detailed-json": generator.generate_detailed_json_report,
            "detailed-markdown": generator.generate_detailed_markdown_report,
        }[args.format](results)
        if args.output:
            args.output.expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
            args.output.expanduser().resolve().write_text(output, encoding="utf-8")
            print(f"Exported {len(results)} experiments to {args.output}")
        else:
            print(output)
        return 0
    if command == "status":
        if args.coordinator:
            token = load_config_from_env().registration_auth_token
            print(json.dumps(HTTPClusterClient(args.coordinator, auth_token=token).status(), indent=2))
        else:
            with LocalClusterRuntime() as runtime:
                runtime.start(args.nodes)
                print(json.dumps(runtime.status(), indent=2))
        return 0
    if command == "run":
        if args.nodes < 1:
            _parser().error("--nodes must be at least 1")
        if args.workload == "cpu" and (args.tasks < 1 or args.complexity < 0):
            _parser().error("--tasks must be positive and --complexity nonnegative")
        if args.workload == "data" and args.records < 0:
            _parser().error("--records must be nonnegative")
        if args.workload == "mixed" and not (
            args.dataset_path and args.output_dir and args.result_url
        ):
            _parser().error(
                "--workload mixed requires --dataset-path, --output-dir, and --result-url"
            )
        if args.workload == "mixed" and (
            args.tasks < 1 or args.complexity < 1 or args.network_timeout <= 0
        ):
            _parser().error("mixed workload requires positive --tasks, --complexity, and --network-timeout")
        if not 1 <= args.repeats <= 20:
            _parser().error("--repeats must be between 1 and 20")
        if not 0 <= args.failure_rate <= 1 or args.delay_ms < 0:
            _parser().error("--failure-rate must be [0,1] and --delay-ms nonnegative")
        if args.coordinator and (args.failure_rate or args.delay_ms):
            _parser().error(
                "Configure remote worker failure/delay with PYCOREMARK_WORKER_FAILURE_RATE "
                "and PYCOREMARK_SIMULATED_NETWORK_DELAY_MS on each worker."
            )
        if args.coordinator:
            token = load_config_from_env().registration_auth_token
            result = HTTPClusterClient(args.coordinator, auth_token=token).run_experiment(
                workload=args.workload,
                nodes=args.nodes,
                tasks=args.tasks,
                complexity=args.complexity,
                records=args.records,
                scheduler_type=args.scheduler.upper().replace("-", "_"),
                repeat_count=args.repeats,
                dataset_path=str(args.dataset_path) if args.dataset_path else None,
                output_dir=str(args.output_dir) if args.output_dir else None,
                result_url=args.result_url,
                network_timeout=args.network_timeout,
            )
        else:
            with LocalClusterRuntime() as runtime:
                runtime.start(
                    args.nodes,
                    failure_rate=args.failure_rate,
                    simulated_delay_ms=args.delay_ms,
                )
                if args.workload == "cpu":
                    result = runtime.run_cpu(
                        task_count=args.tasks,
                        complexity=args.complexity,
                        node_count=args.nodes,
                        scheduler_type=args.scheduler.upper().replace("-", "_"),
                        repeat_count=args.repeats,
                    )
                elif args.workload == "data":
                    result = runtime.run_data(
                        record_count=args.records,
                        node_count=args.nodes,
                        scheduler_type=args.scheduler.upper().replace("-", "_"),
                        repeat_count=args.repeats,
                    )
                else:
                    result = runtime.run_mixed(
                        dataset_path=str(args.dataset_path),
                        output_dir=str(args.output_dir),
                        result_url=args.result_url,
                        hash_rounds=args.complexity,
                        partitions=args.tasks,
                        network_timeout=args.network_timeout,
                        node_count=args.nodes,
                        scheduler_type=args.scheduler.upper().replace("-", "_"),
                        repeat_count=args.repeats,
                    )
        if args.as_json:
            print(json.dumps(asdict(result), default=_json_default, indent=2))
        else:
            print(
                f"{result.status.value}: {result.successful_tasks}/{result.total_tasks} tasks "
                f"in {result.duration_seconds:.3f}s; "
                f"{result.throughput_tasks_per_sec:.2f} tasks/s; "
                f"speedup {result.speedup:.2f}x; efficiency {result.efficiency:.1%}"
            )
            if result.failed_tasks:
                print(f"{result.failed_tasks} tasks failed; inspect persisted run details.")
            if result.metrics_summary:
                print(json.dumps(result.metrics_summary, default=_json_default, indent=2))
        return 0 if result.status.value == "COMPLETED" else 1
    return 0


def dashboard_main() -> int:
    return main(["dashboard"])


if __name__ == "__main__":
    raise SystemExit(main())

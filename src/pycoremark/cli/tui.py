"""Interactive mouse- and keyboard-driven terminal dashboard."""

from __future__ import annotations

import asyncio
from pathlib import Path
import tempfile
from uuid import uuid4

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, DataTable, Footer, Header, ProgressBar, RichLog, Static

from pycoremark.cli.runtime import LocalClusterRuntime
from pycoremark.communication.http_transport import HTTPClusterClient
from pycoremark.storage.benchmark import benchmark_storage


class PyCoreMarkApp(App):
    """A compact htop-style control room for local cluster benchmarks."""

    TITLE = "PyCoreMark"
    SUB_TITLE = "Distributed benchmark control room"
    CSS = """
    Screen { background: #090f1d; }
    Header { background: #151d30; }
    #summary { height: 3; margin: 0 1; padding: 0 1; background: #142238; color: #dce8f7; border: round #38517a; }
    #main { height: 11; layout: horizontal; }
    #nodes-panel, #topology-panel, #progress-panel, #history-panel { margin: 0 1 1 1; border: round #29415f; padding: 0 1; background: #0e1728; }
    #nodes-panel { width: 2fr; }
    #topology-panel { width: 1fr; }
    #lower { height: 9; layout: horizontal; }
    #progress-panel, #history-panel { width: 1fr; }
    #node-table, #history-table { height: 1fr; }
    #progress-label { height: 2; color: #dce8f7; }
    #progress-chart { height: 2; color: #4be2d0; text-style: bold; }
    #events { height: 1fr; min-height: 6; border: round #29415f; margin: 0 1 1 1; background: #0d1524; }
    #actions { height: 3; margin: 0 1; align: center middle; }
    Button { margin: 0 1; border: none; }
    Button:hover { text-style: bold; }
    #run-cpu { background: #117d72; }
    #run-data { background: #315f9e; }
    #run-storage { background: #7452a2; }
    .panel-title { color: #83a8ff; text-style: bold; }
    """

    BINDINGS = [("q", "quit", "Quit"), ("r", "refresh", "Refresh")]

    def __init__(self, coordinator_url: str | None = None, auth_token: str | None = None) -> None:
        super().__init__()
        self.remote = HTTPClusterClient(coordinator_url, auth_token=auth_token) if coordinator_url else None
        self.runtime = LocalClusterRuntime() if self.remote is None else None
        self._busy = False
        self._seen_event_ids: set[str] = set()
        self._active_label = "Ready for benchmark"

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Starting local virtual cluster…", id="summary")
        with Horizontal(id="main"):
            with Vertical(id="nodes-panel"):
                yield Static("◈  CLUSTER NODES", classes="panel-title")
                table = DataTable(id="node-table", zebra_stripes=True)
                table.add_columns("NODE", "STATE", "CPU LOAD", "CPU TREND", "MEMORY", "TASKS")
                yield table
            with Vertical(id="topology-panel"):
                yield Static("◉  CLUSTER TOPOLOGY", classes="panel-title")
                yield Static("", id="topology")
        with Horizontal(id="lower"):
            with Vertical(id="progress-panel"):
                yield Static("◌  LIVE BENCHMARK", classes="panel-title")
                yield Static("Ready — run a benchmark to see live progress.", id="progress-label")
                yield ProgressBar(total=100, show_eta=False, id="bench-progress")
                yield Static("", id="progress-chart")
            with Vertical(id="history-panel"):
                yield Static("▤  RECENT RESULTS", classes="panel-title")
                history = DataTable(id="history-table", zebra_stripes=True)
                history.add_columns("STATUS", "WORKLOAD", "TASKS/S", "POINTS", "TIME")
                yield history
        with Horizontal(id="actions"):
            yield Button("− Worker", id="scale-down")
            yield Button("+ Worker", id="scale-up")
            yield Button("Run CPU", id="run-cpu", variant="success")
            yield Button("Run word-count", id="run-data", variant="primary")
            yield Button("Storage test", id="run-storage", variant="warning")
            yield Button("Refresh", id="refresh")
            yield Button("Quit", id="quit", variant="error")
        yield RichLog(id="events", wrap=False, highlight=True, markup=True)
        yield Footer()

    def on_mount(self) -> None:
        if self.runtime is not None:
            self.runtime.start(2)
            self.write_event("Local workers online. Click an action or use keyboard navigation.")
        else:
            self.query_one("#scale-down", Button).disabled = True
            self.query_one("#scale-up", Button).disabled = True
            self.write_event(f"Connected to coordinator {self.remote.coordinator_url}.")
        self.set_interval(1.0, self.refresh_view)
        self.refresh_view()

    def on_resize(self, event) -> None:
        """Switch to a stacked layout on narrow terminal windows."""
        main = self.query_one("#main", Horizontal)
        lower = self.query_one("#lower", Horizontal)
        if event.size.width < 90:
            main.styles.layout = "vertical"
            lower.styles.layout = "vertical"
            self.query_one("#nodes-panel", Vertical).styles.height = "2fr"
            self.query_one("#topology-panel", Vertical).styles.height = "1fr"
            self.query_one("#nodes-panel", Vertical).styles.width = "1fr"
            self.query_one("#topology-panel", Vertical).styles.width = "1fr"
            self.query_one("#progress-panel", Vertical).styles.height = "1fr"
            self.query_one("#history-panel", Vertical).styles.height = "1fr"
            self.query_one("#progress-panel", Vertical).styles.width = "1fr"
            self.query_one("#history-panel", Vertical).styles.width = "1fr"
        else:
            main.styles.layout = "horizontal"
            lower.styles.layout = "horizontal"
            self.query_one("#nodes-panel", Vertical).styles.width = "2fr"
            self.query_one("#topology-panel", Vertical).styles.width = "1fr"
            self.query_one("#nodes-panel", Vertical).styles.height = "1fr"
            self.query_one("#topology-panel", Vertical).styles.height = "1fr"
            self.query_one("#progress-panel", Vertical).styles.width = "1fr"
            self.query_one("#history-panel", Vertical).styles.width = "1fr"
            self.query_one("#progress-panel", Vertical).styles.height = "1fr"
            self.query_one("#history-panel", Vertical).styles.height = "1fr"

    def on_unmount(self) -> None:
        if self.runtime is not None:
            self.runtime.stop()

    def action_refresh(self) -> None:
        self.refresh_view()

    def refresh_view(self) -> None:
        try:
            status = self.remote.status() if self.remote else self.runtime.status()
            if self.remote:
                latest = self.remote.metrics()
                samples = self.remote.metrics_history()
                history_results = self.remote.history()
                events = self.remote.events()
                valid_cpu = [sample.cpu_percent for sample in latest.values() if sample.cpu_percent is not None]
                avg_cpu = sum(valid_cpu) / len(valid_cpu) if valid_cpu else 0.0
            else:
                latest = self.runtime.metrics.get_latest_node_samples(max_age_seconds=10)
                samples = self.runtime.metrics.get_all_samples()
                history_results = self.runtime.history()
                events = self.runtime.events()
                avg_cpu = self.runtime.metrics.get_cluster_average_cpu(max_age_seconds=10)
        except Exception as exc:
            self.query_one("#summary", Static).update(f"COORDINATOR UNAVAILABLE  •  {exc}")
            return
        table = self.query_one("#node-table", DataTable)
        table.clear()
        cpu_history: dict[str, list[float]] = {}
        for sample in samples:
            if sample.cpu_percent is not None:
                cpu_history.setdefault(sample.node_id, []).append(sample.cpu_percent)
        for node in status["nodes"]:
            sample = latest.get(node["node_id"])
            if sample and sample.cpu_percent is not None:
                percentage = max(0, min(100, round(sample.cpu_percent)))
                cpu = f"{'█' * (percentage // 10)}{'·' * (10 - percentage // 10)} {percentage:>3}%"
            else:
                cpu = "N/A"
            trend = self._sparkline(cpu_history.get(node["node_id"], [])[-12:])
            memory = (
                f"{sample.memory_used_mb / 1024:.1f}/{sample.memory_total_mb / 1024:.1f} GiB"
                if sample and sample.memory_used_mb is not None and sample.memory_total_mb
                else "N/A"
            )
            tasks = str(sample.active_tasks) if sample else "—"
            table.add_row(node["node_id"], node["status"], cpu, trend, memory, tasks)
        running = "● RUNNING" if self._busy else "● READY"
        self.query_one("#summary", Static).update(
            f"{running}  •  {status['total_nodes']} workers  •  cluster CPU {avg_cpu:.0f}%  "
            f"•  {self._active_label}  •  press R to refresh"
        )
        graph = status.get("network_graph", {})
        edges = graph.get("edges", [])
        topology_lines = [f"{len(graph.get('nodes', []))} nodes  •  {len(edges)} known links"]
        if edges:
            topology_lines.extend(
                f"{edge['source']}  ↔  {edge['target']}" for edge in edges[:5]
            )
        else:
            topology_lines.append("coordinator (task/heartbeat API)")
            node_ids = [node["node_id"] for node in status.get("nodes", [])]
            topology_lines.extend(
                f"  {'└' if index == len(node_ids) - 1 else '├'}─ {node_id}"
                for index, node_id in enumerate(node_ids[:8])
            )
        self.query_one("#topology", Static).update("\n".join(topology_lines))
        for event in events[-40:]:
            event_id = event.get("event_id")
            if event_id and event_id not in self._seen_event_ids:
                self._seen_event_ids.add(event_id)
                self.write_event(self._format_event(event))
        history = self.query_one("#history-table", DataTable)
        history.clear()
        for result in history_results[:8]:
            score = result.metrics_summary.get("benchmark_score", {}).get("points")
            workload = result.metrics_summary.get("experiment_config", {}).get(
                "workload", {}
            ).get("name", "benchmark")
            workload = {
                "cpu_benchmark": "CPU",
                "partitioned_data": "DATA",
                "mixed": "MIXED",
            }.get(workload, str(workload).upper())
            history.add_row(
                result.status.value,
                workload,
                f"{result.throughput_tasks_per_sec:.1f}",
                f"{score:.2f}" if score is not None else "—",
                f"{result.duration_seconds:.2f}s",
                key=result.experiment_id,
            )

    @staticmethod
    def _format_event(event: dict) -> str:
        """Keep the event feed readable by showing concise, useful fields."""
        kind = event.get("event_type", "EVENT")
        source = event.get("source_node_id", "?")
        logical_clock = event.get("logical_clock", 0)
        payload = event.get("payload", {})
        if not isinstance(payload, dict):
            payload = {}
        if kind.startswith("TASK_"):
            task_id = str(payload.get("task_id", "task"))
            detail = f"{task_id[-12:]}  attempt {payload.get('attempt', 1)}"
        elif kind.startswith("EXPERIMENT_"):
            detail = (
                f"{str(payload.get('experiment_id', 'run'))[-16:]}  "
                f"{payload.get('status', '')}"
            ).strip()
        else:
            detail = str(payload.get("node_id", payload.get("worker_id", "")))
        return (
            f"[dim]L{logical_clock:<4} {source:<16}[/] "
            f"[bold cyan]{kind:<20}[/] [white]{detail}[/]"
        )

    @staticmethod
    def _sparkline(values: list[float]) -> str:
        glyphs = "▁▂▃▄▅▆▇█"
        if not values:
            return "—"
        return "".join(glyphs[min(7, max(0, int(value / 100 * 8)))] for value in values)

    def write_event(self, text: str) -> None:
        self.query_one("#events", RichLog).write(text)

    def _render_progress(self, progress: dict) -> None:
        completed = int(progress.get("completed_tasks", 0))
        total = int(progress.get("total_tasks", 0))
        fraction = min(1.0, max(0.0, float(progress.get("progress_fraction", 0.0))))
        phase = progress.get("phase", "Running benchmark")
        trial = progress.get("trial", 1)
        trials = progress.get("trials", 1)
        elapsed = float(progress.get("elapsed_seconds", 0.0))
        trial_label = f"  •  trial {trial}/{trials}" if trials > 1 else ""
        self.query_one("#progress-label", Static).update(
            f"[bold]{phase}[/]  •  {completed:,}/{total:,} tasks  •  "
            f"{fraction:.0%}{trial_label}  •  {elapsed:.1f}s"
        )
        self.query_one("#bench-progress", ProgressBar).update(
            total=100, progress=fraction * 100
        )
        points = progress.get("points", [])
        values = [int(point.get("completed_tasks", 0)) for point in points]
        self.query_one("#progress-chart", Static).update(
            f"[dim]TASKS COMPLETED[/]  {self._sparkline(values[-48:])}"
            if values else "[dim]Waiting for task results…[/]"
        )
        self._active_label = f"{phase} · {fraction:.0%}"

    @staticmethod
    def _remote_operation(client, workload: str, experiment_id: str):
        def operation():
            status = client.status()
            return client.run_experiment(
                workload=workload,
                nodes=max(1, len(status.get("nodes", []))),
                experiment_id=experiment_id,
            )
        return operation

    async def _run_action(self, label: str, operation, progress_getter=None) -> None:
        if self._busy:
            return
        self._busy = True
        self._active_label = f"Starting {label}…"
        self.refresh_view()
        self.query_one("#bench-progress", ProgressBar).update(total=100, progress=0)
        self.query_one("#progress-chart", Static).update("[dim]Collecting task timings…[/]")
        self.write_event(f"[bold cyan]Starting {label}…[/]")
        try:
            operation_task = asyncio.create_task(asyncio.to_thread(operation))
            spinner = ("◌", "◔", "◑", "◕")
            tick = 0
            latest_progress = None
            while not operation_task.done():
                if progress_getter is not None:
                    try:
                        latest_progress = await asyncio.to_thread(progress_getter) or latest_progress
                    except Exception:
                        pass
                if latest_progress:
                    self._render_progress(latest_progress)
                else:
                    self.query_one("#progress-label", Static).update(
                        f"[bold cyan]{spinner[tick % len(spinner)]}  "
                        f"{label} — preparing benchmark and workers…[/]"
                    )
                    self._active_label = f"{spinner[tick % len(spinner)]} {label}"
                tick += 1
                await asyncio.sleep(0.2)
            result = await operation_task
            if self.runtime is not None:
                latest_progress = self.runtime.controller.get_progress(result.experiment_id) or latest_progress
            elif progress_getter is not None:
                try:
                    latest_progress = progress_getter() or latest_progress
                except Exception:
                    pass
            if latest_progress:
                self._render_progress(latest_progress)
            score = result.metrics_summary.get("benchmark_score", {}).get("points")
            score_text = f"{score:.2f} PyCoreMark points" if score is not None else "N/A score"
            successful = result.status.value == "COMPLETED"
            outcome_color = "green" if successful else "red"
            outcome_icon = "✓" if successful else "✗"
            self.query_one("#progress-label", Static).update(
                f"[bold {outcome_color}]{outcome_icon} {label} "
                f"{'complete' if successful else 'failed'}[/]  •  "
                f"{result.successful_tasks:,}/{result.total_tasks:,} tasks  •  "
                f"{score:.2f} PyCoreMark points" if score is not None else
                f"[bold {outcome_color}]{outcome_icon} {label} "
                f"{'complete' if successful else 'failed'}[/]  •  "
                f"{result.successful_tasks:,}/{result.total_tasks:,} tasks"
            )
            self._active_label = "Last benchmark complete" if successful else "Last benchmark failed"
            self.write_event(
                f"[{outcome_color}]{result.status.value}[/] {result.experiment_id}: "
                f"{result.successful_tasks}/{result.total_tasks} tasks, "
                f"{result.throughput_tasks_per_sec:.2f} tasks/s, "
                f"{score_text}, "
                f"{result.duration_seconds:.3f}s"
            )
        except Exception as exc:
            self.query_one("#progress-label", Static).update(
                f"[bold red]✗ {label} failed[/]  •  {exc}"
            )
            self._active_label = "Last benchmark failed"
            self.write_event(f"[red]Benchmark failed: {exc}[/]")
        finally:
            self._busy = False
            self.refresh_view()

    @on(Button.Pressed, "#run-cpu")
    async def run_cpu(self) -> None:
        if self.runtime:
            operation = self.runtime.run_cpu
            progress_getter = self.runtime.controller.get_active_progress
        else:
            experiment_id = f"tui-cpu-{uuid4().hex}"
            operation = self._remote_operation(self.remote, "cpu", experiment_id)
            progress_getter = lambda: self.remote.experiment_progress(experiment_id)
        await self._run_action("CPU benchmark", operation, progress_getter)

    @on(Button.Pressed, "#run-data")
    async def run_data(self) -> None:
        if self.runtime:
            operation = self.runtime.run_data
            progress_getter = self.runtime.controller.get_active_progress
        else:
            experiment_id = f"tui-data-{uuid4().hex}"
            operation = self._remote_operation(self.remote, "data", experiment_id)
            progress_getter = lambda: self.remote.experiment_progress(experiment_id)
        await self._run_action("word-count benchmark", operation, progress_getter)

    @on(Button.Pressed, "#run-storage")
    async def run_storage(self) -> None:
        if self._busy:
            return
        self._busy = True
        self._active_label = "Running storage benchmark"
        self.refresh_view()
        self.query_one("#bench-progress", ProgressBar).update(total=100, progress=0)
        self.query_one("#progress-chart", Static).update("[dim]Local storage I/O test[/]")
        self.query_one("#progress-label", Static).update(
            "[bold cyan]◌  Running safe storage I/O benchmark…[/]"
        )
        self.write_event("[bold cyan]Running safe 64 MiB storage test in a temporary folder…[/]")
        try:
            test_task = asyncio.create_task(asyncio.to_thread(
                benchmark_storage, Path(tempfile.gettempdir()), size_mib=64
            ))
            spinner = ("◌", "◔", "◑", "◕")
            tick = 0
            while not test_task.done():
                self.query_one("#progress-label", Static).update(
                    f"[bold cyan]{spinner[tick % len(spinner)]}  "
                    "Storage benchmark — sequential and random I/O…[/]"
                )
                tick += 1
                await asyncio.sleep(0.2)
            result = await test_task
            self.query_one("#progress-label", Static).update(
                "[bold green]✓ Storage test complete[/]  •  temporary test data removed"
            )
            self._active_label = "Storage test complete"
            self.write_event(
                f"[green]Storage[/] write {result.sequential_write_mib_s:.1f} MiB/s, "
                f"read {result.sequential_read_mib_s:.1f} MiB/s, "
                f"random p95 {result.random_latency_p95_ms:.3f} ms (test data removed)"
            )
        except Exception as exc:
            self.write_event(f"[red]Storage test failed: {exc}[/]")
        finally:
            self._busy = False
            self.refresh_view()

    @on(Button.Pressed, "#refresh")
    def refresh_button(self) -> None:
        self.refresh_view()

    @on(Button.Pressed, "#scale-up")
    def scale_up(self) -> None:
        if self.runtime is None:
            return
        count = len(self.runtime.workers)
        if count >= 8:
            self.write_event("[yellow]Local UI scaling is limited to 8 workers.[/]")
            return
        self.runtime.scale_to(count + 1)
        self.write_event(f"[cyan]Scaled local cluster to {count + 1} workers.[/]")
        self.refresh_view()

    @on(Button.Pressed, "#scale-down")
    def scale_down(self) -> None:
        if self.runtime is None:
            return
        count = len(self.runtime.workers)
        if count <= 1:
            self.write_event("[yellow]At least one local worker must remain active.[/]")
            return
        self.runtime.scale_to(count - 1)
        self.write_event(f"[cyan]Scaled local cluster to {count - 1} workers.[/]")
        self.refresh_view()

    @on(Button.Pressed, "#quit")
    def quit_button(self) -> None:
        self.exit()

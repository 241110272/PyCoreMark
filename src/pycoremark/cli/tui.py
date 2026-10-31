"""Interactive mouse- and keyboard-driven terminal dashboard."""

from __future__ import annotations

import asyncio
from pathlib import Path
import tempfile

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, DataTable, Footer, Header, RichLog, Static

from pycoremark.cli.runtime import LocalClusterRuntime
from pycoremark.communication.http_transport import HTTPClusterClient
from pycoremark.storage.benchmark import benchmark_storage


class PyCoreMarkApp(App):
    """A compact htop-style control room for local cluster benchmarks."""

    TITLE = "PyCoreMark"
    SUB_TITLE = "Cluster monitor and benchmark console"
    CSS = """
    Screen { background: #0b1220; }
    #summary { height: 3; margin: 0 1; padding: 0 1; background: #142238; color: #dce8f7; }
    #main { height: 1fr; layout: horizontal; }
    #nodes-panel { width: 2fr; margin: 0 1 1 1; border: round #29415f; padding: 0 1; }
    #side-panel { width: 1fr; margin: 0 1 1 1; border: round #29415f; padding: 0 1; }
    #node-table { height: 1fr; }
    #history { height: 1fr; }
    #events { height: 1fr; border: round #29415f; margin: 0 1 1 1; }
    #actions { height: 3; margin: 0 1; align: center middle; }
    Button { margin: 0 1; }
    #run-cpu { background: #167d67; }
    #run-data { background: #315f9e; }
    #run-storage { background: #79539b; }
    """

    BINDINGS = [("q", "quit", "Quit"), ("r", "refresh", "Refresh")]

    def __init__(self, coordinator_url: str | None = None, auth_token: str | None = None) -> None:
        super().__init__()
        self.remote = HTTPClusterClient(coordinator_url, auth_token=auth_token) if coordinator_url else None
        self.runtime = LocalClusterRuntime() if self.remote is None else None
        self._busy = False
        self._seen_event_ids: set[str] = set()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Starting local virtual cluster…", id="summary")
        with Horizontal(id="main"):
            with Vertical(id="nodes-panel"):
                yield Static("CLUSTER NODES", classes="panel-title")
                table = DataTable(id="node-table", zebra_stripes=True)
                table.add_columns("NODE", "STATE", "CPU LOAD", "CPU TREND", "MEMORY", "TASKS")
                yield table
            with Vertical(id="side-panel"):
                yield Static("TOPOLOGY", classes="panel-title")
                yield Static("", id="topology")
                yield Static("RECENT BENCHMARKS", classes="panel-title")
                yield RichLog(id="history", wrap=True, highlight=True)
        with Horizontal(id="actions"):
            yield Button("− Worker", id="scale-down")
            yield Button("+ Worker", id="scale-up")
            yield Button("Run CPU", id="run-cpu", variant="success")
            yield Button("Run word-count", id="run-data", variant="primary")
            yield Button("Storage test", id="run-storage", variant="warning")
            yield Button("Refresh", id="refresh")
            yield Button("Quit", id="quit", variant="error")
        yield RichLog(id="events", wrap=True, highlight=True, markup=True)
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
        if event.size.width < 90:
            main.styles.layout = "vertical"
            self.query_one("#nodes-panel", Vertical).styles.height = "2fr"
            self.query_one("#side-panel", Vertical).styles.height = "1fr"
            self.query_one("#nodes-panel", Vertical).styles.width = "1fr"
            self.query_one("#side-panel", Vertical).styles.width = "1fr"
        else:
            main.styles.layout = "horizontal"
            self.query_one("#nodes-panel", Vertical).styles.width = "2fr"
            self.query_one("#side-panel", Vertical).styles.width = "1fr"
            self.query_one("#nodes-panel", Vertical).styles.height = "1fr"
            self.query_one("#side-panel", Vertical).styles.height = "1fr"

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
        running = "RUNNING" if self._busy else "READY"
        self.query_one("#summary", Static).update(
            f"{running}  •  {status['total_nodes']} nodes  •  cluster CPU {avg_cpu:.0f}%  "
            "•  click buttons or press R to refresh"
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
                self.write_event(
                    f"[dim]L{event.get('logical_clock', 0)} "
                    f"{event.get('source_node_id', '?')}[/] "
                    f"{event.get('event_type', 'EVENT')} "
                    f"{event.get('payload', {})}"
                )
        history = self.query_one("#history", RichLog)
        history.clear()
        for result in history_results[:8]:
            history.write(
                f"{result.status.value:<9} {result.experiment_id}  "
                f"{result.throughput_tasks_per_sec:.2f} tasks/s  "
                f"{result.duration_seconds:.2f}s"
            )

    @staticmethod
    def _sparkline(values: list[float]) -> str:
        glyphs = "▁▂▃▄▅▆▇█"
        if not values:
            return "—"
        return "".join(glyphs[min(7, max(0, int(value / 100 * 8)))] for value in values)

    def write_event(self, text: str) -> None:
        self.query_one("#events", RichLog).write(text)

    async def _run_action(self, label: str, operation) -> None:
        if self._busy:
            return
        self._busy = True
        self.refresh_view()
        self.write_event(f"[bold cyan]Starting {label}…[/]")
        try:
            result = await asyncio.to_thread(operation)
            self.write_event(
                f"[green]{result.status.value}[/] {result.experiment_id}: "
                f"{result.successful_tasks}/{result.total_tasks} tasks, "
                f"{result.throughput_tasks_per_sec:.2f} tasks/s, "
                f"{result.duration_seconds:.3f}s"
            )
        except Exception as exc:
            self.write_event(f"[red]Benchmark failed: {exc}[/]")
        finally:
            self._busy = False
            self.refresh_view()

    @on(Button.Pressed, "#run-cpu")
    async def run_cpu(self) -> None:
        operation = self.runtime.run_cpu if self.runtime else lambda: self.remote.run_experiment(
            workload="cpu", nodes=max(1, len(self.remote.status().get("nodes", [])) or 2)
        )
        await self._run_action("CPU benchmark", operation)

    @on(Button.Pressed, "#run-data")
    async def run_data(self) -> None:
        operation = self.runtime.run_data if self.runtime else lambda: self.remote.run_experiment(
            workload="data", nodes=max(1, len(self.remote.status().get("nodes", [])) or 2)
        )
        await self._run_action("word-count benchmark", operation)

    @on(Button.Pressed, "#run-storage")
    async def run_storage(self) -> None:
        if self._busy:
            return
        self._busy = True
        self.refresh_view()
        self.write_event("[bold cyan]Running safe 64 MiB storage test in a temporary folder…[/]")
        try:
            result = await asyncio.to_thread(
                benchmark_storage, Path(tempfile.gettempdir()), size_mib=64
            )
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

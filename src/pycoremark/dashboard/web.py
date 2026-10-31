"""Streamlit/Plotly dashboard for live cluster and benchmark analysis."""

from __future__ import annotations

import math
import os
import threading
import time
from datetime import datetime
from uuid import uuid4

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from pycoremark.common.config import load_config_from_env
from pycoremark.communication.http_transport import HTTPClusterClient


st.set_page_config(page_title="PyCoreMark", page_icon="⚙️", layout="wide")
st.markdown("""
<style>
    [data-testid="stMetric"] { background: linear-gradient(135deg, #151b2c, #202744);
        border: 1px solid #303b5c; padding: 14px 18px; border-radius: 14px; }
    div.stButton > button[kind="primary"] { border: 0; border-radius: 10px;
        background: linear-gradient(90deg, #6d5dfc, #20c6d7); font-weight: 700; }
    .stProgress > div > div > div > div { background: linear-gradient(90deg, #735bff, #28d7c4); }
</style>
""", unsafe_allow_html=True)
st.title("PyCoreMark — Distributed Cluster Benchmark")
st.caption("Live membership · worker telemetry · consistent benchmark results · scalability analysis")

with st.sidebar:
    st.header("Coordinator")
    coordinator_url = st.text_input(
        "API URL", value=os.getenv("PYCOREMARK_COORDINATOR_URL", "http://127.0.0.1:8000")
    ).rstrip("/")
    default_token = load_config_from_env().registration_auth_token or ""
    token = st.text_input("API token", value=default_token, type="password")
    st.caption("Use Refresh to query the coordinator again.")
    refresh = st.button("Refresh", use_container_width=True)

client = HTTPClusterClient(coordinator_url, auth_token=token or None)
try:
    status = client.status()
    metrics = client.metrics()
    samples = client.metrics_history()
    events = client.events()
    history = client.history()
except Exception as exc:
    st.error(f"Coordinator unavailable: {exc}")
    st.info("Start `python -m pycoremark coordinator` and connect workers before opening this page.")
    st.stop()

nodes = status.get("nodes", [])
active_nodes = [node for node in nodes if node["status"] in ("READY", "BUSY")]
cpu_values = [sample.cpu_percent for sample in metrics.values() if sample.cpu_percent is not None]
average_cpu = sum(cpu_values) / len(cpu_values) if cpu_values else 0.0
kpis = st.columns(4)
kpis[0].metric("Workers", len(nodes), f"{len(active_nodes)} active")
kpis[1].metric("Average CPU", f"{average_cpu:.1f}%")
kpis[2].metric("Known network links", status.get("network_graph", {}).get("total_edges", 0))
kpis[3].metric("Saved experiments", len(history))

topology_tab, run_tab, compare_tab = st.tabs(["Cluster topology", "Run benchmark", "Compare & export"])

with topology_tab:
    left, right = st.columns([1.1, 1])
    with left:
        st.subheader("Node status and resources")
        rows = []
        for node in nodes:
            sample = metrics.get(node["node_id"])
            rows.append({
                "Node": node["node_id"],
                "Host": node["hostname"],
                "State": node["status"],
                "CPU %": round(sample.cpu_percent, 1)
                if sample and sample.cpu_percent is not None else None,
                "Memory used MiB": round(sample.memory_used_mb, 1)
                if sample and sample.memory_used_mb is not None else None,
                "Memory total MiB": round(sample.memory_total_mb, 1)
                if sample and sample.memory_total_mb is not None else None,
                "Active tasks": sample.active_tasks if sample else None,
                "Heartbeat delay ms": round(
                    sample.additional_metrics.get("heartbeat_delay_seconds") * 1000, 2
                ) if sample and sample.additional_metrics.get("heartbeat_delay_seconds") is not None else None,
                "Completed": sample.additional_metrics.get("tasks_completed") if sample else None,
                "Failed": sample.additional_metrics.get("tasks_failed") if sample else None,
                "Last heartbeat": node["last_heartbeat"],
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    with right:
        st.subheader("Coordinator → workers")
        graph_nodes = status.get("network_graph", {}).get("nodes", [])
        figure = go.Figure()
        if graph_nodes:
            count = len(graph_nodes)
            xs = [0.0] + [0.9 * math.cos(2 * math.pi * index / count) for index in range(count)]
            ys = [0.0] + [0.9 * math.sin(2 * math.pi * index / count) for index in range(count)]
            for index in range(1, count + 1):
                figure.add_trace(go.Scatter(
                    x=[xs[0], xs[index]], y=[ys[0], ys[index]], mode="lines",
                    line={"color": "#73839b", "width": 1}, hoverinfo="skip", showlegend=False,
                ))
            labels = ["Coordinator"] + [node["id"] for node in graph_nodes]
            colors = ["#4f8cff"] + [
                "#27ae60" if node["status"] == "READY" else
                "#f39c12" if node["status"] == "BUSY" else "#e74c3c"
                for node in graph_nodes
            ]
            figure.add_trace(go.Scatter(
                x=xs, y=ys, mode="markers+text", text=labels,
                textposition="bottom center", marker={"size": [32] + [24] * count, "color": colors},
                hovertext=["Coordinator"] + [f"{node['id']} — {node['status']}" for node in graph_nodes],
                hoverinfo="text", showlegend=False,
            ))
        figure.update_layout(
            height=360, margin={"l": 10, "r": 10, "t": 10, "b": 10},
            xaxis={"visible": False}, yaxis={"visible": False},
        )
        st.plotly_chart(figure, use_container_width=True)

    cpu_rows = [
        {"time": sample.timestamp, "worker": sample.node_id, "CPU %": sample.cpu_percent}
        for sample in samples if sample.cpu_percent is not None
    ]
    if cpu_rows:
        st.subheader("Per-worker CPU timeline")
        st.plotly_chart(
            px.line(pd.DataFrame(cpu_rows), x="time", y="CPU %", color="worker"),
            use_container_width=True,
        )
    st.subheader("Distributed event timeline")
    if events:
        frame = pd.DataFrame(events)
        frame = frame.sort_values(["logical_clock", "source_node_id", "event_id"])
        columns = [name for name in (
            "logical_clock", "timestamp", "source_node_id", "event_type", "payload"
        ) if name in frame]
        st.dataframe(frame[columns], use_container_width=True, hide_index=True)
    else:
        st.caption("No events received yet.")

with run_tab:
    st.subheader("Run a distributed experiment")
    with st.form("benchmark-form"):
        workload = st.selectbox("Workload", ["cpu", "data", "mixed"])
        workers = st.select_slider("Workers", options=[1, 2, 4, 6, 8], value=2)
        scheduler = st.selectbox("Scheduler", ["dynamic", "static", "resource-aware"])
        repeats = st.number_input("Independent trials", min_value=1, max_value=10, value=1)
        task_count = st.number_input(
            "Tasks / partitions", min_value=1, max_value=400, value=16
        )
        complexity = st.number_input(
            "Numbers per CPU task / hash rounds", min_value=1, max_value=1_000_000, value=10_000
        )
        record_count = st.number_input("Data records", min_value=1, max_value=1_000_000, value=1000)
        dataset_path = output_dir = result_url = None
        network_timeout = 10.0
        if workload == "mixed":
            dataset_path = st.text_input("Shared UTF-8 dataset path")
            output_dir = st.text_input("Worker output directory")
            result_url = st.text_input("HTTP result receiver URL")
            network_timeout = st.number_input("Network timeout (seconds)", min_value=0.1, value=10.0)
        submitted = st.form_submit_button("Run and validate", type="primary")
    if submitted:
        if workers > len(active_nodes):
            st.error(
                f"Requested {workers} workers, but only {len(active_nodes)} are active. "
                "Scale the Compose worker service or start more worker processes first."
            )
        else:
            experiment_id = f"web-{uuid4().hex}"
            outcome = {}

            def run_in_background():
                try:
                    outcome["result"] = client.run_experiment(
                        workload=workload, nodes=workers, tasks=int(task_count),
                        complexity=int(complexity), records=int(record_count),
                        scheduler_type=scheduler.upper().replace("-", "_"),
                        repeat_count=int(repeats),
                        dataset_path=dataset_path,
                        output_dir=output_dir,
                        result_url=result_url,
                        network_timeout=float(network_timeout),
                        experiment_id=experiment_id,
                    )
                except Exception as exc:
                    outcome["error"] = exc

            runner = threading.Thread(target=run_in_background, daemon=True)
            runner.start()
            progress_status = st.status("Starting benchmark…", expanded=True)
            phase_slot = st.empty()
            progress_slot = st.empty()
            chart_slot = st.empty()
            latest_progress = {
                "phase": "Measuring sequential baseline",
                "status": "RUNNING",
                "progress_fraction": 0.0,
                "completed_tasks": 0,
                "total_tasks": 0,
                "elapsed_seconds": 0.0,
                "points": [{"elapsed_seconds": 0.0, "completed_tasks": 0}],
            }

            while runner.is_alive():
                try:
                    fetched = client.experiment_progress(experiment_id)
                    if isinstance(fetched, dict):
                        latest_progress = fetched
                except Exception:
                    # The sequential baseline is local to the client and finishes
                    # before the coordinator begins publishing live progress.
                    pass

                phase = latest_progress.get("phase", "Preparing benchmark")
                completed = latest_progress.get("completed_tasks", 0)
                total = latest_progress.get("total_tasks", 0)
                elapsed = latest_progress.get("elapsed_seconds", 0.0)
                fraction = min(1.0, max(0.0, latest_progress.get("progress_fraction", 0.0)))
                phase_slot.markdown(
                    f"**{phase}** · {completed:,}/{total:,} tasks · {elapsed:.1f}s elapsed"
                )
                progress_slot.progress(fraction, text=f"{fraction:.0%} complete")
                points = latest_progress.get("points", [])
                if points:
                    chart = go.Figure(go.Scatter(
                        x=[point.get("elapsed_seconds", 0.0) for point in points],
                        y=[point.get("completed_tasks", 0) for point in points],
                        mode="lines+markers",
                        name="Tasks completed",
                        line={"color": "#36d6c5", "width": 3, "shape": "spline"},
                        marker={"size": 7, "color": "#8a70ff"},
                        fill="tozeroy",
                        fillcolor="rgba(54, 214, 197, 0.12)",
                        hovertemplate="%{x:.2f}s · %{y} tasks<extra></extra>",
                    ))
                    chart.update_layout(
                        title="Live benchmark progress",
                        xaxis_title="Elapsed time (seconds)",
                        yaxis_title="Tasks completed",
                        template="plotly_dark",
                        height=360,
                        margin={"l": 10, "r": 10, "t": 55, "b": 10},
                        hovermode="x unified",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(12,17,31,0.65)",
                    )
                    chart_slot.plotly_chart(chart, use_container_width=True)
                time.sleep(0.35)

            runner.join()
            try:
                final_progress = client.experiment_progress(experiment_id)
                if isinstance(final_progress, dict):
                    latest_progress = final_progress
            except Exception:
                pass
            st.session_state["last_progress"] = latest_progress
            if "error" in outcome:
                progress_status.update(label="Benchmark failed", state="error")
                st.error(f"Benchmark request failed: {outcome['error']}")
            else:
                st.session_state["last_result"] = outcome["result"]
                progress_status.update(label="Benchmark complete", state="complete")
    result = st.session_state.get("last_result")
    if result is not None:
        values = st.columns(6)
        values[0].metric("Status", result.status.value)
        values[1].metric("Tasks", f"{result.successful_tasks}/{result.total_tasks}")
        values[2].metric("Duration", f"{result.duration_seconds:.3f}s")
        values[3].metric("Throughput", f"{result.throughput_tasks_per_sec:.2f} tasks/s")
        values[4].metric("Speedup", f"{result.speedup:.2f}x")
        score = result.metrics_summary.get("benchmark_score")
        values[5].metric("PyCoreMark points", f"{score['points']:.2f}" if score else "N/A")
        check = result.metrics_summary.get("consistency")
        if check and check.get("valid"):
            st.success(f"Sequential and distributed checksums match: `{check['distributed_checksum']}`")
        elif check:
            st.error("Distributed result differs from the sequential baseline.")
        progress = st.session_state.get("last_progress")
        if progress and progress.get("points"):
            st.subheader("Task completion over time")
            points = progress["points"]
            result_chart = go.Figure(go.Scatter(
                x=[point.get("elapsed_seconds", 0.0) for point in points],
                y=[point.get("completed_tasks", 0) for point in points],
                mode="lines+markers",
                name="Tasks completed",
                line={"color": "#36d6c5", "width": 3, "shape": "spline"},
                marker={"size": 7, "color": "#8a70ff"},
                fill="tozeroy",
                fillcolor="rgba(54, 214, 197, 0.12)",
                hovertemplate="%{x:.2f}s · %{y} tasks<extra></extra>",
            ))
            result_chart.update_layout(
                title="Benchmark progress",
                xaxis_title="Elapsed time (seconds)",
                yaxis_title="Tasks completed",
                template="plotly_dark",
                height=360,
                margin={"l": 10, "r": 10, "t": 55, "b": 10},
                hovermode="x unified",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(12,17,31,0.65)",
            )
            st.plotly_chart(result_chart, use_container_width=True)
        with st.expander("Full calculation inputs, trace, and workload output"):
            st.json(result.metrics_summary)

with compare_tab:
    st.subheader("Saved experiment comparisons")
    if not history:
        st.info("No saved experiment results. Run a benchmark first.")
    else:
        table_rows = []
        for result in history:
            inputs = result.metrics_summary.get("calculation_inputs", {})
            table_rows.append({
                "Experiment": result.experiment_id,
                "Status": result.status.value,
                "Workers": inputs.get("workers_that_completed_tasks", 0),
                "Scheduler": inputs.get("scheduler_type", "unknown"),
                "Workload": inputs.get("workload", "unknown"),
                "Duration (s)": result.duration_seconds,
                "Throughput (tasks/s)": result.throughput_tasks_per_sec,
                "PyCoreMark points": result.metrics_summary.get("benchmark_score", {}).get("points"),
                "Speedup": result.speedup,
                "Efficiency": result.efficiency,
                "Communication overhead (s)": result.communication_overhead_seconds,
                "Average latency (s)": result.average_latency_seconds,
                "Retry rate": result.metrics_summary.get("reliability", {}).get("retry_rate", 0.0),
                "Task failure rate": result.metrics_summary.get("reliability", {}).get("task_failure_rate", 0.0),
                "Recovery time (s)": result.metrics_summary.get("reliability", {}).get("mean_recovery_time_seconds", 0.0),
                "Consistent": result.metrics_summary.get("consistency", {}).get("valid"),
            })
        frame = pd.DataFrame(table_rows)
        st.dataframe(frame, use_container_width=True, hide_index=True)
        st.plotly_chart(
            px.line(frame.sort_values("Workers"), x="Workers", y="Throughput (tasks/s)",
                    color="Experiment", markers=True),
            use_container_width=True,
        )
        st.plotly_chart(
            px.line(frame.sort_values("Workers"), x="Workers", y="Duration (s)",
                    color="Scheduler", markers=True),
            use_container_width=True,
        )
        st.plotly_chart(
            px.bar(frame, x="Experiment", y=["Speedup", "Efficiency"], barmode="group"),
            use_container_width=True,
        )
        st.plotly_chart(
            px.bar(frame, x="Experiment", y="Communication overhead (s)", color="Scheduler"),
            use_container_width=True,
        )
        node_rows = []
        for result in history:
            for node_id, values in result.metrics_summary.get("per_node_metrics", {}).items():
                node_rows.append({"Experiment": result.experiment_id, "Node": node_id, **values})
        if node_rows:
            st.subheader("Per-node task distribution and resource peaks")
            st.dataframe(pd.DataFrame(node_rows), use_container_width=True, hide_index=True)
            st.plotly_chart(
                px.bar(pd.DataFrame(node_rows), x="Experiment", y="tasks_completed", color="Node", barmode="group"),
                use_container_width=True,
            )
            if any(row.get("average_heartbeat_delay_seconds") is not None for row in node_rows):
                st.plotly_chart(
                    px.bar(pd.DataFrame(node_rows), x="Experiment", y="average_heartbeat_delay_seconds",
                           color="Node", barmode="group", title="Heartbeat latency estimate"),
                    use_container_width=True,
                )
        st.plotly_chart(
            px.bar(frame, x="Experiment", y=["Retry rate", "Task failure rate"], barmode="group"),
            use_container_width=True,
        )
        trace_rows = []
        for result in history:
            summaries = result.metrics_summary.get("trials", [result.metrics_summary])
            for summary in summaries:
                for span in summary.get("trace", {}).get("spans", []):
                    if span.get("start_time") and span.get("end_time") and span.get("task_id"):
                        trace_rows.append({
                            **span,
                            "experiment_id": result.experiment_id,
                            "start_time": pd.to_datetime(span["start_time"]),
                            "end_time": pd.to_datetime(span["end_time"]),
                            "node_id": span.get("node_id", "unknown"),
                        })
        if trace_rows:
            st.subheader("Task execution timeline")
            st.plotly_chart(
                px.timeline(
                    pd.DataFrame(trace_rows), x_start="start_time", x_end="end_time",
                    y="task_id", color="node_id", hover_data=["experiment_id", "attempt"],
                ),
                use_container_width=True,
            )
        st.download_button(
            "Download CSV", frame.to_csv(index=False),
            file_name=f"pycoremark-results-{datetime.now():%Y%m%d-%H%M%S}.csv",
            mime="text/csv",
        )

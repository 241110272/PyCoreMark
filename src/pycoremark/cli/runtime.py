"""Composition root for a working, in-process virtual cluster."""

import os
from collections import deque
from pathlib import Path
from queue import Empty
from threading import Event, Thread
import time
from uuid import uuid4

from pycoremark.analysis.analyzer import PerformanceAnalyzer
from pycoremark.analysis.baseline import run_sequential_baseline
from pycoremark.analysis.metrics_calc import (
    EfficiencyCalculator,
    LoadBalanceCalculator,
    SpeedupCalculator,
    ThroughputCalculator,
)
from pycoremark.analysis.overhead import OverheadCalculator
from pycoremark.cluster.detector import FailureDetector
from pycoremark.cluster.heartbeat import HeartbeatService
from pycoremark.cluster.manager import ClusterManager
from pycoremark.cluster.registration import NodeRegistrationService
from pycoremark.cluster.registry import InMemoryNodeRegistry
from pycoremark.common.config import ClusterConfig, load_config_from_env
from pycoremark.communication.broker import InMemoryMessageBroker
from pycoremark.communication.clock import LamportClock
from pycoremark.communication.events import EventPublisher
from pycoremark.coordinator.aggregator import ResultAggregator
from pycoremark.coordinator.controller import ExperimentController
from pycoremark.coordinator.dispatcher import TaskDispatcher
from pycoremark.dashboard.topology import ClusterTopologyService
from pycoremark.models.experiment import ExperimentConfig, ExperimentResult
from pycoremark.models.event import ClusterEvent, ClusterEventType
from pycoremark.models.node import NodeInfo, NodeResources, NodeStatus
from pycoremark.models.workload import WorkloadConfig, WorkloadType
from pycoremark.monitoring.cluster_collector import ClusterMetricsCollector
from pycoremark.monitoring.collector import NodeMetricsCollector
from pycoremark.scheduling.dynamic import DynamicScheduler
from pycoremark.storage.sqlite import SQLiteExperimentRepository
from pycoremark.worker.agents import WorkerHeartbeatAgent, WorkerMetricsAgent
from pycoremark.worker.executor import TaskExecutor
from pycoremark.worker.service import WorkerService
from pycoremark.workloads.cpu import CPUWorkload
from pycoremark.workloads.data import PartitionedDataWorkload
from pycoremark.workloads.mixed import MixedWorkload
from pycoremark.workloads.registry import WorkloadRegistry


def _default_database_path() -> Path:
    base = Path(os.environ.get("LOCALAPPDATA", Path.home() / ".local" / "share"))
    return base / "PyCoreMark" / "pycoremark.sqlite3"


class LocalClusterRuntime:
    """Run coordinator and virtual worker processes over one shared broker."""

    def __init__(self, database_path: str | Path | None = None) -> None:
        self.config = load_config_from_env()
        self.registry = InMemoryNodeRegistry()
        self.registration = NodeRegistrationService(self.registry)
        self.heartbeats = HeartbeatService(self.registry)
        self.detector = FailureDetector(self.registry, timeout_seconds=6.0)
        self.manager = ClusterManager(
            self.registry, self.registration, self.heartbeats, self.detector
        )
        self.broker = InMemoryMessageBroker()
        self._event_clock = LamportClock()
        self.scheduler = DynamicScheduler()
        self.aggregator = ResultAggregator()
        self.dispatcher = TaskDispatcher(self.broker, clock=self._event_clock)
        self.workloads = WorkloadRegistry()
        self.workloads.register("cpu_benchmark", CPUWorkload)
        self.workloads.register("partitioned_data", PartitionedDataWorkload)
        self.workloads.register("mixed", MixedWorkload)
        if database_path is None:
            if self.config.database_url == "sqlite:///pycoremark.db":
                database_path = _default_database_path()
            elif self.config.database_url.startswith("sqlite:///"):
                database_path = self.config.database_url.removeprefix("sqlite:///")
            else:
                raise ValueError("Local runtime currently supports SQLite database URLs only")
        self.experiments = SQLiteExperimentRepository(database_path)
        self.metrics = ClusterMetricsCollector()
        self._events: deque[dict] = deque(maxlen=2000)
        self.event_publisher = EventPublisher(self.broker, self._event_clock)
        self.detector._event_publisher = self.event_publisher
        self.topology = ClusterTopologyService(self.registry)
        self.controller = ExperimentController(
            cluster_manager=self.manager,
            scheduler=self.scheduler,
            dispatcher=self.dispatcher,
            aggregator=self.aggregator,
            workload_registry=self.workloads,
            analyzer=PerformanceAnalyzer(
                throughput_calc=ThroughputCalculator(),
                speedup_calc=SpeedupCalculator(),
                efficiency_calc=EfficiencyCalculator(),
                load_balance_calc=LoadBalanceCalculator(),
                overhead_calc=OverheadCalculator(),
            ),
            experiment_repository=self.experiments,
            result_timeout_seconds=60.0,
            metrics_provider=self.metrics.get_all_samples,
        )
        self.workers: list[WorkerService] = []
        self._worker_sequence = 0
        self._failure_rate = 0.0
        self._simulated_delay_ms = 0.0
        self._stop = Event()
        self._monitor_thread: Thread | None = None
        self._heartbeat_queue = self.broker.subscribe("cluster.heartbeats")
        self._metrics_queue = self.broker.subscribe("cluster.metrics")
        self._events_queue = self.broker.subscribe("cluster.events")
        self._is_running = False

    def start(
        self,
        node_count: int = 2,
        *,
        failure_rate: float = 0.0,
        simulated_delay_ms: float = 0.0,
    ) -> None:
        if isinstance(node_count, bool) or not isinstance(node_count, int) or node_count < 1:
            raise ValueError("node_count must be a positive integer")
        if self._is_running:
            return
        self._failure_rate = failure_rate
        self._simulated_delay_ms = simulated_delay_ms
        self._is_running = True
        self._stop.clear()
        self._monitor_thread = Thread(
            target=self._monitor_loop, name="pycoremark-local-monitor", daemon=True
        )
        self._monitor_thread.start()

        for index in range(node_count):
            self._add_worker(node_count)

    def _add_worker(self, target_count: int) -> None:
        self._worker_sequence += 1
        worker_id = f"worker-{self._worker_sequence}"
        config = ClusterConfig(
            worker_id=worker_id,
            worker_cores=max(1, (os.cpu_count() or 2) // max(target_count, 1)),
            worker_memory_mb=4096,
            worker_failure_rate=self._failure_rate,
            simulated_network_delay_ms=self._simulated_delay_ms,
        )
        worker_workloads = WorkloadRegistry()
        worker_workloads.register("cpu_benchmark", CPUWorkload)
        worker_workloads.register("partitioned_data", PartitionedDataWorkload)
        worker_workloads.register("mixed", MixedWorkload)
        executor = TaskExecutor(
            worker_id,
            worker_workloads,
            failure_rate=config.worker_failure_rate,
            artificial_delay_ms=config.simulated_network_delay_ms,
        )
        heartbeat_agent = WorkerHeartbeatAgent(worker_id, self.broker, interval_seconds=1.0)
        metrics_agent = WorkerMetricsAgent(
            NodeMetricsCollector(worker_id), self.broker, interval_seconds=1.0
        )
        worker = WorkerService(
            config=config,
            executor=executor,
            heartbeat_agent=heartbeat_agent,
            metrics_agent=metrics_agent,
            broker=self.broker,
        )
        self.registration.register_worker(worker.build_node_info())
        self.event_publisher.publish("cluster.events", ClusterEvent(
            event_id=uuid4().hex,
            event_type=ClusterEventType.NODE_REGISTERED,
            source_node_id=worker_id,
            payload={"node_id": worker_id},
        ))
        worker.start()
        self.workers.append(worker)

    def scale_to(self, desired_nodes: int) -> int:
        """Add or drain local virtual workers without restarting the coordinator."""
        if isinstance(desired_nodes, bool) or not isinstance(desired_nodes, int) or desired_nodes < 1:
            raise ValueError("desired_nodes must be a positive integer")
        if not self._is_running:
            self.start(desired_nodes)
            return len(self.workers)
        while len(self.workers) < desired_nodes:
            self._add_worker(desired_nodes)
        while len(self.workers) > desired_nodes:
            worker = self.workers.pop()
            worker.stop()
            node = self.registry.get(worker.config.worker_id)
            if node is not None:
                node.active_tasks = 0
                self.registry.update_node(node)
                self.registry.update_status(node.node_id, NodeStatus.OFFLINE)
                self.event_publisher.publish("cluster.events", ClusterEvent(
                    event_id=uuid4().hex,
                    event_type=ClusterEventType.NODE_DEREGISTERED,
                    source_node_id=node.node_id,
                    payload={"node_id": node.node_id},
                ))
            self.heartbeats.forget_node(worker.config.worker_id)
        return len(self.workers)

    def _payload(self, message):
        if hasattr(message, "message_id") and hasattr(message, "topic"):
            return message.payload
        return message

    def _monitor_loop(self) -> None:
        next_liveness_check = time.monotonic()
        while not self._stop.is_set():
            for queue, handler in (
                (self._heartbeat_queue, self.manager.process_heartbeat),
                (self._metrics_queue, self._ingest_metrics),
                (self._events_queue, self._ingest_event),
            ):
                try:
                    message = queue.get(timeout=0.02)
                except Empty:
                    continue
                try:
                    handler(self._payload(message))
                except Exception:
                    # Monitoring errors should not terminate the runtime loop.
                    continue
            if time.monotonic() >= next_liveness_check:
                self.manager.check_cluster_liveness()
                next_liveness_check = time.monotonic() + 1.0

    def _ingest_metrics(self, sample) -> None:
        self.metrics.ingest_sample(sample)
        self.controller.update_metrics(sample)

    def _ingest_event(self, event: ClusterEvent) -> None:
        self._event_clock.update(event.logical_clock)
        self._events.append({
            "event_id": event.event_id,
            "event_type": event.event_type.value,
            "source_node_id": event.source_node_id,
            "timestamp": event.timestamp.isoformat(),
            "logical_clock": event.logical_clock,
            "coordinator_logical_clock": self._event_clock.get_time(),
            "payload": event.payload,
        })

    def events(self) -> list[dict]:
        return list(self._events)

    def run_cpu(
        self,
        *,
        task_count: int = 8,
        complexity: int = 25000,
        node_count: int | None = None,
        scheduler_type: str = "DYNAMIC",
        repeat_count: int = 1,
    ) -> ExperimentResult:
        if not self._is_running:
            self.start(node_count or 2)
        elif node_count is not None and node_count != len(self.workers):
            self.scale_to(node_count)
        selected_nodes = node_count or len(self.workers)
        workload_config = WorkloadConfig(
            name="cpu_benchmark",
            workload_type=WorkloadType.CPU_BOUND,
            total_tasks=task_count,
            task_complexity=complexity,
        )
        baseline = run_sequential_baseline(CPUWorkload(workload_config))
        config = ExperimentConfig(
            experiment_id=f"cpu-{int(time.time() * 1000)}",
            name="CPU prime benchmark",
            workload=workload_config,
            scheduler_type=scheduler_type,
            target_nodes=selected_nodes,
            parameters={
                "baseline_single_node_duration": baseline.duration_seconds,
                "baseline_checksum": baseline.checksum,
            },
            repeat_count=repeat_count,
        )
        return self.controller.run_experiment(config)

    def run_data(
        self,
        *,
        record_count: int = 1000,
        node_count: int | None = None,
        scheduler_type: str = "DYNAMIC",
        repeat_count: int = 1,
    ) -> ExperimentResult:
        if not self._is_running:
            self.start(node_count or 2)
        elif node_count is not None and node_count != len(self.workers):
            self.scale_to(node_count)
        workload_config = WorkloadConfig(
            name="partitioned_data",
            workload_type=WorkloadType.DATA_PARTITIONED,
            total_tasks=max(1, min(16, len(self.workers) * 2)),
            dataset_size_records=record_count,
            partitions=max(1, min(16, len(self.workers) * 2)),
        )
        baseline = run_sequential_baseline(PartitionedDataWorkload(workload_config))
        config = ExperimentConfig(
            experiment_id=f"data-{int(time.time() * 1000)}",
            name="Partitioned word-count benchmark",
            workload=workload_config,
            scheduler_type=scheduler_type,
            target_nodes=node_count or len(self.workers),
            parameters={
                "baseline_single_node_duration": baseline.duration_seconds,
                "baseline_checksum": baseline.checksum,
            },
            repeat_count=repeat_count,
        )
        return self.controller.run_experiment(config)

    def run_mixed(
        self,
        *,
        dataset_path: str,
        output_dir: str,
        result_url: str,
        hash_rounds: int = 2,
        partitions: int = 4,
        network_timeout: float = 10.0,
        node_count: int | None = None,
        scheduler_type: str = "DYNAMIC",
        repeat_count: int = 1,
    ) -> ExperimentResult:
        if not self._is_running:
            self.start(node_count or 2)
        elif node_count is not None and node_count != len(self.workers):
            self.scale_to(node_count)
        workload_config = WorkloadConfig(
            name="mixed",
            workload_type=WorkloadType.MIXED,
            total_tasks=partitions,
            task_complexity=hash_rounds,
            partitions=partitions,
            parameters={
                "dataset_path": dataset_path,
                "output_dir": output_dir,
                "result_url": result_url,
                "network_timeout_seconds": network_timeout,
            },
        )
        baseline = run_sequential_baseline(MixedWorkload(workload_config))
        config = ExperimentConfig(
            experiment_id=f"mixed-{int(time.time() * 1000)}",
            name="Mixed file/compute/network benchmark",
            workload=workload_config,
            scheduler_type=scheduler_type,
            target_nodes=node_count or len(self.workers),
            repeat_count=repeat_count,
            parameters={
                "baseline_single_node_duration": baseline.duration_seconds,
                "baseline_checksum": baseline.checksum,
            },
        )
        return self.controller.run_experiment(config)

    def status(self) -> dict:
        return self.topology.get_topology_snapshot()

    def history(self) -> list[ExperimentResult]:
        return self.experiments.list_all()

    def stop(self) -> None:
        if not self._is_running:
            return
        self._stop.set()
        for worker in reversed(self.workers):
            worker.stop()
        self.workers.clear()
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=2)
            self._monitor_thread = None
        for node in self.registry.list_all():
            self.registry.update_status(node.node_id, NodeStatus.OFFLINE)
        self._is_running = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

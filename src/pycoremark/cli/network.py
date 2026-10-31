"""Runtime composition for a coordinator with remote worker processes."""

from collections import deque
from threading import RLock

from pycoremark.analysis.analyzer import PerformanceAnalyzer
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
from pycoremark.common.config import ClusterConfig
from pycoremark.communication.clock import LamportClock
from pycoremark.communication.grpc_transport import GrpcCoordinatorServer
from pycoremark.communication.fastapi_transport import FastAPIClusterServer
from pycoremark.communication.http_transport import HTTPTaskDispatcher
from pycoremark.communication.redis_streams import (
    RedisStreamsCoordinatorObserver,
    RedisStreamsTaskDispatcher,
)
from pycoremark.coordinator.aggregator import ResultAggregator
from pycoremark.coordinator.controller import ExperimentController
from pycoremark.dashboard.topology import ClusterTopologyService
from pycoremark.models.experiment import ExperimentConfig
from pycoremark.models.event import ClusterEvent, ClusterEventType
from pycoremark.models.node import NodeStatus
from pycoremark.models.workload import WorkloadConfig, WorkloadType
from pycoremark.monitoring.cluster_collector import ClusterMetricsCollector
from pycoremark.scheduling.dynamic import DynamicScheduler
from pycoremark.storage.sqlite import SQLiteExperimentRepository
from pycoremark.workloads.cpu import CPUWorkload
from pycoremark.workloads.data import PartitionedDataWorkload
from pycoremark.workloads.mixed import MixedWorkload
from pycoremark.workloads.registry import WorkloadRegistry
from uuid import uuid4


class _EventSink:
    def __init__(self, handler):
        self._handler = handler

    def publish(self, topic: str, event: ClusterEvent) -> None:
        self._handler(event)


class NetworkCoordinatorRuntime:
    """HTTP coordinator runtime that accepts real worker processes."""

    def __init__(self, config: ClusterConfig) -> None:
        self.config = config
        self.registry = InMemoryNodeRegistry()
        self.registration = NodeRegistrationService(
            self.registry, auth_token=config.registration_auth_token
        )
        self.heartbeats = HeartbeatService(self.registry)
        self.detector = FailureDetector(
            self.registry, timeout_seconds=config.failure_timeout
        )
        self.manager = ClusterManager(
            self.registry, self.registration, self.heartbeats, self.detector
        )
        self.scheduler = DynamicScheduler()
        self.metrics = ClusterMetricsCollector()
        self._events = deque(maxlen=2000)
        self._events_lock = RLock()
        self._event_clock = LamportClock()
        self.detector._event_publisher = _EventSink(self._record_event)
        self.redis_client = None
        self.redis_observer = None
        self.topology = ClusterTopologyService(self.registry)
        self.server = FastAPIClusterServer(
            config.coordinator_host,
            config.coordinator_port,
            registration_service=self.registration,
            cluster_manager=self.manager,
            metrics_handler=self._ingest_metrics,
            topology_service=self.topology,
            metrics_provider=self._latest_metrics,
            metrics_history_provider=self._metrics_history,
            history_provider=self._history_payload,
            experiment_handler=self._handle_experiment,
            auth_token=config.registration_auth_token,
            event_handler=self._record_event,
            events_provider=self._event_payload,
            node_event_handler=self._emit_node_event,
        )
        self.grpc_server = GrpcCoordinatorServer(
            config.coordinator_host,
            config.grpc_port,
            registration_service=self.registration,
            cluster_manager=self.manager,
            metrics_handler=self._ingest_metrics,
            event_handler=self._record_event,
            result_queue=self.server.results,
            result_handler=self.server.submit_result,
            task_getter=self.server.get_task,
            cancel_handler=self.server.cancel_task,
            node_event_handler=self._emit_node_event,
            auth_token=config.registration_auth_token,
        )
        address_host = config.coordinator_host
        if address_host in {"0.0.0.0", "::"}:
            address_host = "127.0.0.1"
        if config.broker_url.startswith("redis://"):
            import redis
            self.redis_client = redis.Redis.from_url(
                config.broker_url, decode_responses=True, health_check_interval=30
            )
            self.redis_client.ping()
            self.dispatcher = RedisStreamsTaskDispatcher(
                self.redis_client, clock=self._event_clock
            )
            self.redis_observer = RedisStreamsCoordinatorObserver(
                self.redis_client, self._ingest_metrics, self._record_event
            )
        else:
            self.dispatcher = HTTPTaskDispatcher(
                f"http://{address_host}:{self.server.address[1]}",
                self.server.results,
                auth_token=config.registration_auth_token,
                clock=self._event_clock,
            )
        self.aggregator = ResultAggregator()
        self.workloads = WorkloadRegistry()
        self.workloads.register("cpu_benchmark", CPUWorkload)
        self.workloads.register("partitioned_data", PartitionedDataWorkload)
        self.workloads.register("mixed", MixedWorkload)
        self.repository = SQLiteExperimentRepository(
            config.database_url.removeprefix("sqlite:///")
        )
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
            experiment_repository=self.repository,
            metrics_provider=self.metrics.get_all_samples,
        )

    def _ingest_metrics(self, sample) -> None:
        self.metrics.ingest_sample(sample)
        self.controller.update_metrics(sample)

    def _latest_metrics(self) -> dict:
        from dataclasses import asdict
        output = {}
        for node_id, sample in self.metrics.get_latest_node_samples(
            max_age_seconds=10
        ).items():
            item = asdict(sample)
            latencies = self.heartbeats.get_latency_history(node_id)
            item["additional_metrics"]["heartbeat_delay_seconds"] = (
                latencies[-1] if latencies else None
            )
            output[node_id] = item
        return output

    def _metrics_history(self) -> list[dict]:
        from dataclasses import asdict
        return [asdict(sample) for sample in self.metrics.get_all_samples()[-100:]]

    def _record_event(self, event) -> None:
        self._event_clock.update(event.logical_clock)
        with self._events_lock:
            self._events.append({
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "source_node_id": event.source_node_id,
                "timestamp": event.timestamp.isoformat(),
                "logical_clock": event.logical_clock,
                "coordinator_logical_clock": self._event_clock.get_time(),
                "payload": event.payload,
            })

    def _emit_node_event(self, event_type: str, node_id: str) -> None:
        event = ClusterEvent(
            event_id=uuid4().hex,
            event_type=ClusterEventType(event_type),
            source_node_id=node_id,
            payload={"node_id": node_id},
            logical_clock=self._event_clock.tick(),
        )
        self._record_event(event)

    def _event_payload(self) -> list[dict]:
        with self._events_lock:
            return list(self._events)

    def _history_payload(self) -> list[dict]:
        from dataclasses import asdict
        return [asdict(result) for result in self.repository.list_all()[:50]]

    def _handle_experiment(self, payload: dict) -> dict:
        from dataclasses import asdict
        workload = dict(payload["workload"])
        workload["workload_type"] = WorkloadType(workload["workload_type"])
        config = ExperimentConfig(
            experiment_id=payload["experiment_id"],
            name=payload["name"],
            workload=WorkloadConfig(**workload),
            target_nodes=int(payload["target_nodes"]),
            scheduler_type=str(payload.get("scheduler_type", "DYNAMIC")),
            repeat_count=max(1, int(payload.get("repeat_count", 1))),
            parameters=dict(payload.get("parameters", {})),
        )
        return asdict(self.controller.run_experiment(config))

    def start(self) -> None:
        self.server.start()
        self.grpc_server.start()
        if self.redis_observer is not None:
            self.redis_observer.start()

    def stop(self) -> None:
        if self.redis_observer is not None:
            self.redis_observer.stop()
        self.grpc_server.stop()
        self.server.stop()
        if self.redis_client is not None:
            self.redis_client.close()
        for node in self.registry.list_all():
            self.registry.update_status(node.node_id, NodeStatus.OFFLINE)

    def trigger_experiment(self, config: ExperimentConfig):
        return self.controller.run_experiment(config)

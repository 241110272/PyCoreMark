from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
from threading import Thread
from datetime import datetime, timezone
import pytest
import fakeredis

from pycoremark.cli.network import NetworkCoordinatorRuntime
from pycoremark.cli.runtime import LocalClusterRuntime
from pycoremark.analysis.scaling import benchmark_cpu_scaling
from pycoremark.common.config import ClusterConfig
from pycoremark.communication.http_transport import HTTPClusterClient, HTTPMessageBroker
from pycoremark.communication.grpc_transport import GrpcWorkerBroker
from pycoremark.coordinator.aggregator import ResultAggregator
from pycoremark.communication.redis_streams import (
    RedisStreamsTaskDispatcher,
    RedisStreamsWorkerBroker,
)
from pycoremark.models.task import Task, TaskResult, TaskStatus
from pycoremark.models.experiment import ExperimentResult, ExperimentStatus
from pycoremark.reporting.validator import ResultValidator
from pycoremark.monitoring.collector import NodeMetricsCollector
from pycoremark.storage.benchmark import benchmark_storage
from pycoremark.storage.sqlite import SQLiteExperimentRepository
from pycoremark.worker.agents import WorkerHeartbeatAgent, WorkerMetricsAgent
from pycoremark.worker.executor import TaskExecutor
from pycoremark.worker.service import WorkerService
from pycoremark.workloads.cpu import CPUWorkload
from pycoremark.workloads.registry import WorkloadRegistry


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_aggregator_rejects_duplicate_results_and_validator_checks_checksum() -> None:
    aggregator = ResultAggregator()
    aggregator.clear(expected_total=1)
    result = TaskResult("dedupe-task", "worker-a", TaskStatus.COMPLETED)
    assert aggregator.add_result(result) is True
    assert aggregator.add_result(result) is False
    assert aggregator.get_completed_count() == 1

    now = datetime.now(timezone.utc)
    experiment = ExperimentResult(
        experiment_id="bad-checksum",
        status=ExperimentStatus.COMPLETED,
        total_tasks=1,
        successful_tasks=1,
        failed_tasks=0,
        start_time=now,
        end_time=now,
        duration_seconds=1.0,
        throughput_tasks_per_sec=1.0,
        speedup=1.0,
        efficiency=1.0,
        metrics_summary={"consistency": {"valid": False}},
    )
    validation = ResultValidator.validate_experiment_result(experiment)
    assert validation.is_valid is False
    assert any("checksum" in error for error in validation.errors)


def test_local_runtime_executes_and_collects_workload_results(tmp_path: Path) -> None:
    runtime = LocalClusterRuntime(tmp_path / "local.sqlite3")
    try:
        runtime.start(2)
        result = runtime.run_cpu(task_count=4, complexity=50, node_count=2)
        assert result.status.value == "COMPLETED", result.metrics_summary
        assert result.successful_tasks == 4
        assert result.metrics_summary["workload"]["processed_numbers"] == 200
        assert result.metrics_summary["consistency"]["valid"] is True
        assert result.metrics_summary["calculation_inputs"]["baseline_single_node_duration_seconds"] > 0
        assert "p95" in result.metrics_summary["task_latency_seconds"]
        assert result.duration_seconds > 0
    finally:
        runtime.stop()


def test_local_cluster_scales_workers_up_and_down(tmp_path: Path) -> None:
    runtime = LocalClusterRuntime(tmp_path / "scale.sqlite3")
    try:
        runtime.start(1)
        assert len(runtime.manager.get_active_nodes()) == 1
        assert runtime.scale_to(4) == 4
        assert len(runtime.manager.get_active_nodes()) == 4
        assert runtime.scale_to(2) == 2
        assert len(runtime.manager.get_active_nodes()) == 2
        offline = [node for node in runtime.registry.list_all() if node.status.value == "OFFLINE"]
        assert len(offline) == 2
        assert "cpu_benchmark" in runtime.registry.list_all()[0].resources.metadata["supported_workloads"]
    finally:
        runtime.stop()


def test_controller_retries_failed_work_on_another_worker(tmp_path: Path) -> None:
    runtime = LocalClusterRuntime(tmp_path / "retry.sqlite3")
    runtime.start(2)

    class FailOnceDispatcher:
        def __init__(self):
            self.dispatched = []

        def dispatch(self, task, target_node_id):
            self.dispatched.append((task, target_node_id))

        def receive_result(self, timeout=0.1):
            task, worker_id = self.dispatched[-1]
            if len(self.dispatched) == 1:
                return TaskResult(
                    task_id=task.task_id,
                    worker_id=worker_id,
                    status=TaskStatus.FAILED,
                    error_message="simulated transient failure",
                )
            output = runtime.workloads.create_for_task(task).execute_task(task)
            return TaskResult(
                task_id=task.task_id,
                worker_id=worker_id,
                status=TaskStatus.COMPLETED,
                output_data=output,
                execution_time_seconds=0.001,
            )

    dispatcher = FailOnceDispatcher()
    runtime.controller.dispatcher = dispatcher
    try:
        result = runtime.run_cpu(task_count=1, complexity=10, node_count=2)
        assert result.status.value == "COMPLETED"
        assert len(dispatcher.dispatched) == 2
        assert dispatcher.dispatched[0][1] != dispatcher.dispatched[1][1]
        assert result.successful_tasks == 1
    finally:
        runtime.stop()


def test_repeated_static_trials_preserve_checksum_and_trial_data(tmp_path: Path) -> None:
    runtime = LocalClusterRuntime(tmp_path / "trials.sqlite3")
    try:
        runtime.start(2)
        result = runtime.run_cpu(
            task_count=2,
            complexity=25,
            node_count=2,
            scheduler_type="STATIC",
            repeat_count=2,
        )
        assert result.status.value == "COMPLETED"
        assert result.total_tasks == 4
        assert result.metrics_summary["repeat_count"] == 2
        assert result.metrics_summary["consistency"]["valid"] is True
        assert len(result.metrics_summary["trial_ids"]) == 2
    finally:
        runtime.stop()


def test_permanent_task_failure_is_dead_lettered(tmp_path: Path) -> None:
    runtime = LocalClusterRuntime(tmp_path / "dead-letter.sqlite3")
    try:
        runtime.start(1, failure_rate=1.0)
        result = runtime.run_cpu(task_count=1, complexity=10, node_count=1)
        assert result.status.value == "FAILED"
        assert result.failed_tasks == 1
        assert result.metrics_summary["reliability"]["retry_count"] == 3
        assert result.metrics_summary["reliability"]["dead_letter_count"] == 1
        assert runtime.controller.dead_letter_queue.list_all()
    finally:
        runtime.stop()


def test_storage_benchmark_removes_its_temporary_data(tmp_path: Path) -> None:
    result = benchmark_storage(tmp_path, size_mib=1, random_operations=10)
    assert result.sequential_write_mib_s > 0
    assert result.sequential_read_mib_s > 0
    assert result.random_operations == 10
    assert result.data_removed
    assert list(tmp_path.iterdir()) == []


def test_fixed_window_single_vs_multi_core_output_consistency() -> None:
    report = benchmark_cpu_scaling(
        duration_seconds=0.1,
        worker_counts=(1, 2),
        task_count=2,
        task_complexity=1000,
    )
    assert report.consistency_valid
    assert [sample.workers for sample in report.samples] == [1, 2]
    assert all(sample.actual_duration_seconds >= 0.1 for sample in report.samples)
    assert all(
        sample.duration_overshoot_seconds <= sample.longest_task_batch_seconds + 0.02
        for sample in report.samples
    )
    assert all(sample.output_matches_sequential for sample in report.samples)
    assert report.samples[0].expected_checksum == report.samples[1].expected_checksum


def test_distributed_mixed_workload_checksum_consistency(tmp_path: Path) -> None:
    received = []

    class ResultReceiver(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), ResultReceiver)
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    runtime = LocalClusterRuntime(tmp_path / "mixed.sqlite3")
    dataset = tmp_path / "dataset.txt"
    dataset.write_text("Alpha beta\nGamma delta\n", encoding="utf-8")
    try:
        runtime.start(2)
        result = runtime.run_mixed(
            dataset_path=str(dataset),
            output_dir=str(tmp_path / "outputs"),
            result_url=f"http://127.0.0.1:{server.server_port}/results",
            hash_rounds=1,
            partitions=2,
            node_count=2,
        )
        assert result.status.value == "COMPLETED"
        assert result.metrics_summary["consistency"]["valid"] is True
        assert result.metrics_summary["workload"]["processed_records"] == 2
        assert received
    finally:
        runtime.stop()
        server.shutdown()
        server.server_close()
        server_thread.join()


def test_sqlite_experiment_repository_round_trip(tmp_path: Path) -> None:
    runtime = LocalClusterRuntime(tmp_path / "roundtrip.sqlite3")
    try:
        runtime.start(1)
        result = runtime.run_cpu(task_count=2, complexity=10, node_count=1)
        repository = SQLiteExperimentRepository(tmp_path / "roundtrip.sqlite3")
        loaded = repository.get_result(result.experiment_id)
        assert loaded is not None
        assert loaded.status == result.status
        assert loaded.metrics_summary == result.metrics_summary
        assert repository.get_config(result.experiment_id) is not None
    finally:
        runtime.stop()


def test_http_coordinator_runs_a_separate_worker_process(tmp_path: Path) -> None:
    port = _free_port()
    config = ClusterConfig(
        coordinator_host="127.0.0.1",
        coordinator_port=port,
        grpc_port=_free_port(),
        database_url=str(tmp_path / "network.sqlite3"),
    )
    coordinator = NetworkCoordinatorRuntime(config)
    coordinator.start()
    broker = HTTPMessageBroker(f"http://127.0.0.1:{port}")
    registry = WorkloadRegistry()
    registry.register("cpu_benchmark", CPUWorkload)
    worker_config = ClusterConfig(worker_id="remote-test-worker")
    worker = WorkerService(
        worker_config,
        TaskExecutor(worker_config.worker_id, registry),
        WorkerHeartbeatAgent(worker_config.worker_id, broker, interval_seconds=0.2),
        WorkerMetricsAgent(
            NodeMetricsCollector(worker_config.worker_id), broker, interval_seconds=0.2
        ),
        broker,
    )
    try:
        broker.register_node(worker.build_node_info())
        worker.start()
        client = HTTPClusterClient(f"http://127.0.0.1:{port}")
        result = client.run_experiment(
            workload="cpu", nodes=1, tasks=2, complexity=20
        )
        assert result.status.value == "COMPLETED"
        assert result.successful_tasks == 2
        assert result.metrics_summary["workload"]["processed_numbers"] == 40
        assert client.status()["total_nodes"] == 1
        assert client.history()[0].experiment_id == result.experiment_id
    finally:
        worker.stop()
        coordinator.stop()


def test_http_registration_authentication_and_remote_run(tmp_path: Path) -> None:
    port = _free_port()
    token = "test-cluster-shared-token"
    config = ClusterConfig(
        coordinator_host="127.0.0.1",
        coordinator_port=port,
        grpc_port=_free_port(),
        database_url=str(tmp_path / "authenticated.sqlite3"),
        registration_auth_token=token,
    )
    coordinator = NetworkCoordinatorRuntime(config)
    coordinator.start()
    broker = HTTPMessageBroker(f"http://127.0.0.1:{port}", auth_token=token)
    registry = WorkloadRegistry()
    registry.register("cpu_benchmark", CPUWorkload)
    worker_config = ClusterConfig(worker_id="authenticated-worker")
    worker = WorkerService(
        worker_config,
        TaskExecutor(worker_config.worker_id, registry),
        WorkerHeartbeatAgent(worker_config.worker_id, broker, interval_seconds=0.2),
        WorkerMetricsAgent(
            NodeMetricsCollector(worker_config.worker_id), broker, interval_seconds=0.2
        ),
        broker,
    )
    try:
        with pytest.raises(ConnectionError):
            HTTPClusterClient(f"http://127.0.0.1:{port}").status()
        broker.register_node(worker.build_node_info())
        worker.start()
        result = HTTPClusterClient(
            f"http://127.0.0.1:{port}", auth_token=token
        ).run_experiment(workload="cpu", nodes=1, tasks=1, complexity=20)
        assert result.status.value == "COMPLETED"
        assert result.successful_tasks == 1
    finally:
        worker.stop()
        coordinator.stop()


def test_grpc_worker_rpcs_and_http_experiment_api(tmp_path: Path) -> None:
    http_port = _free_port()
    grpc_port = _free_port()
    config = ClusterConfig(
        coordinator_host="127.0.0.1",
        coordinator_port=http_port,
        grpc_port=grpc_port,
        database_url=str(tmp_path / "grpc.sqlite3"),
    )
    coordinator = NetworkCoordinatorRuntime(config)
    coordinator.start()
    broker = GrpcWorkerBroker(
        "127.0.0.1", grpc_port, worker_id="grpc-worker", timeout_seconds=3
    )
    registry = WorkloadRegistry()
    registry.register("cpu_benchmark", CPUWorkload)
    worker_config = ClusterConfig(worker_id="grpc-worker")
    worker = WorkerService(
        worker_config,
        TaskExecutor(worker_config.worker_id, registry),
        WorkerHeartbeatAgent(worker_config.worker_id, broker, interval_seconds=0.2),
        WorkerMetricsAgent(
            NodeMetricsCollector(worker_config.worker_id), broker, interval_seconds=0.2
        ),
        broker,
    )
    try:
        broker.register_node(worker.build_node_info())
        assert broker.health_check()["status"] == "ok"
        worker.start()
        client = HTTPClusterClient(f"http://127.0.0.1:{http_port}")
        result = client.run_experiment(workload="cpu", nodes=1, tasks=2, complexity=25)
        assert result.status.value == "COMPLETED", result.metrics_summary
        assert result.metrics_summary["consistency"]["valid"] is True
        assert any(event["event_type"] == "TASK_STARTED" for event in client.events())
        worker.stop()
        broker.deregister_node(worker_config.worker_id)
        assert client.status()["nodes"][0]["status"] == "OFFLINE"
    finally:
        worker.stop()
        try:
            broker.deregister_node(worker_config.worker_id)
        except Exception:
            pass
        worker.broker.close()
        coordinator.stop()


def test_redis_stream_task_result_ack_and_deduplication() -> None:
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    worker = RedisStreamsWorkerBroker(
        redis_client,
        worker_id="redis-worker",
        coordinator_host="127.0.0.1",
        grpc_port=50051,
    )

    class ControlStub:
        def publish(self, topic, payload):
            pass

        def register_node(self, node):
            pass

        def deregister_node(self, node_id):
            pass

        def close(self):
            pass

    worker._control = ControlStub()
    worker.subscribe("worker.redis-worker.tasks")
    dispatcher = RedisStreamsTaskDispatcher(redis_client)
    task = Task(
        task_id="redis-task-1",
        workload_name="cpu_benchmark",
        payload={"start": 0, "stop": 10},
    )
    dispatcher.dispatch(task, "redis-worker")
    received = worker.poll("worker.redis-worker.tasks", timeout=0.02)
    assert received is not None
    assert received.task_id == task.task_id
    result = TaskResult(
        task_id=task.task_id,
        worker_id="redis-worker",
        status=TaskStatus.COMPLETED,
        output_data={"prime_count": 4},
    )
    worker.publish("cluster.task_results", result)
    aggregated = dispatcher.receive_result(timeout=0.02)
    assert aggregated is not None
    assert aggregated.output_data == result.output_data
    assert dispatcher.receive_result(timeout=0.01) is None

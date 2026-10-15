"""Unit tests verifying creation and schema validity of PyCoreMark data models."""

from datetime import datetime, timezone
from pycoremark.models.event import ClusterEvent, ClusterEventType, RetryPolicy, ValidationResult
from pycoremark.models.experiment import ExperimentConfig, ExperimentResult, ExperimentStatus
from pycoremark.models.metrics import MetricSample, TraceContext, TraceSpan
from pycoremark.models.node import Heartbeat, NodeInfo, NodeResources, NodeStatus
from pycoremark.models.task import Task, TaskAttempt, TaskResult, TaskStatus
from pycoremark.models.workload import WorkloadConfig, WorkloadType


def test_node_models() -> None:
    """Verify NodeResources, NodeInfo, and Heartbeat models."""
    resources = NodeResources(
        total_cores=8,
        available_cores=6,
        total_memory_mb=8192,
        available_memory_mb=6144,
    )
    assert resources.total_cores == 8

    node = NodeInfo(
        node_id="worker-test",
        hostname="10.0.0.1",
        port=9000,
        resources=resources,
        status=NodeStatus.READY,
    )
    assert node.status == NodeStatus.READY
    assert node.resources.available_cores == 6

    heartbeat = Heartbeat(
        node_id="worker-test",
        sequence_number=1,
        status=NodeStatus.READY,
    )
    assert heartbeat.sequence_number == 1


def test_task_models() -> None:
    """Verify Task, TaskAttempt, and TaskResult models."""
    task = Task(
        task_id="task-01",
        workload_name="cpu_bench",
        payload={"param": 42},
    )
    assert task.status == TaskStatus.PENDING
    assert task.max_retries == 3

    attempt = TaskAttempt(
        attempt_number=1,
        worker_id="worker-test",
        started_at=datetime.now(timezone.utc),
    )
    task.attempts.append(attempt)
    assert len(task.attempts) == 1

    result = TaskResult(
        task_id=task.task_id,
        worker_id="worker-test",
        status=TaskStatus.COMPLETED,
        output_data={"ans": 42},
        execution_time_seconds=0.15,
    )
    assert result.status == TaskStatus.COMPLETED


def test_experiment_models() -> None:
    """Verify WorkloadConfig, ExperimentConfig, and ExperimentResult models."""
    wl_config = WorkloadConfig(
        name="test_workload",
        workload_type=WorkloadType.CPU_BOUND,
        total_tasks=10,
    )
    exp_config = ExperimentConfig(
        experiment_id="exp-1",
        name="Test Run",
        workload=wl_config,
    )
    assert exp_config.workload.total_tasks == 10

    exp_result = ExperimentResult(
        experiment_id="exp-1",
        status=ExperimentStatus.COMPLETED,
        total_tasks=10,
        successful_tasks=10,
        failed_tasks=0,
        start_time=datetime.now(timezone.utc),
        throughput_tasks_per_sec=100.0,
    )
    assert exp_result.throughput_tasks_per_sec == 100.0


def test_metrics_and_events() -> None:
    """Verify MetricSample, TraceContext, ClusterEvent, and RetryPolicy models."""
    sample = MetricSample(
        node_id="node-1",
        timestamp=datetime.now(timezone.utc),
        cpu_percent=45.5,
        memory_used_mb=1024.0,
        memory_total_mb=4096.0,
        active_tasks=2,
    )
    assert sample.cpu_percent == 45.5

    trace = TraceContext(trace_id="trace-123")
    assert trace.trace_id == "trace-123"

    event = ClusterEvent(
        event_id="evt-1",
        event_type=ClusterEventType.NODE_REGISTERED,
        source_node_id="node-1",
    )
    assert event.event_type == ClusterEventType.NODE_REGISTERED

    policy = RetryPolicy(max_retries=5)
    assert policy.max_retries == 5

    val = ValidationResult(is_valid=True)
    assert val.is_valid is True

"""Redis Streams transport for distributed tasks, results, metrics, and events."""

from dataclasses import asdict
from datetime import datetime, timezone
import json
import random
import threading
import time
from uuid import uuid4

from redis.exceptions import ResponseError

from pycoremark.communication.clock import LamportClock
from pycoremark.communication.grpc_transport import GrpcWorkerBroker
from pycoremark.communication.http_transport import (
    _event_from_dict,
    _json_default,
    _metric_from_dict,
    _result_from_dict,
    _task_from_dict,
)
from pycoremark.models.event import ClusterEvent, ClusterEventType
from pycoremark.common.logging import get_logger
from pycoremark.models.task import Task, TaskStatus


TASK_PREFIX = "pycoremark:tasks:"
RESULT_STREAM = "pycoremark:results"
METRICS_STREAM = "pycoremark:metrics"
EVENTS_STREAM = "pycoremark:events"
DLQ_STREAM = "pycoremark:dead-letter"
WORKER_GROUP = "pycoremark-workers"
COORDINATOR_GROUP = "pycoremark-coordinators"
logger = get_logger("communication.redis-streams")


def _payload(value) -> str:
    return json.dumps(asdict(value) if hasattr(value, "__dataclass_fields__") else value,
                      default=_json_default, separators=(",", ":"))


def _read_payload(fields: dict):
    return json.loads(fields.get("json", "{}"))


def _ensure_group(client, stream: str, group: str, start_id: str = "0-0") -> None:
    try:
        client.xgroup_create(stream, group, id=start_id, mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


class RedisStreamsWorkerBroker:
    """Redis Streams task broker; membership and heartbeat use gRPC RPCs."""

    def __init__(
        self,
        redis_client,
        *,
        worker_id: str,
        coordinator_host: str,
        grpc_port: int,
        auth_token: str | None = None,
        poll_timeout_seconds: float = 0.5,
        simulated_latency_ms: float = 0.0,
        simulated_failure_rate: float = 0.0,
    ) -> None:
        self.redis = redis_client
        self.worker_id = worker_id
        self.poll_timeout_seconds = poll_timeout_seconds
        self._control = GrpcWorkerBroker(
            coordinator_host,
            grpc_port,
            worker_id=worker_id,
            auth_token=auth_token,
            timeout_seconds=max(2.0, poll_timeout_seconds + 1),
            simulated_latency_ms=simulated_latency_ms,
            simulated_failure_rate=simulated_failure_rate,
        )
        self.simulated_latency_ms = max(0.0, simulated_latency_ms)
        self.simulated_failure_rate = min(1.0, max(0.0, simulated_failure_rate))
        self._task_stream = f"{TASK_PREFIX}{worker_id}"
        self._pending_tasks: dict[str, str] = {}
        self._lock = threading.RLock()

    def subscribe(self, topic: str):
        if topic == f"worker.{self.worker_id}.tasks":
            _ensure_group(self.redis, self._task_stream, WORKER_GROUP)
        return None

    def poll(self, topic: str, timeout: float | None = None):
        if topic != f"worker.{self.worker_id}.tasks":
            return None
        if self._apply_network_impairment():
            return None
        _ensure_group(self.redis, self._task_stream, WORKER_GROUP)
        wait_seconds = self.poll_timeout_seconds if timeout is None else max(0, timeout)
        # Reclaim a previous consumer's unacknowledged task before new work.
        try:
            claimed = self.redis.xautoclaim(
                self._task_stream, WORKER_GROUP, self.worker_id,
                min_idle_time=1000, start_id="0-0", count=1,
            )
            entries = claimed[1] if claimed and len(claimed) > 1 else []
        except (AttributeError, ResponseError):
            entries = []
        if not entries:
            response = self.redis.xreadgroup(
                WORKER_GROUP,
                self.worker_id,
                {self._task_stream: ">"},
                count=1,
                block=max(1, int(wait_seconds * 1000)),
            )
            entries = response[0][1] if response else []
        if not entries:
            return None
        message_id, fields = entries[0]
        task = _task_from_dict(_read_payload(fields))
        with self._lock:
            self._pending_tasks[task.task_id] = message_id
        return task

    def publish(self, topic: str, message) -> None:
        if topic == "cluster.heartbeats":
            self._control.publish(topic, message)
        elif topic == "cluster.metrics":
            if self._apply_network_impairment():
                return
            self.redis.xadd(METRICS_STREAM, {"json": _payload(message)}, maxlen=100_000, approximate=True)
        elif topic == "cluster.events":
            if self._apply_network_impairment():
                return
            self.redis.xadd(EVENTS_STREAM, {"json": _payload(message)}, maxlen=100_000, approximate=True)
        elif topic == "cluster.task_results":
            if self._apply_network_impairment():
                return
            self.redis.xadd(RESULT_STREAM, {"json": _payload(message)}, maxlen=100_000, approximate=True)
            task_id = message.task_id
            with self._lock:
                message_id = self._pending_tasks.pop(task_id, None)
            if message_id is not None:
                self.redis.xack(self._task_stream, WORKER_GROUP, message_id)
        else:
            raise ValueError(f"Unsupported Redis stream topic: {topic}")

    def register_node(self, node) -> None:
        self._control.register_node(node)

    def deregister_node(self, node_id: str) -> None:
        self._control.deregister_node(node_id)

    def health_check(self) -> dict:
        return self._control.health_check()

    def _apply_network_impairment(self) -> bool:
        if self.simulated_latency_ms:
            time.sleep(self.simulated_latency_ms / 1000.0)
        return random.random() < self.simulated_failure_rate

    def close(self) -> None:
        self._control.close()


class RedisStreamsTaskDispatcher:
    """Coordinator dispatcher and result consumer over Redis Streams."""

    def __init__(self, redis_client, *, coordinator_id: str = "coordinator", clock=None) -> None:
        self.redis = redis_client
        self.coordinator_id = coordinator_id
        self._event_clock = clock or LamportClock()
        self._assigned: dict[str, str] = {}
        self._lock = threading.RLock()
        self._consumer = f"{coordinator_id}-{uuid4().hex[:8]}"
        _ensure_group(self.redis, RESULT_STREAM, COORDINATOR_GROUP, "$")

    def publish_event(self, event_type: ClusterEventType, payload: dict) -> int:
        event = ClusterEvent(
            event_id=uuid4().hex,
            event_type=event_type,
            source_node_id=self.coordinator_id,
            payload=payload,
            logical_clock=self._event_clock.tick(),
        )
        self.redis.xadd(EVENTS_STREAM, {"json": _payload(event)}, maxlen=100_000, approximate=True)
        return event.logical_clock

    def dispatch(self, task: Task, target_node_id: str) -> None:
        task.assigned_node_id = target_node_id
        task.status = TaskStatus.SCHEDULED
        task.queued_at = datetime.now(timezone.utc)
        stream = f"{TASK_PREFIX}{target_node_id}"
        with self._lock:
            if task.task_id in self._assigned and task.retry_count <= 0:
                raise ValueError(f"task {task.task_id!r} was already dispatched")
            self._assigned[task.task_id] = target_node_id
        task.logical_clock = self.publish_event(
            ClusterEventType.TASK_SCHEDULED,
            {"task_id": task.task_id, "worker_id": target_node_id},
        )
        self.redis.xadd(stream, {"json": _payload(task)})

    def dead_letter(self, task: Task, reason: str) -> None:
        self.redis.xadd(DLQ_STREAM, {
            "json": _payload({"task": asdict(task), "reason": reason}),
        })

    def receive_result(self, timeout: float = 0.1):
        response = self.redis.xreadgroup(
            COORDINATOR_GROUP,
            self._consumer,
            {RESULT_STREAM: ">"},
            count=1,
            block=max(1, int(max(0.0, timeout) * 1000)),
        )
        if not response:
            return None
        message_id, fields = response[0][1][0]
        result = _result_from_dict(_read_payload(fields))
        with self._lock:
            expected_worker = self._assigned.get(result.task_id)
            if expected_worker != result.worker_id:
                self.redis.xack(RESULT_STREAM, COORDINATOR_GROUP, message_id)
                return None
            self._assigned.pop(result.task_id, None)
        self.redis.xack(RESULT_STREAM, COORDINATOR_GROUP, message_id)
        return result

    def close(self) -> None:
        close = getattr(self.redis, "close", None)
        if callable(close):
            close()


class RedisStreamsCoordinatorObserver:
    """Consumes Redis telemetry/event streams and forwards them to handlers."""

    def __init__(self, redis_client, metrics_handler, event_handler) -> None:
        self.redis = redis_client
        self.metrics_handler = metrics_handler
        self.event_handler = event_handler
        self.consumer = f"coordinator-observer-{uuid4().hex[:8]}"
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        for stream in (METRICS_STREAM, EVENTS_STREAM):
            _ensure_group(self.redis, stream, COORDINATOR_GROUP, "$")

    def _consume(self, stream: str, decode, handler) -> None:
        response = self.redis.xreadgroup(
            COORDINATOR_GROUP, self.consumer, {stream: ">"}, count=32, block=100
        )
        for _, entries in response or []:
            for message_id, fields in entries:
                try:
                    handler(decode(_read_payload(fields)))
                finally:
                    self.redis.xack(stream, COORDINATOR_GROUP, message_id)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._consume(METRICS_STREAM, _metric_from_dict, self.metrics_handler)
                self._consume(EVENTS_STREAM, _event_from_dict, self.event_handler)
            except Exception:
                logger.exception("Redis telemetry consumer failed")
                self._stop.wait(0.25)

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="pycoremark-redis-observer", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

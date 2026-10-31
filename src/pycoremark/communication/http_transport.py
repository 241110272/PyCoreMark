"""Small standard-library HTTP transport for separate worker processes."""

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import random
from queue import Empty, Queue
import threading
import time
import hashlib
import hmac
from uuid import uuid4
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen

from pycoremark.models.metrics import MetricSample
from pycoremark.models.event import ClusterEvent, ClusterEventType
from pycoremark.models.node import (
    GPUResource,
    Heartbeat,
    NetworkInterface,
    NodeInfo,
    NodeResources,
    NodeStatus,
)
from pycoremark.models.task import Task, TaskAttempt, TaskResult, TaskStatus


def _json_default(value):
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "value"):
        return value.value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(f"Cannot encode {type(value).__name__}")


def _datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _node_from_dict(data: dict) -> NodeInfo:
    resources = dict(data["resources"])
    for field_name in ("total_cores", "available_cores", "total_memory_mb", "available_memory_mb"):
        if resources.get(field_name) is not None:
            resources[field_name] = int(resources[field_name])
    resources["gpus"] = [
        GPUResource(**{
            **gpu,
            "total_memory_mb": int(gpu["total_memory_mb"]),
            "available_memory_mb": int(gpu["available_memory_mb"]),
        })
        for gpu in resources.get("gpus", [])
    ]
    interfaces = [NetworkInterface(**item) for item in data.get("network_interfaces", [])]
    return NodeInfo(
        **{
            **data,
            "port": int(data["port"]),
            "resources": NodeResources(**resources),
            "network_interfaces": interfaces,
            "status": NodeStatus(data["status"]),
            "registered_at": _datetime(data["registered_at"]),
            "last_heartbeat_at": _datetime(data["last_heartbeat_at"]),
        }
    )


def _heartbeat_from_dict(data: dict) -> Heartbeat:
    return Heartbeat(
        **{
            **data,
            "sequence_number": int(data["sequence_number"]),
            "active_tasks": int(data["active_tasks"]),
            "timestamp": _datetime(data["timestamp"]),
            "status": NodeStatus(data["status"]),
        }
    )


def _metric_from_dict(data: dict) -> MetricSample:
    return MetricSample(**{
        **data,
        "active_tasks": int(data["active_tasks"]),
        "network_bytes_sent": int(data.get("network_bytes_sent", 0)),
        "network_bytes_received": int(data.get("network_bytes_received", 0)),
        "timestamp": _datetime(data["timestamp"]),
    })


def _task_from_dict(data: dict) -> Task:
    task = dict(data)
    for field_name in (
        "required_cores", "required_memory_mb", "priority", "max_retries", "retry_count", "logical_clock"
    ):
        if field_name in task:
            task[field_name] = int(task[field_name])
    task["status"] = TaskStatus(task["status"])
    task["payload"] = _coerce_integer_payload(task.get("payload", {}))
    task["created_at"] = _datetime(task["created_at"])
    task["queued_at"] = _datetime(task["queued_at"]) if task.get("queued_at") else None
    task["attempts"] = [TaskAttempt(**{
        **attempt,
        "attempt_number": int(attempt["attempt_number"]),
        "started_at": _datetime(attempt["started_at"]),
        "finished_at": _datetime(attempt["finished_at"]) if attempt.get("finished_at") else None,
    }) for attempt in task.get("attempts", [])]
    return Task(**task)


def _coerce_integer_payload(value):
    integer_fields = {
        "start", "stop", "start_index", "end_index", "partition_id",
        "hash_rounds", "record_count", "processed_records",
    }
    if isinstance(value, dict):
        converted = {}
        for key, item in value.items():
            if key in integer_fields and isinstance(item, float) and item.is_integer():
                converted[key] = int(item)
            else:
                converted[key] = _coerce_integer_payload(item)
        return converted
    if isinstance(value, list):
        return [_coerce_integer_payload(item) for item in value]
    return value


def _result_from_dict(data: dict) -> TaskResult:
    result = dict(data)
    result["attempt_number"] = int(result.get("attempt_number", 1))
    result["status"] = TaskStatus(result["status"])
    result["completed_at"] = _datetime(result["completed_at"])
    result["started_at"] = _datetime(result["started_at"]) if result.get("started_at") else None
    return TaskResult(**result)


def _event_from_dict(data: dict) -> ClusterEvent:
    event = dict(data)
    event["event_type"] = ClusterEventType(event["event_type"])
    event["timestamp"] = _datetime(event["timestamp"])
    event["logical_clock"] = int(event.get("logical_clock", 0))
    return ClusterEvent(**event)


def _json_bytes(value) -> bytes:
    return json.dumps(asdict(value) if hasattr(value, "__dataclass_fields__") else value,
                      default=_json_default).encode("utf-8")


class HTTPMessageBroker:
    """Worker-side broker adapter that speaks HTTP/JSON to the coordinator."""

    def __init__(
        self,
        coordinator_url: str,
        poll_timeout: float = 0.5,
        auth_token: str | None = None,
        simulated_latency_ms: float = 0.0,
        simulated_failure_rate: float = 0.0,
    ) -> None:
        self.coordinator_url = coordinator_url.rstrip("/")
        self.poll_timeout = poll_timeout
        self.auth_token = auth_token
        self._registered_node = None
        self.simulated_latency_ms = max(0.0, simulated_latency_ms)
        self.simulated_failure_rate = min(1.0, max(0.0, simulated_failure_rate))

    def _request(self, method: str, path: str, body=None):
        if self.simulated_latency_ms:
            time.sleep(self.simulated_latency_ms / 1000.0)
        if random.random() < self.simulated_failure_rate:
            raise ConnectionError("Injected network message loss")
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        request = Request(
            self.coordinator_url + path,
            data=_json_bytes(body) if body is not None else None,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=max(2.0, self.poll_timeout + 1)) as response:
                payload = response.read()
                return json.loads(payload) if payload else None
        except HTTPError as exc:
            if exc.code == 401:
                raise PermissionError("Coordinator authentication failed") from exc
            if exc.code in (400, 409):
                detail = exc.read().decode("utf-8", errors="replace")
                raise ValueError(f"Coordinator rejected request: {detail}") from exc
            raise ConnectionError(f"Coordinator request failed: {exc}") from exc
        except (URLError, TimeoutError) as exc:
            raise ConnectionError(f"Coordinator request failed: {exc}") from exc

    def publish(self, topic: str, message) -> None:
        if topic == "cluster.heartbeats":
            try:
                self._request("POST", "/api/heartbeat", message)
            except ValueError:
                if self._registered_node is None:
                    raise
                self.register_node(self._registered_node)
                self._request("POST", "/api/heartbeat", message)
        elif topic == "cluster.metrics":
            self._request("POST", "/api/metrics", message)
        elif topic == "cluster.task_results":
            self._request("POST", "/api/results", message)
        elif topic == "cluster.events":
            self._request("POST", "/api/events", message)
        else:
            raise ValueError(f"Unsupported remote broker topic: {topic}")

    def subscribe(self, topic: str):
        return None

    def poll(self, topic: str, timeout: float | None = None):
        parts = topic.split(".")
        if len(parts) != 3 or parts[0] != "worker" or parts[2] != "tasks":
            return None
        worker_id = quote(parts[1], safe="")
        wait = self.poll_timeout if timeout is None else max(0.0, min(timeout, 25.0))
        response = self._request(
            "GET", f"/api/tasks/next?worker_id={worker_id}&timeout={wait:.3f}"
        )
        return _task_from_dict(response["task"]) if response and response.get("task") else None

    def register_node(self, node: NodeInfo) -> None:
        self._registered_node = node
        challenge = None
        response = None
        if self.auth_token is not None:
            challenge_payload = self._request(
                "POST", "/api/challenge", {"node_id": node.node_id}
            )
            challenge = challenge_payload["challenge"]
            message = f"{node.node_id}:{challenge}".encode("utf-8")
            response = hmac.new(
                self.auth_token.encode("utf-8"), message, hashlib.sha256
            ).hexdigest()
        self._request("POST", "/api/register", {
            "node": node,
            "auth_token": self.auth_token,
            "handshake_response": response,
        })

    def deregister_node(self, node_id: str) -> None:
        self._request("POST", "/api/deregister", {"node_id": node_id})


class HTTPClusterServer:
    """Coordinator HTTP API for registration, monitoring, queues, and results."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        registration_service,
        cluster_manager,
        metrics_handler,
        topology_service,
        metrics_provider=None,
        metrics_history_provider=None,
        history_provider=None,
        experiment_handler=None,
        auth_token: str | None = None,
        event_handler=None,
        events_provider=None,
    ) -> None:
        self._registration = registration_service
        self._cluster_manager = cluster_manager
        self._metrics_handler = metrics_handler
        self._topology = topology_service
        self._metrics_provider = metrics_provider or (lambda: {})
        self._metrics_history_provider = metrics_history_provider or (lambda: [])
        self._history_provider = history_provider or (lambda: [])
        self._experiment_handler = experiment_handler
        self._auth_token = auth_token
        self._event_handler = event_handler or (lambda event: None)
        self._events_provider = events_provider or (lambda: [])
        self._tasks: dict[str, Queue] = {}
        self._assigned_tasks: dict[str, str] = {}
        self._results: Queue = Queue()
        self._lock = threading.RLock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                return

            def _send(self, status: int, data=None):
                payload = json.dumps(data, default=_json_default).encode("utf-8") if data is not None else b""
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                if payload:
                    self.wfile.write(payload)

            def _body(self):
                size = int(self.headers.get("Content-Length", "0"))
                return json.loads(self.rfile.read(size).decode("utf-8"))

            def _authorized(self):
                if owner._auth_token is None:
                    return True
                supplied = self.headers.get("Authorization", "")
                expected = f"Bearer {owner._auth_token}"
                if not hmac.compare_digest(supplied, expected):
                    self._send(401, {"error": "authentication required"})
                    return False
                return True

            def do_GET(self):
                parsed = urlparse(self.path)
                if parsed.path == "/health":
                    self._send(200, {"status": "ok"})
                    return
                if not self._authorized():
                    return
                if parsed.path == "/api/status":
                    self._send(200, owner._topology.get_topology_snapshot())
                    return
                if parsed.path == "/api/metrics":
                    self._send(200, owner._metrics_provider())
                    return
                if parsed.path == "/api/metrics/history":
                    self._send(200, owner._metrics_history_provider())
                    return
                if parsed.path == "/api/history":
                    self._send(200, owner._history_provider())
                    return
                if parsed.path == "/api/events":
                    self._send(200, owner._events_provider())
                    return
                if parsed.path == "/api/tasks/next":
                    values = parse_qs(parsed.query)
                    worker_id = values.get("worker_id", [""])[0]
                    timeout = min(25.0, max(0.0, float(values.get("timeout", ["0.5"])[0])))
                    try:
                        task = owner._queue_for(worker_id).get(timeout=timeout)
                    except Empty:
                        self._send(200, {"task": None})
                    else:
                        self._send(200, {"task": json.loads(_json_bytes(task))})
                    return
                self._send(404, {"error": "not found"})

            def do_POST(self):
                try:
                    if not self._authorized():
                        return
                    payload = self._body()
                    if self.path == "/api/register":
                        owner._registration.register_worker(
                            _node_from_dict(payload["node"]),
                            auth_token=payload.get("auth_token"),
                            handshake_response=payload.get("handshake_response"),
                        )
                    elif self.path == "/api/deregister":
                        node = owner._cluster_manager.registry.get(payload["node_id"])
                        if node is not None:
                            node.active_tasks = 0
                            owner._cluster_manager.registry.update_status(
                                node.node_id, NodeStatus.OFFLINE
                            )
                            owner._cluster_manager.heartbeat_service.forget_node(node.node_id)
                    elif self.path == "/api/challenge":
                        self._send(200, {
                            "challenge": owner._registration.issue_handshake_challenge(
                                payload["node_id"]
                            )
                        })
                        return
                    elif self.path == "/api/heartbeat":
                        if not owner._cluster_manager.process_heartbeat(_heartbeat_from_dict(payload)):
                            self._send(409, {"error": "heartbeat rejected"})
                            return
                    elif self.path == "/api/metrics":
                        owner._metrics_handler(_metric_from_dict(payload))
                    elif self.path == "/api/results":
                        result = _result_from_dict(payload)
                        with owner._lock:
                            assigned_worker = owner._assigned_tasks.get(result.task_id)
                            if assigned_worker == result.worker_id:
                                owner._assigned_tasks.pop(result.task_id, None)
                        if assigned_worker != result.worker_id:
                            self._send(409, {"error": "result task/worker assignment is unknown"})
                            return
                        owner._results.put(result)
                    elif self.path == "/api/events":
                        owner._event_handler(_event_from_dict(payload))
                    elif self.path == "/api/experiments/run":
                        if owner._experiment_handler is None:
                            self._send(503, {"error": "experiment control is disabled"})
                            return
                        result = owner._experiment_handler(payload)
                        self._send(200, result)
                        return
                    elif self.path == "/api/tasks":
                        task = _task_from_dict(payload["task"])
                        worker_id = str(payload["worker_id"])
                        owner.enqueue_task(worker_id, task)
                    else:
                        self._send(404, {"error": "not found"})
                        return
                    self._send(200, {"ok": True})
                except (KeyError, TypeError, ValueError, PermissionError) as exc:
                    self._send(400, {"error": str(exc)})
                except Exception as exc:
                    self._send(500, {"error": str(exc)})

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._server.daemon_threads = True
        self._thread: threading.Thread | None = None

    @property
    def address(self) -> tuple[str, int]:
        return self._server.server_address

    @property
    def results(self) -> Queue:
        return self._results

    def _queue_for(self, worker_id: str) -> Queue:
        if not worker_id:
            raise ValueError("worker_id must not be empty")
        with self._lock:
            return self._tasks.setdefault(worker_id, Queue())

    def enqueue_task(self, worker_id: str, task: Task) -> None:
        with self._lock:
            if task.task_id in self._assigned_tasks:
                raise ValueError(f"task {task.task_id!r} has already been dispatched")
            self._assigned_tasks[task.task_id] = worker_id
        self._queue_for(worker_id).put(task)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="pycoremark-http-coordinator",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            self._server.shutdown()
        self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None


class HTTPTaskDispatcher:
    """Coordinator-side HTTP task dispatcher with a result receive queue."""

    def __init__(
        self,
        coordinator_url: str,
        result_queue: Queue,
        auth_token: str | None = None,
        clock=None,
    ) -> None:
        self.coordinator_url = coordinator_url.rstrip("/")
        self._results = result_queue
        self.auth_token = auth_token
        if clock is None:
            from pycoremark.communication.clock import LamportClock
            clock = LamportClock()
        self._event_clock = clock

    def publish_event(self, event_type: ClusterEventType, payload: dict) -> int:
        event = ClusterEvent(
            event_id=uuid4().hex,
            event_type=event_type,
            source_node_id="coordinator",
            payload=payload,
            logical_clock=self._event_clock.tick(),
        )
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        request = Request(
            self.coordinator_url + "/api/events",
            data=_json_bytes(event),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=5) as response:
                response.read()
        except (HTTPError, URLError, TimeoutError) as exc:
            raise ConnectionError(f"Unable to publish event: {exc}") from exc
        return event.logical_clock

    def dispatch(self, task: Task, target_node_id: str) -> None:
        task.assigned_node_id = target_node_id
        task.status = TaskStatus.SCHEDULED
        task.queued_at = datetime.now(timezone.utc)
        task.logical_clock = self.publish_event(
            ClusterEventType.TASK_SCHEDULED,
            {"task_id": task.task_id, "worker_id": target_node_id},
        )
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        request = Request(
            self.coordinator_url + "/api/tasks",
            data=_json_bytes({"worker_id": target_node_id, "task": task}),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=5) as response:
                response.read()
        except (HTTPError, URLError, TimeoutError) as exc:
            raise ConnectionError(f"Unable to dispatch task to {target_node_id}: {exc}") from exc

    def receive_result(self, timeout: float = 0.1):
        try:
            return self._results.get(timeout=max(0.0, timeout))
        except Empty:
            return None


class HTTPClusterClient:
    """Dashboard/CLI client for a remote coordinator API."""

    def __init__(self, coordinator_url: str, timeout: float = 120.0, auth_token: str | None = None) -> None:
        self.coordinator_url = coordinator_url.rstrip("/")
        self.timeout = timeout
        self.auth_token = auth_token

    def request(self, method: str, path: str, body=None, timeout: float | None = None):
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        request = Request(
            self.coordinator_url + path,
            data=_json_bytes(body) if body is not None else None,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=self.timeout if timeout is None else timeout) as response:
                payload = response.read()
                return json.loads(payload) if payload else None
        except (HTTPError, URLError, TimeoutError) as exc:
            raise ConnectionError(f"Coordinator API request failed: {exc}") from exc

    def status(self):
        return self.request("GET", "/api/status", timeout=1.0)

    def metrics(self):
        values = self.request("GET", "/api/metrics", timeout=1.0) or {}
        return {node_id: _metric_from_dict(sample) for node_id, sample in values.items()}

    def metrics_history(self):
        return [_metric_from_dict(sample) for sample in self.request(
            "GET", "/api/metrics/history", timeout=1.0
        ) or []]

    def history(self):
        return [self._result_from_dict(value) for value in self.request("GET", "/api/history", timeout=1.0) or []]

    def events(self):
        return self.request("GET", "/api/events", timeout=1.0) or []

    def run_experiment(self, *, workload: str, nodes: int, tasks: int = 8,
                       complexity: int = 25000, records: int = 1000,
                       scheduler_type: str = "DYNAMIC", repeat_count: int = 1,
                       dataset_path: str | None = None, output_dir: str | None = None,
                       result_url: str | None = None, network_timeout: float = 10.0):
        from pycoremark.analysis.baseline import run_sequential_baseline
        from pycoremark.models.workload import WorkloadConfig, WorkloadType
        from pycoremark.workloads.cpu import CPUWorkload
        from pycoremark.workloads.data import PartitionedDataWorkload
        from pycoremark.workloads.mixed import MixedWorkload
        if workload == "cpu":
            workload_config = {
                "name": "cpu_benchmark", "workload_type": "CPU_BOUND",
                "total_tasks": tasks, "task_complexity": complexity,
            }
            experiment_name = "CPU prime benchmark"
            workload_type = "cpu_benchmark"
            baseline_workload = CPUWorkload(WorkloadConfig(
                name=workload_type,
                workload_type=WorkloadType.CPU_BOUND,
                total_tasks=tasks,
                task_complexity=complexity,
            ))
        elif workload == "data":
            partitions = max(1, min(16, nodes * 2))
            workload_config = {
                "name": "partitioned_data", "workload_type": "DATA_PARTITIONED",
                "total_tasks": partitions, "dataset_size_records": records,
                "partitions": partitions,
            }
            experiment_name = "Partitioned word-count benchmark"
            workload_type = "partitioned_data"
            baseline_workload = PartitionedDataWorkload(WorkloadConfig(
                name=workload_type,
                workload_type=WorkloadType.DATA_PARTITIONED,
                total_tasks=partitions,
                dataset_size_records=records,
                partitions=partitions,
            ))
        else:
            if not dataset_path or not output_dir or not result_url:
                raise ValueError(
                    "mixed workload requires dataset_path, output_dir, and result_url"
                )
            params = {
                "dataset_path": dataset_path,
                "output_dir": output_dir,
                "result_url": result_url,
                "network_timeout_seconds": network_timeout,
            }
            workload_config = {
                "name": "mixed", "workload_type": "MIXED",
                "total_tasks": tasks, "task_complexity": complexity,
                "partitions": tasks, "parameters": params,
            }
            experiment_name = "Mixed file/compute/network benchmark"
            workload_type = "mixed"
            baseline_workload = MixedWorkload(WorkloadConfig(
                name=workload_type,
                workload_type=WorkloadType.MIXED,
                total_tasks=tasks,
                task_complexity=complexity,
                partitions=tasks,
                parameters=params,
            ))
        payload = {
            "experiment_id": f"{workload_type}-{threading.get_ident()}-{int(datetime.now().timestamp() * 1000)}",
            "name": experiment_name,
            "workload": workload_config,
            "target_nodes": nodes,
            "scheduler_type": scheduler_type,
            "repeat_count": repeat_count,
            "parameters": {},
        }
        baseline = run_sequential_baseline(baseline_workload)
        payload["parameters"] = {
            "baseline_single_node_duration": baseline.duration_seconds,
            "baseline_checksum": baseline.checksum,
        }
        result = self.request("POST", "/api/experiments/run", payload)
        return self._result_from_dict(result)

    @staticmethod
    def _result_from_dict(value):
        from pycoremark.models.experiment import ExperimentResult, ExperimentStatus
        data = dict(value)
        data["status"] = ExperimentStatus(data["status"])
        data["start_time"] = _datetime(data["start_time"])
        if data.get("end_time"):
            data["end_time"] = _datetime(data["end_time"])
        return ExperimentResult(**data)

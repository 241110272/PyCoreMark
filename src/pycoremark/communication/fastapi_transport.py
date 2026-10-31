"""FastAPI coordinator API used by remote workers and dashboards."""

from dataclasses import asdict
from contextlib import asynccontextmanager
from queue import Empty, Queue
import hmac
import threading
import time
from typing import Callable

from fastapi import Depends, FastAPI, Header, HTTPException, Query
import uvicorn

from pycoremark.communication.http_transport import (
    _event_from_dict,
    _heartbeat_from_dict,
    _metric_from_dict,
    _node_from_dict,
    _result_from_dict,
    _task_from_dict,
)
from pycoremark.models.node import NodeStatus


class FastAPIClusterServer:
    """FastAPI/uvicorn implementation of the coordinator's REST surface."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        registration_service,
        cluster_manager,
        metrics_handler: Callable,
        topology_service,
        metrics_provider=None,
        metrics_history_provider=None,
        history_provider=None,
        experiment_handler=None,
        auth_token: str | None = None,
        event_handler=None,
        events_provider=None,
        node_event_handler=None,
    ) -> None:
        self.host, self.port = host, port
        self._registration = registration_service
        self._cluster_manager = cluster_manager
        self._metrics_handler = metrics_handler
        self._topology = topology_service
        self._metrics_provider = metrics_provider or (lambda: {})
        self._metrics_history_provider = metrics_history_provider or (lambda: [])
        self._history_provider = history_provider or (lambda: [])
        self._experiment_handler = experiment_handler
        self._event_handler = event_handler or (lambda event: None)
        self._events_provider = events_provider or (lambda: [])
        self._node_event_handler = node_event_handler or (lambda event_type, node_id: None)
        self._auth_token = auth_token
        self._tasks: dict[str, Queue] = {}
        self._assigned_tasks: dict[str, str] = {}
        self._results: Queue = Queue()
        self._lock = threading.RLock()
        self._started = threading.Event()

        @asynccontextmanager
        async def lifespan(app):
            self._started.set()
            yield
            self._started.clear()

        self.app = FastAPI(
            title="PyCoreMark Coordinator", version="0.1.0", lifespan=lifespan
        )
        self._build_routes()
        self._server = uvicorn.Server(uvicorn.Config(
            self.app, host=host, port=port, log_level="warning", access_log=False
        ))
        self._thread: threading.Thread | None = None

    def _authorize(self, authorization: str | None = Header(default=None)) -> None:
        if self._auth_token is None:
            return
        if not hmac.compare_digest(authorization or "", f"Bearer {self._auth_token}"):
            raise HTTPException(status_code=401, detail="authentication required")

    def _build_routes(self) -> None:
        auth = [Depends(self._authorize)]

        @self.app.get("/health")
        def health() -> dict:
            return {"status": "ok", "service": "pycoremark-coordinator"}

        @self.app.get("/api/status", dependencies=auth)
        def status() -> dict:
            return self._topology.get_topology_snapshot()

        @self.app.get("/api/metrics", dependencies=auth)
        def metrics() -> dict:
            return self._metrics_provider()

        @self.app.get("/api/metrics/history", dependencies=auth)
        def metrics_history() -> list:
            return self._metrics_history_provider()

        @self.app.get("/api/history", dependencies=auth)
        def history() -> list:
            return self._history_provider()

        @self.app.get("/api/events", dependencies=auth)
        def events() -> list:
            return self._events_provider()

        @self.app.post("/api/challenge", dependencies=auth)
        def challenge(payload: dict) -> dict:
            try:
                return {"challenge": self._registration.issue_handshake_challenge(payload["node_id"])}
            except (KeyError, ValueError, RuntimeError) as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        @self.app.post("/api/register", dependencies=auth)
        def register(payload: dict) -> dict:
            try:
                registered = self._registration.register_worker(
                    _node_from_dict(payload["node"]),
                    auth_token=payload.get("auth_token"),
                    handshake_response=payload.get("handshake_response"),
                )
                self._node_event_handler("NODE_REGISTERED", payload["node"]["node_id"])
                return {"registered": registered}
            except PermissionError as exc:
                raise HTTPException(status_code=401, detail=str(exc)) from exc
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        @self.app.post("/api/deregister", dependencies=auth)
        def deregister(payload: dict) -> dict:
            node_id = str(payload.get("node_id", ""))
            node = self._cluster_manager.registry.get(node_id)
            if node is not None:
                node.active_tasks = 0
                self._cluster_manager.registry.update_status(node_id, NodeStatus.OFFLINE)
                self._cluster_manager.heartbeat_service.forget_node(node_id)
                self._node_event_handler("NODE_DEREGISTERED", node_id)
            return {"deregistered": node is not None}

        @self.app.post("/api/heartbeat", dependencies=auth)
        def heartbeat(payload: dict) -> dict:
            try:
                accepted = self._cluster_manager.process_heartbeat(_heartbeat_from_dict(payload))
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            if not accepted:
                raise HTTPException(status_code=409, detail="heartbeat rejected")
            return {"ok": True}

        @self.app.post("/api/metrics", dependencies=auth)
        def publish_metrics(payload: dict) -> dict:
            try:
                self._metrics_handler(_metric_from_dict(payload))
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return {"ok": True}

        @self.app.post("/api/events", dependencies=auth)
        def publish_event(payload: dict) -> dict:
            try:
                self._event_handler(_event_from_dict(payload))
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return {"ok": True}

        @self.app.post("/api/tasks", dependencies=auth)
        def dispatch_task(payload: dict) -> dict:
            try:
                task = _task_from_dict(payload["task"])
                self.enqueue_task(str(payload["worker_id"]), task)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return {"ok": True}

        @self.app.get("/api/tasks/next", dependencies=auth)
        def next_task(
            worker_id: str,
            timeout: float = Query(default=0.5, ge=0.0, le=25.0),
        ) -> dict:
            try:
                task = self._queue_for(worker_id).get(timeout=timeout)
            except Empty:
                return {"task": None}
            return {"task": asdict(task)}

        @self.app.post("/api/results", dependencies=auth)
        def publish_result(payload: dict) -> dict:
            try:
                result = _result_from_dict(payload)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            try:
                self.submit_result(result)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            return {"ok": True}

        @self.app.post("/api/experiments/run", dependencies=auth)
        def run_experiment(payload: dict) -> dict:
            if self._experiment_handler is None:
                raise HTTPException(status_code=503, detail="experiment control is disabled")
            try:
                return self._experiment_handler(payload)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    @property
    def address(self) -> tuple[str, int]:
        return (self.host, self.port)

    @property
    def results(self) -> Queue:
        return self._results

    def _queue_for(self, worker_id: str) -> Queue:
        if not worker_id:
            raise ValueError("worker_id must not be empty")
        with self._lock:
            return self._tasks.setdefault(worker_id, Queue())

    def enqueue_task(self, worker_id: str, task) -> None:
        with self._lock:
            if task.task_id in self._assigned_tasks and task.retry_count <= 0:
                raise ValueError(f"task {task.task_id!r} has already been dispatched")
            self._assigned_tasks[task.task_id] = worker_id
        self._queue_for(worker_id).put(task)

    def get_task(self, worker_id: str, timeout: float = 0.5):
        try:
            return self._queue_for(worker_id).get(timeout=max(0.0, timeout))
        except Empty:
            return None

    def submit_result(self, result) -> None:
        with self._lock:
            assigned_worker = self._assigned_tasks.get(result.task_id)
            if assigned_worker == result.worker_id:
                self._assigned_tasks.pop(result.task_id, None)
        if assigned_worker != result.worker_id:
            raise ValueError("unknown task/worker assignment")
        self._results.put(result)

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a queued task; running tasks require worker-side cancellation."""
        with self._lock:
            for queue in self._tasks.values():
                with queue.mutex:
                    for task in tuple(queue.queue):
                        if task.task_id == task_id:
                            queue.queue.remove(task)
                            queue.unfinished_tasks = max(0, queue.unfinished_tasks - 1)
                            queue.not_full.notify()
                            self._assigned_tasks.pop(task_id, None)
                            return True
        return False

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._server.should_exit = False
        self._thread = threading.Thread(
            target=self._server.run, name="pycoremark-fastapi", daemon=True
        )
        self._thread.start()
        if not self._started.wait(timeout=10):
            raise RuntimeError("FastAPI coordinator failed to start")

    def stop(self) -> None:
        self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        self._started.clear()

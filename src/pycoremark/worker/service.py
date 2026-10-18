
"""Worker node service runtime with concurrent task execution."""

from concurrent.futures import Future, ThreadPoolExecutor
from queue import Empty, Queue
from threading import Event, Lock, Thread
import time

from pycoremark.common.config import ClusterConfig
from pycoremark.common.logging import get_logger
from pycoremark.communication.broker import MessageBroker
from pycoremark.models.node import NodeInfo, NodeResources, NodeStatus
from pycoremark.worker.agents import WorkerHeartbeatAgent, WorkerMetricsAgent
from pycoremark.worker.executor import TaskExecutor

logger = get_logger("worker.service")


class WorkerService:
    """Virtual cluster worker node daemon."""

    def __init__(
        self,
        config: ClusterConfig,
        executor: TaskExecutor,
        heartbeat_agent: WorkerHeartbeatAgent,
        metrics_agent: WorkerMetricsAgent,
        broker: MessageBroker,
    ) -> None:
        self.config = config
        self.executor = executor
        self.heartbeat_agent = heartbeat_agent
        self.metrics_agent = metrics_agent
        self.broker = broker

        self._is_running = False
        self._stop_event = Event()
        self._poll_thread: Thread | None = None
        self._pool: ThreadPoolExecutor | None = None
        self._pending: set[Future] = set()
        self._pending_lock = Lock()

        self._task_queue: Queue = Queue()
        self._topic = f"worker.{self.config.worker_id}.tasks"

    def build_node_info(self) -> NodeInfo:
        """Build the descriptor used to register this worker."""
        return NodeInfo(
            node_id=self.config.worker_id,
            hostname="localhost",
            port=0,
            resources=NodeResources(
                total_cores=self.config.worker_cores,
                available_cores=self.config.worker_cores,
                total_memory_mb=self.config.worker_memory_mb,
                available_memory_mb=self.config.worker_memory_mb,
            ),
            status=NodeStatus.READY,
        )

    def submit_task(self, task) -> Future:
        """Submit a task to the local thread pool."""
        if not self._is_running or self._pool is None:
            raise RuntimeError("WorkerService is not running")

        future = self._pool.submit(self.executor.execute, task)

        with self._pending_lock:
            self._pending.add(future)

        future.add_done_callback(self._on_task_done)
        return future

    def _on_task_done(self, future: Future) -> None:
        with self._pending_lock:
            self._pending.discard(future)

        try:
            result = future.result()
            logger.info(
                "Task finished: task_id=%s status=%s",
                getattr(result, "task_id", "unknown"),
                getattr(result, "status", "unknown"),
            )
            # Publish results only if the broker implements this operation.
            publish_result = getattr(self.broker, "publish", None)
            if callable(publish_result):
                publish_result("cluster.task_results", result)
        except Exception:
            logger.exception("Worker task future failed")

    def _poll_task(self):
        """Try to retrieve one task from the broker.

        Returns None when no supported polling API is available or no
        message is ready. Adjust this method to the project's broker API.
        """
        for method_name in ("poll", "get_message", "receive", "consume", "get"):
            method = getattr(self.broker, method_name, None)
            if not callable(method):
                continue

            try:
                message = method(self._topic)
            except (Empty, TimeoutError):
                return None
            except TypeError:
                continue
            except Exception:
                logger.exception("Broker polling failed using %s", method_name)
                return None

            if message is None:
                return None

            # Some brokers return (topic, payload) or an envelope with payload.
            if isinstance(message, tuple) and len(message) == 2:
                message = message[1]
            elif hasattr(message, "payload"):
                message = message.payload

            return message

        return None

    def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                task = self._task_queue.get_nowait()
            except Empty:
                task = self._poll_task()

            if task is None:
                self._stop_event.wait(0.05)
                continue

            try:
                self.submit_task(task)
            except Exception:
                logger.exception("Unable to submit worker task")

    def start(self) -> None:
        """Start task processing and background agents."""
        if self._is_running:
            return

        logger.info(
            "WorkerService '%s' starting with %d cores and %d MB RAM",
            self.config.worker_id,
            self.config.worker_cores,
            self.config.worker_memory_mb,
        )

        workers = max(1, int(self.config.worker_cores))
        self._pool = ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix=f"worker-{self.config.worker_id}",
        )
        self._stop_event.clear()
        self._is_running = True

        try:
            self.broker.subscribe(self._topic)
            self.heartbeat_agent.send_heartbeat()
            self.metrics_agent.send_metrics()

            self.heartbeat_agent.start()
            self.metrics_agent.start()

            self._poll_thread = Thread(
                target=self._poll_loop,
                name=f"worker-poller-{self.config.worker_id}",
                daemon=True,
            )
            self._poll_thread.start()
        except Exception:
            logger.exception("Failed to start WorkerService")
            self.stop()
            raise

    def stop(self) -> None:
        """Stop polling, stop agents, and wait for submitted tasks."""
        if not self._is_running and self._pool is None:
            return

        logger.info("WorkerService '%s' stopping", self.config.worker_id)
        self._is_running = False
        self._stop_event.set()

        self.heartbeat_agent.stop()
        self.metrics_agent.stop()

        if self._poll_thread is not None:
            self._poll_thread.join(timeout=5.0)
            self._poll_thread = None

        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=False)
            self._pool = None

        logger.info("WorkerService '%s' stopped", self.config.worker_id)

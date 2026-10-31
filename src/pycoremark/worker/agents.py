
"""Worker background agents for heartbeat emission and telemetry reporting."""

from datetime import datetime, timezone
from threading import Event, Lock, Thread
from typing import Callable

from pycoremark.communication.broker import MessageBroker
from pycoremark.common.logging import get_logger
from pycoremark.models.metrics import MetricSample
from pycoremark.models.node import Heartbeat, NodeStatus
from pycoremark.monitoring.collector import NodeMetricsCollector

logger = get_logger("worker.agents")


class _PeriodicAgent:
    """Base class for cancellable periodic background agents."""

    def __init__(self, interval_seconds: float) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")

        self.interval_seconds = interval_seconds
        self._stop_event = Event()
        self._thread: Thread | None = None
        self._lock = Lock()

    def _run_once(self) -> None:
        raise NotImplementedError

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_once()
            except Exception:
                logger.exception("%s periodic execution failed", type(self).__name__)

            self._stop_event.wait(self.interval_seconds)

    def start(self) -> None:
        """Start the periodic loop once."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return

            self._stop_event.clear()
            self._thread = Thread(
                target=self._run_loop,
                name=f"{type(self).__name__}-thread",
                daemon=True,
            )
            self._thread.start()

    def stop(self, timeout: float | None = 5.0) -> None:
        """Signal cancellation and wait briefly for the loop to finish."""
        self._stop_event.set()
        thread = self._thread

        if thread is not None and thread is not __import__("threading").current_thread():
            thread.join(timeout=timeout)

        with self._lock:
            if thread is not None and not thread.is_alive():
                self._thread = None


class WorkerHeartbeatAgent(_PeriodicAgent):
    """Periodically publishes worker heartbeat packets."""

    def __init__(
        self,
        node_id: str,
        broker: MessageBroker,
        interval_seconds: float = 2.0,
        active_tasks_provider: Callable[[], int] | None = None,
    ) -> None:
        super().__init__(interval_seconds)
        self.node_id = node_id
        self.broker = broker
        self.active_tasks_provider = active_tasks_provider or (lambda: 0)
        self._seq = 0
        self._seq_lock = Lock()

    def generate_heartbeat(self) -> Heartbeat:
        """Create a heartbeat with a monotonically increasing sequence."""
        with self._seq_lock:
            self._seq += 1
            sequence = self._seq

        active_tasks = self.active_tasks_provider()
        return Heartbeat(
            node_id=self.node_id,
            sequence_number=sequence,
            timestamp=datetime.now(timezone.utc),
            status=NodeStatus.BUSY if active_tasks else NodeStatus.READY,
            current_load=float(active_tasks),
            active_tasks=active_tasks,
        )

    def send_heartbeat(self) -> None:
        """Publish one heartbeat."""
        self.broker.publish("cluster.heartbeats", self.generate_heartbeat())

    def _run_once(self) -> None:
        self.send_heartbeat()


class WorkerMetricsAgent(_PeriodicAgent):
    """Periodically collects and publishes worker telemetry."""

    def __init__(
        self,
        collector: NodeMetricsCollector,
        broker: MessageBroker,
        interval_seconds: float = 1.0,
        active_tasks_provider: Callable[[], int] | None = None,
        additional_metrics_provider: Callable[[], dict] | None = None,
    ) -> None:
        super().__init__(interval_seconds)
        self.collector = collector
        self.broker = broker
        self.active_tasks_provider = active_tasks_provider or (lambda: 0)
        self.additional_metrics_provider = additional_metrics_provider or (lambda: {})

    def send_metrics(self) -> MetricSample:
        """Collect and publish one metrics sample."""
        sample = self.collector.collect()
        sample.active_tasks = self.active_tasks_provider()
        sample.additional_metrics.update(self.additional_metrics_provider())
        self.broker.publish("cluster.metrics", sample)
        return sample

    def _run_once(self) -> None:
        self.send_metrics()

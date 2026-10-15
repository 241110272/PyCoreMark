"""Worker background agents for heartbeat emission and telemetry reporting.

TODO(Member 7): Implement asynchronous threading loops with graceful cancellation signals.
"""

from datetime import datetime, timezone
from pycoremark.communication.broker import MessageBroker
from pycoremark.models.metrics import MetricSample
from pycoremark.models.node import Heartbeat, NodeStatus
from pycoremark.monitoring.collector import NodeMetricsCollector


class WorkerHeartbeatAgent:
    """Periodically produces Heartbeat packets to notify the coordinator of liveness."""

    def __init__(
        self,
        node_id: str,
        broker: MessageBroker,
        interval_seconds: float = 2.0,
    ) -> None:
        """Initialize heartbeat agent."""
        self.node_id = node_id
        self.broker = broker
        self.interval_seconds = interval_seconds
        self._seq = 0

    def generate_heartbeat(self) -> Heartbeat:
        """Create a new heartbeat payload."""
        self._seq += 1
        return Heartbeat(
            node_id=self.node_id,
            sequence_number=self._seq,
            timestamp=datetime.now(timezone.utc),
            status=NodeStatus.READY,
        )

    def send_heartbeat(self) -> None:
        """Send heartbeat packet to broker topic."""
        hb = self.generate_heartbeat()
        self.broker.publish("cluster.heartbeats", hb)


class WorkerMetricsAgent:
    """Periodically gathers node telemetry and publishes metrics."""

    def __init__(
        self,
        collector: NodeMetricsCollector,
        broker: MessageBroker,
        interval_seconds: float = 1.0,
    ) -> None:
        """Initialize metrics agent."""
        self.collector = collector
        self.broker = broker
        self.interval_seconds = interval_seconds

    def send_metrics(self) -> MetricSample:
        """Collect and publish one metric sample."""
        sample = self.collector.collect()
        self.broker.publish("cluster.metrics", sample)
        return sample

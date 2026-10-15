"""Metrics publisher broadcasting telemetry to message broker topics.

TODO(Member 6): Add periodic background flush daemon and batch buffering.
"""

from pycoremark.communication.broker import MessageBroker
from pycoremark.models.metrics import MetricSample


class MetricsPublisher:
    """Publishes node MetricSample records over the communication bus."""

    def __init__(self, broker: MessageBroker, topic: str = "cluster.metrics") -> None:
        """Initialize publisher with injected MessageBroker.

        Args:
            broker: MessageBroker backend instance.
            topic: Destination topic name.
        """
        self._broker = broker
        self._topic = topic

    def publish_sample(self, sample: MetricSample) -> None:
        """Publish a metric sample to the broker."""
        self._broker.publish(self._topic, sample)


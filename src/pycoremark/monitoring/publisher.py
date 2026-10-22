"""Metrics publisher broadcasting telemetry to message broker topics.

TODO(Member 6): Add periodic background flush daemon and batch buffering.
"""

from typing import List

from pycoremark.communication.broker import MessageBroker
from pycoremark.models.metrics import MetricSample


class MetricsPublisher:
    """Publishes node MetricSample records over the communication bus."""

    def __init__(self, broker: MessageBroker, topic: str = "cluster.metrics") -> None:
        self._broker = broker
        self._topic = topic

    def publish_sample(self, sample: MetricSample) -> None:
        """Publish a metric sample to the broker."""
        self._broker.publish(self._topic, sample)

    def publish_batch(self, samples: List[MetricSample]) -> None:
        """Publish beberapa sample sekaligus (placeholder untuk batching)."""
        for sample in samples:
            self.publish_sample(sample)
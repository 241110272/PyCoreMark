"""Event publisher and consumer abstractions built on top of MessageBroker.

TODO(Member 2): Add topic pattern matching (wildcard subscriptions) and schema enforcement.
"""

from typing import Callable, Optional
from pycoremark.communication.broker import MessageBroker
from pycoremark.models.event import ClusterEvent


class EventPublisher:
    """Dispatches strongly typed ClusterEvents to the message broker."""

    def __init__(self, broker: MessageBroker) -> None:
        """Initialize publisher with injected MessageBroker.

        Args:
            broker: MessageBroker backend instance.
        """
        self._broker = broker

    def publish(self, topic: str, event: ClusterEvent) -> None:
        """Publish a ClusterEvent instance to the given topic.

        Args:
            topic: Topic name string.
            event: ClusterEvent payload.
        """
        # TODO(Member 2): Serialize event and attach Lamport timestamp
        self._broker.publish(topic, event)


class EventConsumer:
    """Subscribes and polls ClusterEvents from message broker topics."""

    def __init__(self, broker: MessageBroker, topic: str) -> None:
        """Initialize consumer with injected broker and target topic.

        Args:
            broker: MessageBroker backend instance.
            topic: Topic name string to consume from.
        """
        self._broker = broker
        self._topic = topic
        self._broker.subscribe(topic)

    def poll(self, timeout: float = 1.0) -> Optional[ClusterEvent]:
        """Poll next event from topic queue.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            ClusterEvent or None if timeout expired.
        """
        # TODO(Member 2): Validate event schema deserialization
        msg = self._broker.poll(self._topic, timeout=timeout)
        if isinstance(msg, ClusterEvent):
            return msg
        return None


"""Message queue and broker abstractions for asynchronous cluster messaging.

TODO(Member 2): Add RabbitMQ / Redis Streams / Kafka backend adapters.
"""

from abc import ABC, abstractmethod
from collections import defaultdict
from queue import Empty, Queue
from typing import Any, Dict, List, Optional


class MessageBroker(ABC):
    """Abstract interface defining pub/sub and queue operations."""

    @abstractmethod
    def publish(self, topic: str, message: Any) -> None:
        """Publish a message to a named topic or queue."""
        raise NotImplementedError

    @abstractmethod
    def subscribe(self, topic: str) -> None:
        """Subscribe the current context to a named topic."""
        raise NotImplementedError

    @abstractmethod
    def poll(self, topic: str, timeout: float = 1.0) -> Optional[Any]:
        """Poll a message from a topic queue with timeout."""
        raise NotImplementedError


class InMemoryMessageBroker(MessageBroker):
    """Simple in-memory thread-safe queue message broker for testing and simulation."""

    def __init__(self) -> None:
        self._queues: Dict[str, Queue] = defaultdict(Queue)

    def publish(self, topic: str, message: Any) -> None:
        """Enqueue a message onto topic queue."""
        # TODO(Member 2): Implement message serialization and dead-letter routing on failure
        self._queues[topic].put(message)

    def subscribe(self, topic: str) -> None:
        """Ensure queue exists for topic."""
        # TODO(Member 2): Support multiple consumer groups / subscriber fanout
        _ = self._queues[topic]

    def poll(self, topic: str, timeout: float = 1.0) -> Optional[Any]:
        """Dequeue a message with timeout, returning None if queue is empty."""
        try:
            return self._queues[topic].get(timeout=timeout)
        except Empty:
            return None


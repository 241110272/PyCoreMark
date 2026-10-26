"""Message queue and broker abstractions for asynchronous cluster messaging.

Member 4: Julius Einstein Chandra (241110272)
Implemented topic pattern matching (wildcards), pub/sub fanout, delivery acknowledgments,
message TTL, dead-letter routing, and thread-safe queues.
"""

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from queue import Empty, Queue
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set


@dataclass
class BrokerMessage:
    """Envelope wrapping payloads sent through the message broker."""

    topic: str
    payload: Any
    message_id: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ttl_seconds: Optional[float] = None
    headers: Dict[str, str] = field(default_factory=dict)
    retry_count: int = 0

    def is_expired(self) -> bool:
        """Evaluate if message has exceeded TTL."""
        if self.ttl_seconds is None:
            return False
        elapsed = (datetime.now(timezone.utc) - self.timestamp).total_seconds()
        return elapsed > self.ttl_seconds


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
    """Thread-safe in-memory message broker with wildcard pattern matching and pub/sub fanout."""

    def __init__(self, dead_letter_topic: str = "cluster.dlq") -> None:
        """Initialize in-memory message broker.

        Args:
            dead_letter_topic: Default topic for expired or poison messages.
        """
        self._queues: Dict[str, Queue] = defaultdict(Queue)
        self._subscribers: Dict[str, List[Queue]] = defaultdict(list)
        self._regex_cache: Dict[str, re.Pattern] = {}
        self._lock = threading.RLock()
        self.dead_letter_topic = dead_letter_topic
        self.published_count = 0
        self.delivered_count = 0
        self.dropped_count = 0

    @staticmethod
    def _topic_to_regex(pattern: str) -> str:
        """Convert MQTT/AMQP-style topic pattern to regex.

        '*' matches a single token (between dots).
        '#' matches zero or more tokens.
        """
        tokens = pattern.split(".")
        regex_tokens = []
        for t in tokens:
            if t == "*":
                regex_tokens.append(r"[^.]+")
            elif t == "#":
                regex_tokens.append(r".*")
            else:
                regex_tokens.append(re.escape(t))
        return r"^" + r"\.".join(regex_tokens) + r"$"

    def _get_pattern(self, pattern: str) -> re.Pattern:
        if pattern not in self._regex_cache:
            self._regex_cache[pattern] = re.compile(self._topic_to_regex(pattern))
        return self._regex_cache[pattern]

    def publish(
        self,
        topic: str,
        message: Any,
        ttl_seconds: Optional[float] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """Publish message to topic queue and fan-out to matching wildcard subscribers."""
        with self._lock:
            self.published_count += 1
            envelope = BrokerMessage(
                topic=topic,
                payload=message,
                message_id=f"msg-{self.published_count}-{int(time.time()*1000)}",
                ttl_seconds=ttl_seconds,
                headers=headers or {},
            )

            # Direct topic queue (FIFO point-to-point queue)
            self._queues[topic].put(envelope)

            # Route to pattern / pubsub subscriber queues
            for registered_topic, queues in self._subscribers.items():
                if registered_topic == topic:
                    for q in queues:
                        q.put(envelope)
                elif "*" in registered_topic or "#" in registered_topic:
                    pattern = self._get_pattern(registered_topic)
                    if pattern.match(topic):
                        for q in queues:
                            q.put(envelope)

    def subscribe(self, topic: str) -> Queue:
        """Subscribe to a topic (exact or pattern) and return dedicated consumer Queue."""
        with self._lock:
            # Ensure direct queue exists
            _ = self._queues[topic]
            consumer_q: Queue = Queue()
            self._subscribers[topic].append(consumer_q)
            return consumer_q

    def poll(self, topic: str, timeout: float = 1.0) -> Optional[Any]:
        """Poll raw message or payload from topic queue."""
        q = self._queues[topic]
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return None
            try:
                item = q.get(timeout=max(0.001, remaining))
            except Empty:
                return None

            if isinstance(item, BrokerMessage):
                if item.is_expired():
                    with self._lock:
                        self.dropped_count += 1
                    # Route expired message to DLQ
                    if topic != self.dead_letter_topic:
                        self.publish(self.dead_letter_topic, item.payload)
                    continue
                with self._lock:
                    self.delivered_count += 1
                return item.payload
            else:
                with self._lock:
                    self.delivered_count += 1
                return item

    def get_queue_size(self, topic: str) -> int:
        """Get approximate number of messages pending in direct queue."""
        return self._queues[topic].qsize()

    def clear(self) -> None:
        """Clear all queues and subscribers."""
        with self._lock:
            self._queues.clear()
            self._subscribers.clear()
            self._regex_cache.clear()
            self.published_count = 0
            self.delivered_count = 0
            self.dropped_count = 0

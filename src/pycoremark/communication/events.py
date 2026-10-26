"""Event publisher and consumer abstractions built on top of MessageBroker.

Member 4: Julius Einstein Chandra (241110272)
Implemented event pub/sub streaming, logical clock timestamping, event type and source filtering,
and batch ingestion.
"""

import time
from typing import List, Optional, Set
from pycoremark.communication.broker import MessageBroker
from pycoremark.communication.clock import LamportClock
from pycoremark.models.event import ClusterEvent, ClusterEventType


class EventPublisher:
    """Dispatches strongly typed ClusterEvents to the message broker with logical clock stamps."""

    def __init__(
        self,
        broker: MessageBroker,
        clock: Optional[LamportClock] = None,
    ) -> None:
        """Initialize publisher with injected MessageBroker and optional LamportClock.

        Args:
            broker: MessageBroker backend instance.
            clock: Optional LamportClock instance to attach causal timestamps.
        """
        self._broker = broker
        self._clock = clock

    def publish(self, topic: str, event: ClusterEvent) -> None:
        """Publish a ClusterEvent instance to the given topic.

        Args:
            topic: Topic name string.
            event: ClusterEvent payload.
        """
        if self._clock:
            event.logical_clock = self._clock.tick()
        self._broker.publish(topic, event)

    def publish_batch(self, topic: str, events: List[ClusterEvent]) -> None:
        """Publish a list of events to the given topic."""
        for event in events:
            self.publish(topic, event)


class EventConsumer:
    """Subscribes and polls ClusterEvents with optional event type & source filtering."""

    def __init__(
        self,
        broker: MessageBroker,
        topic: str,
        filter_types: Optional[List[ClusterEventType]] = None,
        filter_sources: Optional[List[str]] = None,
        clock: Optional[LamportClock] = None,
    ) -> None:
        """Initialize consumer with injected broker and target topic.

        Args:
            broker: MessageBroker backend instance.
            topic: Topic name string to consume from.
            filter_types: Optional list of ClusterEventType values to accept.
            filter_sources: Optional list of source_node_id strings to accept.
            clock: Optional LamportClock to synchronize on event arrival.
        """
        self._broker = broker
        self._topic = topic
        self._filter_types: Optional[Set[ClusterEventType]] = set(filter_types) if filter_types else None
        self._filter_sources: Optional[Set[str]] = set(filter_sources) if filter_sources else None
        self._clock = clock
        self._broker.subscribe(topic)

    def poll(self, timeout: float = 1.0) -> Optional[ClusterEvent]:
        """Poll next matching event from topic queue.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            ClusterEvent or None if timeout expired without matching events.
        """
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return None

            msg = self._broker.poll(self._topic, timeout=max(0.001, remaining))
            if not isinstance(msg, ClusterEvent):
                continue

            # Apply type filters
            if self._filter_types and msg.event_type not in self._filter_types:
                continue

            # Apply source node filters
            if self._filter_sources and msg.source_node_id not in self._filter_sources:
                continue

            # Synchronize local logical clock
            if self._clock and msg.logical_clock > 0:
                self._clock.update(msg.logical_clock)

            return msg

    def poll_batch(self, max_items: int = 10, timeout: float = 1.0) -> List[ClusterEvent]:
        """Poll multiple events matching filters within the timeout."""
        events: List[ClusterEvent] = []
        deadline = time.time() + timeout
        while len(events) < max_items:
            rem = deadline - time.time()
            if rem <= 0:
                break
            ev = self.poll(timeout=max(0.001, rem))
            if ev:
                events.append(ev)
            else:
                break
        return events

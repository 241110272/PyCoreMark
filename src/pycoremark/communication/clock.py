"""Lamport and Vector logical clocks for causal event ordering in distributed cluster simulations.

Member 4: Julius Einstein Chandra (241110272)
Implemented thread-safe Lamport logical clock and Vector Clock with causal precedence
and concurrency comparison methods.
"""

from collections import defaultdict
import threading
from typing import Dict, Optional


class LamportClock:
    """Thread-safe Lamport logical clock implementation."""

    def __init__(self, initial_value: int = 0) -> None:
        """Initialize clock with starting counter value.

        Args:
            initial_value: Non-negative initial timestamp value.
        """
        self._value = initial_value
        self._lock = threading.Lock()

    def tick(self) -> int:
        """Increment logical clock on local event and return new timestamp.

        Returns:
            Updated integer timestamp.
        """
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_time: int) -> int:
        """Update clock upon receiving a remote event timestamp: max(local, remote) + 1.

        Args:
            received_time: Remote timestamp attached to incoming event.

        Returns:
            Updated synchronized integer timestamp.
        """
        with self._lock:
            self._value = max(self._value, received_time) + 1
            return self._value

    def get_time(self) -> int:
        """Retrieve current logical timestamp without incrementing.

        Returns:
            Current integer timestamp.
        """
        with self._lock:
            return self._value

    def reset(self) -> None:
        """Reset clock to zero."""
        with self._lock:
            self._value = 0


class VectorClock:
    """Thread-safe Vector Clock implementation for tracking distributed causal dependencies."""

    def __init__(self, local_node_id: str) -> None:
        """Initialize vector clock for a specific node.

        Args:
            local_node_id: Identifier of the local node owning this clock.
        """
        self.local_node_id = local_node_id
        self._clock: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def tick(self, node_id: Optional[str] = None) -> Dict[str, int]:
        """Increment local clock counter upon local event.

        Args:
            node_id: Node identifier to increment (defaults to local_node_id).

        Returns:
            Snapshot copy of the updated vector clock.
        """
        target = node_id or self.local_node_id
        with self._lock:
            self._clock[target] += 1
            return dict(self._clock)

    def update(self, remote_vector: Dict[str, int]) -> Dict[str, int]:
        """Update vector clock upon receiving remote vector: max(local[k], remote[k]) + tick(local).

        Args:
            remote_vector: Incoming vector clock from peer message.

        Returns:
            Updated synchronized vector clock snapshot.
        """
        with self._lock:
            for node, time_val in remote_vector.items():
                self._clock[node] = max(self._clock[node], time_val)
            self._clock[self.local_node_id] += 1
            return dict(self._clock)

    def get_vector(self) -> Dict[str, int]:
        """Return snapshot copy of the vector clock."""
        with self._lock:
            return dict(self._clock)

    @staticmethod
    def is_causally_before(v1: Dict[str, int], v2: Dict[str, int]) -> bool:
        """Check if vector v1 happened-before vector v2 (v1 <= v2 and v1 != v2)."""
        keys = set(v1.keys()).union(set(v2.keys()))
        strictly_less = False
        for k in keys:
            val1 = v1.get(k, 0)
            val2 = v2.get(k, 0)
            if val1 > val2:
                return False
            if val1 < val2:
                strictly_less = True
        return strictly_less

    @staticmethod
    def is_concurrent(v1: Dict[str, int], v2: Dict[str, int]) -> bool:
        """Check if vectors v1 and v2 are concurrent (neither happened-before the other)."""
        return (not VectorClock.is_causally_before(v1, v2)) and (not VectorClock.is_causally_before(v2, v1))

"""Lamport logical clock for causal event ordering in distributed cluster simulations.

TODO(Member 2): Add Vector Clock implementation for concurrent event causality tracking.
"""

import threading


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


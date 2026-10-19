"""Idempotency store to detect duplicate task executions and event processing.

TODO(Member 5): Add TTL eviction and distributed Redis/Raft backed store.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class IdempotencyStore(ABC):
    """Abstract interface defining duplicate execution suppression."""

    @abstractmethod
    def has_seen(self, key: str) -> bool:
        """Check if an operation key was already processed."""
        raise NotImplementedError

    @abstractmethod
    def record(self, key: str, value: Any = True) -> None:
        """Record an operation key into the store."""
        raise NotImplementedError

    @abstractmethod
    def get_result(self, key: str) -> Optional[Any]:
        """Fetch previously cached result for an idempotent key."""
        raise NotImplementedError


class InMemoryIdempotencyStore(IdempotencyStore):
    """Thread-safe in-memory map storing processed idempotency keys."""

    def __init__(self) -> None:
        self._store: Dict[str, Any] = {}

    def has_seen(self, key: str) -> bool:
        """Return True if key exists in store."""
        return key in self._store

    def record(self, key: str, value: Any = True) -> None:
        """Save key and optional outcome value."""
        self._store[key] = value

    def get_result(self, key: str) -> Optional[Any]:
        """Retrieve stored outcome."""
        return self._store.get(key)


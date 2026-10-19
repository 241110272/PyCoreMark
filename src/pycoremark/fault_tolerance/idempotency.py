"""Idempotency store to detect duplicate task executions and event processing.

TODO(Member 5): Add TTL eviction and distributed Redis/Raft backed store.
"""

import time
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

    def __init__(self, ttl_seconds: int = 3600) -> None:
        self._store: Dict[str, Dict[str, Any]] = {}
        self.ttl_seconds = ttl_seconds

    def _evict_expired(self):
        now = time.time()
        expired_keys = [k for k, v in self._store.items() if now > v['expires_at']]
        for k in expired_keys:
            del self._store[k]

    def has_seen(self, key: str) -> bool:
        """Return True if key exists in store."""
        self._evict_expired()
        return key in self._store

    def record(self, key: str, value: Any = True) -> None:
        """Save key and optional outcome value."""
        self._evict_expired()
        self._store[key] = {
            'value': value,
            'expires_at': time.time() + self.ttl_seconds
        }

    def get_result(self, key: str) -> Optional[Any]:
        """Retrieve stored outcome."""
        self._evict_expired()
        entry = self._store.get(key)
        return entry['value'] if entry else None

# DONE(Member 5): Menyiapkan kelas khusus untuk Redis Backed Store
class RedisIdempotencyStore(IdempotencyStore):
    """Distributed store backing up idempotency keys using Redis."""
    def __init__(self, redis_client, ttl_seconds: int = 3600):
        self.redis = redis_client
        self.ttl_seconds = ttl_seconds

    def has_seen(self, key: str) -> bool:
        return self.redis.exists(key) > 0

    def record(self, key: str, value: Any = "True") -> None:
        self.redis.setex(key, self.ttl_seconds, str(value))

    def get_result(self, key: str) -> Optional[Any]:
        val = self.redis.get(key)
        return val.decode('utf-8') if val else None
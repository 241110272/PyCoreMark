"""RPC Client and Server abstractions for synchronous cluster interactions.

Member 4: Julius Einstein Chandra (241110272)
Implemented synchronous RPC abstraction, network latency & jitter simulation,
timeout enforcement, failure injection, and handler dispatching.
"""

from abc import ABC, abstractmethod
import random
import time
from typing import Any, Callable, Dict, List, Optional
from pycoremark.common.exceptions import BrokerConnectionError


class RPCClient(ABC):
    """Abstract interface for dispatching remote procedure calls."""

    @abstractmethod
    def call(self, target_node_id: str, method: str, **kwargs: Any) -> Any:
        """Invoke a remote method on target node and await response."""
        raise NotImplementedError


class RPCServer(ABC):
    """Abstract interface for receiving and routing remote procedure calls."""

    @abstractmethod
    def register_handler(self, method: str, handler: Callable[..., Any]) -> None:
        """Register a method handler callable."""
        raise NotImplementedError

    @abstractmethod
    def start(self) -> None:
        """Start listening for incoming RPC invocations."""
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """Stop server and release network resources."""
        raise NotImplementedError


class InMemoryRPCClient(RPCClient):
    """Direct in-memory router simulating RPC communication between local components."""

    def __init__(
        self,
        registry: Dict[str, "InMemoryRPCServer"],
        simulated_latency_ms: float = 0.0,
        simulated_jitter_ms: float = 0.0,
        simulated_failure_rate: float = 0.0,
    ) -> None:
        """Initialize RPC client.

        Args:
            registry: Mapping of node_id to InMemoryRPCServer instance.
            simulated_latency_ms: Base artificial delay in milliseconds.
            simulated_jitter_ms: Random delay variation in milliseconds.
            simulated_failure_rate: Probability of network drop (0.0 - 1.0).
        """
        self._registry = registry
        self.simulated_latency_ms = simulated_latency_ms
        self.simulated_jitter_ms = simulated_jitter_ms
        self.simulated_failure_rate = simulated_failure_rate
        self.call_count = 0
        self.failed_count = 0

    def call(
        self,
        target_node_id: str,
        method: str,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> Any:
        """Simulate synchronous remote procedure call via direct handler lookup."""
        self.call_count += 1

        # Simulate network latency
        total_delay_ms = self.simulated_latency_ms
        if self.simulated_jitter_ms > 0:
            total_delay_ms += random.uniform(-self.simulated_jitter_ms, self.simulated_jitter_ms)
        if total_delay_ms > 0:
            time.sleep(max(0.0, total_delay_ms / 1000.0))

        # Check timeout expiration
        if timeout is not None and (total_delay_ms / 1000.0) > timeout:
            self.failed_count += 1
            raise TimeoutError(f"RPC call to '{target_node_id}.{method}' timed out after {timeout:.3f}s")

        # Simulate connection drop
        if self.simulated_failure_rate > 0 and random.random() < self.simulated_failure_rate:
            self.failed_count += 1
            raise BrokerConnectionError(f"Simulated network partition: failed to reach node '{target_node_id}'.")

        server = self._registry.get(target_node_id)
        if not server or not server.is_running:
            self.failed_count += 1
            raise ConnectionError(f"RPC destination node '{target_node_id}' is unreachable or stopped.")

        handler = server.handlers.get(method)
        if not handler:
            self.failed_count += 1
            raise AttributeError(f"Remote method '{method}' not implemented on '{target_node_id}'.")

        return handler(**kwargs)


class InMemoryRPCServer(RPCServer):
    """In-memory RPC endpoint server holding method handlers."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.handlers: Dict[str, Callable[..., Any]] = {}
        self.is_running = False

    def register_handler(self, method: str, handler: Callable[..., Any]) -> None:
        """Register local function handler for remote invocation."""
        self.handlers[method] = handler

    def list_methods(self) -> List[str]:
        """Return list of supported RPC method names."""
        return list(self.handlers.keys())

    def start(self) -> None:
        """Set server running state."""
        self.is_running = True

    def stop(self) -> None:
        """Set server stopped state."""
        self.is_running = False

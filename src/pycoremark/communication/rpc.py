"""RPC Client and Server abstractions for synchronous cluster interactions.

TODO(Member 2): Add gRPC and HTTP/REST transport implementations.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict


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

    def __init__(self, registry: Dict[str, "InMemoryRPCServer"]) -> None:
        self._registry = registry

    def call(self, target_node_id: str, method: str, **kwargs: Any) -> Any:
        """Simulate synchronous remote procedure call via direct handler lookup."""
        # TODO(Member 2): Add network delay simulation and transport error injection
        server = self._registry.get(target_node_id)
        if not server:
            raise ConnectionError(f"RPC destination node '{target_node_id}' unreachable.")
        handler = server.handlers.get(method)
        if not handler:
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

    def start(self) -> None:
        """Set server running state."""
        self.is_running = True

    def stop(self) -> None:
        """Set server stopped state."""
        self.is_running = False


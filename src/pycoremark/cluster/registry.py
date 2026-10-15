"""Node registry abstraction and in-memory implementation.

TODO(Member 1): Add thread-safe synchronization locks and persistent node caching.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from pycoremark.models.node import NodeInfo, NodeStatus


class NodeRegistry(ABC):
    """Abstract interface defining operations for worker node membership."""

    @abstractmethod
    def register(self, node: NodeInfo) -> None:
        """Register a new node into the cluster."""
        raise NotImplementedError

    @abstractmethod
    def unregister(self, node_id: str) -> None:
        """Remove a node from the cluster."""
        raise NotImplementedError

    @abstractmethod
    def get(self, node_id: str) -> Optional[NodeInfo]:
        """Fetch node information by node_id."""
        raise NotImplementedError

    @abstractmethod
    def list_all(self) -> List[NodeInfo]:
        """List all registered nodes."""
        raise NotImplementedError

    @abstractmethod
    def list_by_status(self, status: NodeStatus) -> List[NodeInfo]:
        """List nodes matching a specific status."""
        raise NotImplementedError

    @abstractmethod
    def update_status(self, node_id: str, status: NodeStatus) -> None:
        """Update the status of a specific node."""
        raise NotImplementedError


class InMemoryNodeRegistry(NodeRegistry):
    """In-memory concrete implementation of NodeRegistry."""

    def __init__(self) -> None:
        self._nodes: Dict[str, NodeInfo] = {}

    def register(self, node: NodeInfo) -> None:
        """Register a new node into the in-memory registry."""
        # TODO(Member 1): Add duplicate registration validation and concurrency control
        self._nodes[node.node_id] = node

    def unregister(self, node_id: str) -> None:
        """Remove a node from the in-memory registry."""
        # TODO(Member 1): Handle cleanup and task reallocation triggers
        self._nodes.pop(node_id, None)

    def get(self, node_id: str) -> Optional[NodeInfo]:
        """Retrieve node info by identifier."""
        return self._nodes.get(node_id)

    def list_all(self) -> List[NodeInfo]:
        """Return all tracked nodes."""
        return list(self._nodes.values())

    def list_by_status(self, status: NodeStatus) -> List[NodeInfo]:
        """Return nodes matching specified status."""
        return [node for node in self._nodes.values() if node.status == status]

    def update_status(self, node_id: str, status: NodeStatus) -> None:
        """Update node status in-memory."""
        if node_id in self._nodes:
            self._nodes[node_id].status = status


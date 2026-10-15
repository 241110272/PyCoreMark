"""Service handling dynamic worker registration and admission control.

TODO(Member 1): Implement authentication tokens and handshake verification.
"""

from pycoremark.cluster.registry import NodeRegistry
from pycoremark.common.exceptions import NodeAlreadyRegisteredError
from pycoremark.common.logging import get_logger
from pycoremark.models.node import NodeInfo, NodeStatus

logger = get_logger("cluster.registration")


class NodeRegistrationService:
    """Manages worker admission into the virtual cluster."""

    def __init__(self, registry: NodeRegistry) -> None:
        """Initialize registration service with injected NodeRegistry.

        Args:
            registry: NodeRegistry interface implementation.
        """
        self._registry = registry

    def register_worker(self, node: NodeInfo) -> bool:
        """Register a new worker node after validating requirements.

        Args:
            node: NodeInfo descriptor sent by worker.

        Returns:
            True if registration succeeded.

        Raises:
            NodeAlreadyRegisteredError: If node_id already exists and is active.
        """
        # TODO(Member 1): Check if node is already active or handle re-joining gracefully
        existing = self._registry.get(node.node_id)
        if existing and existing.status in (NodeStatus.READY, NodeStatus.BUSY):
            raise NodeAlreadyRegisteredError(f"Node {node.node_id} already registered and active.")

        node.status = NodeStatus.READY
        self._registry.register(node)
        logger.info("Worker node %s successfully admitted to cluster.", node.node_id)
        return True

    def deregister_worker(self, node_id: str) -> None:
        """Deregister and decommission a worker from the cluster.

        Args:
            node_id: Identifier of the worker to deregister.
        """
        # TODO(Member 1): Drain running tasks before removing node
        self._registry.unregister(node_id)
        logger.info("Worker node %s deregistered.", node_id)


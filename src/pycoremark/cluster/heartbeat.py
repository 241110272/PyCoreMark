"""Heartbeat monitoring and processing service.

TODO(Member 1): Implement sliding window latency tracking for heartbeat delays.
"""

from datetime import datetime
from pycoremark.cluster.registry import NodeRegistry
from pycoremark.common.logging import get_logger
from pycoremark.models.node import Heartbeat, NodeStatus

logger = get_logger("cluster.heartbeat")


class HeartbeatService:
    """Processes incoming worker heartbeats and maintains liveness timestamps."""

    def __init__(self, registry: NodeRegistry) -> None:
        """Initialize HeartbeatService with injected NodeRegistry.

        Args:
            registry: Registry holding cluster nodes.
        """
        self._registry = registry

    def process_heartbeat(self, heartbeat: Heartbeat) -> bool:
        """Process an incoming heartbeat packet from a node.

        Args:
            heartbeat: Heartbeat dataclass instance.

        Returns:
            True if heartbeat was accepted and recorded, False if node unknown.
        """
        # TODO(Member 1): Update sequence check, record jitter and load history
        node = self._registry.get(heartbeat.node_id)
        if not node:
            logger.warning("Received heartbeat from unknown node: %s", heartbeat.node_id)
            return False

        node.last_heartbeat_at = heartbeat.timestamp
        if node.status in (NodeStatus.SUSPECTED, NodeStatus.DEAD):
            logger.info("Node %s recovered from %s to %s", node.node_id, node.status, heartbeat.status)
        node.status = heartbeat.status
        return True


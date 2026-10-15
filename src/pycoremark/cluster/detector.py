"""Cluster node failure detector implementation.

TODO(Member 1): Implement Phi Accrual failure detection algorithm.
"""

from datetime import datetime, timezone
from typing import List
from pycoremark.cluster.registry import NodeRegistry
from pycoremark.common.logging import get_logger
from pycoremark.models.node import NodeInfo, NodeStatus

logger = get_logger("cluster.detector")


class FailureDetector:
    """Monitors heartbeat staleness to identify crashed or disconnected nodes."""

    def __init__(self, registry: NodeRegistry, timeout_seconds: float = 6.0) -> None:
        """Initialize failure detector.

        Args:
            registry: NodeRegistry to inspect.
            timeout_seconds: Duration after which missing heartbeats trigger failure.
        """
        self._registry = registry
        self._timeout_seconds = timeout_seconds

    def check_liveness(self) -> List[str]:
        """Check all nodes and flag those exceeding heartbeat timeout.

        Returns:
            List of node_ids that transitioned to SUSPECTED or DEAD.
        """
        # TODO(Member 1): Add configurable grace periods and notify event bus
        failed_nodes: List[str] = []
        now = datetime.now(timezone.utc)

        for node in self._registry.list_all():
            if node.status in (NodeStatus.DEAD, NodeStatus.OFFLINE):
                continue

            delta = (now - node.last_heartbeat_at).total_seconds()
            if delta > self._timeout_seconds:
                logger.warning(
                    "Node %s heartbeat timed out (%.2fs elapsed > %.2fs). Marking DEAD.",
                    node.node_id,
                    delta,
                    self._timeout_seconds,
                )
                self._registry.update_status(node.node_id, NodeStatus.DEAD)
                failed_nodes.append(node.node_id)

        return failed_nodes

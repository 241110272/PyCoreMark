"""Cluster manager orchestrating membership, scaling, and node lifecycle.

TODO(Member 1): Implement auto-scaling threshold policies and cluster drain semantics.
"""

from typing import List
from pycoremark.cluster.detector import FailureDetector
from pycoremark.cluster.heartbeat import HeartbeatService
from pycoremark.cluster.registration import NodeRegistrationService
from pycoremark.cluster.registry import NodeRegistry
from pycoremark.common.logging import get_logger
from pycoremark.models.node import NodeInfo, NodeStatus

logger = get_logger("cluster.manager")


class ClusterManager:
    """Unified coordinator component for cluster state, scaling, and health."""

    def __init__(
        self,
        registry: NodeRegistry,
        registration_service: NodeRegistrationService,
        heartbeat_service: HeartbeatService,
        failure_detector: FailureDetector,
    ) -> None:
        """Initialize ClusterManager with constituent cluster services.

        Args:
            registry: Node registry state.
            registration_service: Node admission controller.
            heartbeat_service: Heartbeat ingest handler.
            failure_detector: Heartbeat timeout watcher.
        """
        self.registry = registry
        self.registration_service = registration_service
        self.heartbeat_service = heartbeat_service
        self.failure_detector = failure_detector

    def get_active_nodes(self) -> List[NodeInfo]:
        """Return nodes currently capable of processing tasks."""
        return [
            n for n in self.registry.list_all()
            if n.status in (NodeStatus.READY, NodeStatus.BUSY)
        ]

    def scale_cluster(self, desired_node_count: int) -> int:
        """Trigger cluster scaling to target worker count.

        Args:
            desired_node_count: Target number of nodes.

        Returns:
            Number of currently active nodes.
        """
        # TODO(Member 1): Implement container spawning or dynamic worker process scaling
        logger.info("Scaling request received: desired nodes = %d", desired_node_count)
        current = len(self.get_active_nodes())
        return current


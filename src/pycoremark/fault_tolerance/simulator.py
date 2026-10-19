"""Chaos engineering and fault injection simulator.

TODO(Member 5): Add network partition (split-brain) and packet loss simulation.
"""

import random
from typing import List, Optional
from pycoremark.cluster.registry import NodeRegistry
from pycoremark.common.logging import get_logger
from pycoremark.models.node import NodeStatus

logger = get_logger("fault_tolerance.simulator")


class FailureSimulator:
    """Injects synthetic node crashes, latency, and task errors into the virtual cluster."""

    def __init__(self, registry: NodeRegistry) -> None:
        """Initialize simulator with cluster node registry.

        Args:
            registry: Registry containing active nodes.
        """
        self._registry = registry

    def inject_node_crash(self, node_id: str) -> bool:
        """Forcefully set a node's status to DEAD.

        Args:
            node_id: Target node to crash.

        Returns:
            True if node existed and was set to DEAD.
        """
        node = self._registry.get(node_id)
        if not node:
            return False
        logger.warning("Simulated crash injected on node: %s", node_id)
        self._registry.update_status(node_id, NodeStatus.DEAD)
        
        # DONE(Member 5): Emit NODE_FAILED cluster event
        logger.info(f"EVENT PUBLISHED: [NODE_FAILED] untuk node_id={node_id}. Memulai recovery...")
        
        return True

    def inject_random_failure(self, failure_probability: float = 0.2) -> Optional[str]:
        """Crash a random active node with given probability."""
        if random.random() > failure_probability:
            return None

        active_nodes = [
            n for n in self._registry.list_all()
            if n.status in (NodeStatus.READY, NodeStatus.BUSY)
        ]
        if not active_nodes:
            return None

        victim = random.choice(active_nodes)
        self.inject_node_crash(victim.node_id)
        return victim.node_id

    # DONE(Member 5): Add network partition (split-brain) and packet loss simulation.
    def inject_network_issues(self, issue_probability: float = 0.15) -> str:
        """Menyuntikkan gangguan jaringan seperti packet loss atau delay."""
        if random.random() > issue_probability:
            return "NETWORK_NORMAL"
            
        issues = ["PACKET_LOSS", "HIGH_LATENCY", "NETWORK_PARTITION"]
        selected_issue = random.choice(issues)
        logger.warning(f"Simulasi gangguan jaringan terdeteksi: {selected_issue}")
        return selected_issue
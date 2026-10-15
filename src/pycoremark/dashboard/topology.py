"""Cluster topology and node status query service.

TODO(Member 7): Add network graph representation and geographical/datacenter rack visualization data.
"""

from typing import Any, Dict, List
from pycoremark.cluster.registry import NodeRegistry


class ClusterTopologyService:
    """Provides snapshot views of active nodes, statuses, and resource allocations."""

    def __init__(self, registry: NodeRegistry) -> None:
        """Initialize topology service with node registry."""
        self._registry = registry

    def get_topology_snapshot(self) -> Dict[str, Any]:
        """Generate structured cluster topology summary for UI dashboard."""
        nodes = self._registry.list_all()
        return {
            "total_nodes": len(nodes),
            "nodes": [
                {
                    "node_id": n.node_id,
                    "status": n.status.value,
                    "total_cores": n.resources.total_cores,
                    "available_cores": n.resources.available_cores,
                    "total_memory_mb": n.resources.total_memory_mb,
                    "last_heartbeat": n.last_heartbeat_at.isoformat(),
                }
                for n in nodes
            ],
        }


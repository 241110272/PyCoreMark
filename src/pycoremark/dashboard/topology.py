"""Cluster topology and node status query service.

Network connections and physical placement are derived from registered
node metadata. Unknown links and unavailable placement data are omitted.
"""

from typing import Any, Dict, List, Mapping, Optional

from pycoremark.cluster.registry import NodeRegistry


def _first_value(
    sources: List[Mapping[str, Any]], *keys: str
) -> Optional[str]:
    """Return the first non-empty value from the supplied mappings."""
    for source in sources:
        for key in keys:
            value = source.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
    return None


class ClusterTopologyService:
    """Provide node snapshots, network graph, and rack/datacenter groupings."""

    def __init__(self, registry: NodeRegistry) -> None:
        self._registry = registry

    def get_topology_snapshot(self) -> Dict[str, Any]:
        nodes = self._registry.list_all()
        node_ids = {node.node_id for node in nodes}

        node_views: List[Dict[str, Any]] = []
        graph_nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, str]] = []
        seen_edges = set()
        grouped: Dict[str, Dict[str, Any]] = {}

        for node in nodes:
            tags = getattr(node, "tags", {}) or {}
            metadata = getattr(node.resources, "metadata", {}) or {}
            sources = [tags, metadata]

            datacenter = _first_value(
                sources, "datacenter", "data_center", "site", "location"
            )
            rack = _first_value(
                sources, "rack", "rack_id", "rack_name"
            )

            connections = metadata.get(
                "connections", metadata.get("connected_nodes", [])
            )
            if isinstance(connections, str):
                connections = [connections]
            if not isinstance(connections, (list, tuple, set)):
                connections = []

            node_views.append({
                "node_id": node.node_id,
                "hostname": node.hostname,
                "port": node.port,
                "status": node.status.value,
                "total_cores": node.resources.total_cores,
                "available_cores": node.resources.available_cores,
                "total_memory_mb": node.resources.total_memory_mb,
                "available_memory_mb": node.resources.available_memory_mb,
                "last_heartbeat": node.last_heartbeat_at.isoformat(),
                "datacenter": datacenter,
                "rack": rack,
            })

            graph_nodes.append({
                "id": node.node_id,
                "label": node.hostname or node.node_id,
                "status": node.status.value,
                "datacenter": datacenter,
                "rack": rack,
            })

            if datacenter or rack:
                dc_key = datacenter or "unspecified"
                rack_key = rack or "unspecified"
                dc_group = grouped.setdefault(
                    dc_key, {"datacenter": datacenter, "racks": {}}
                )
                rack_group = dc_group["racks"].setdefault(
                    rack_key, {"rack": rack, "node_ids": []}
                )
                rack_group["node_ids"].append(node.node_id)

            for target in connections:
                target_id = str(target).strip()
                if (
                    not target_id
                    or target_id not in node_ids
                    or target_id == node.node_id
                ):
                    continue

                edge_key = tuple(sorted((node.node_id, target_id)))
                if edge_key not in seen_edges:
                    seen_edges.add(edge_key)
                    edges.append({
                        "source": edge_key[0],
                        "target": edge_key[1],
                    })

        datacenter_racks = []
        for _, dc_group in sorted(grouped.items()):
            racks = [
                {
                    "rack": rack_group["rack"],
                    "node_ids": sorted(rack_group["node_ids"]),
                    "node_count": len(rack_group["node_ids"]),
                }
                for _, rack_group in sorted(dc_group["racks"].items())
            ]
            datacenter_racks.append({
                "datacenter": dc_group["datacenter"],
                "racks": racks,
            })

        return {
            "total_nodes": len(nodes),
            "nodes": node_views,
            "network_graph": {
                "nodes": graph_nodes,
                "edges": edges,
                "total_edges": len(edges),
            },
            "datacenter_racks": datacenter_racks,
        }
    
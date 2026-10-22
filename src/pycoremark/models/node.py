
"""Cluster node and heartbeat domain models."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class NodeStatus(str, Enum):
    """Lifecycle status of a cluster node."""

    INITIALIZING = "INITIALIZING"
    READY = "READY"
    BUSY = "BUSY"
    DRAINING = "DRAINING"
    SUSPECTED = "SUSPECTED"
    DEAD = "DEAD"
    OFFLINE = "OFFLINE"


@dataclass
class NetworkInterface:
    """Network interface information reported by a worker."""

    name: str
    mac_address: Optional[str] = None
    ipv4_addresses: List[str] = field(default_factory=list)
    ipv6_addresses: List[str] = field(default_factory=list)
    bandwidth_mbps: Optional[float] = None


@dataclass
class GPUResource:
    """GPU device and memory information reported by a worker."""

    device_id: str
    name: str
    total_memory_mb: int
    available_memory_mb: int
    compute_capability: Optional[str] = None
    utilization_percent: Optional[float] = None


@dataclass
class NodeResources:
    """Hardware and capacity specifications for a node."""

    total_cores: int
    available_cores: int
    total_memory_mb: int
    available_memory_mb: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    gpus: List[GPUResource] = field(default_factory=list)


@dataclass
class NodeInfo:
    """Registration and identity profile of a worker node."""

    node_id: str
    hostname: str
    port: int
    resources: NodeResources
    status: NodeStatus = NodeStatus.INITIALIZING
    registered_at: datetime = field(default_factory=_utc_now)
    last_heartbeat_at: datetime = field(default_factory=_utc_now)
    tags: Dict[str, str] = field(default_factory=dict)
    active_tasks: int = 0
    network_interfaces: List[NetworkInterface] = field(
        default_factory=list
    )


@dataclass
class Heartbeat:
    """Periodic liveness signal emitted by cluster workers."""

    node_id: str
    sequence_number: int
    timestamp: datetime = field(default_factory=_utc_now)
    status: NodeStatus = NodeStatus.READY
    current_load: float = 0.0
    active_tasks: int = 0

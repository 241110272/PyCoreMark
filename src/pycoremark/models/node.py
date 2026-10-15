"""Cluster node and heartbeat domain models.

TODO(Member 1): Add network interface metadata and GPU resource descriptors.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class NodeStatus(str, Enum):
    """Lifecycle status of a cluster node."""

    INITIALIZING = "INITIALIZING"
    READY = "READY"
    BUSY = "BUSY"
    SUSPECTED = "SUSPECTED"
    DEAD = "DEAD"
    OFFLINE = "OFFLINE"


@dataclass
class NodeResources:
    """Hardware and capacity specifications for a node."""

    total_cores: int
    available_cores: int
    total_memory_mb: int
    available_memory_mb: int
    metadata: Dict[str, Any] = field(default_factory=dict)


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


@dataclass
class Heartbeat:
    """Periodic liveness signal emitted by cluster workers."""

    node_id: str
    sequence_number: int
    timestamp: datetime = field(default_factory=_utc_now)
    status: NodeStatus = NodeStatus.READY
    current_load: float = 0.0
    active_tasks: int = 0

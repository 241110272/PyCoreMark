"""Telemetry, telemetry sampling, and distributed tracing models.

TODO(Member 6): Add OpenTelemetry context propagation format interop.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class MetricSample:
    """Point-in-time telemetry sample collected from a node."""

    node_id: str
    timestamp: datetime
    cpu_percent: float
    memory_used_mb: float
    memory_total_mb: float
    active_tasks: int
    network_bytes_sent: int = 0
    network_bytes_received: int = 0
    additional_metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TraceContext:
    """Distributed tracing propagation context."""

    trace_id: str
    parent_span_id: Optional[str] = None
    flags: int = 0
    baggage: Dict[str, str] = field(default_factory=dict)


@dataclass
class TraceSpan:
    """Individual execution interval within a distributed trace."""

    span_id: str
    trace_id: str
    parent_span_id: Optional[str]
    name: str
    node_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    tags: Dict[str, Any] = field(default_factory=dict)
    events: list[Dict[str, Any]] = field(default_factory=list)


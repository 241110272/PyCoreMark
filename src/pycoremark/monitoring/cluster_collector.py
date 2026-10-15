"""Cluster-wide aggregated metrics collector.

TODO(Member 6): Add Prometheus exporter endpoint /metrics integration.
"""

from typing import Dict, List
from pycoremark.models.metrics import MetricSample


class ClusterMetricsCollector:
    """Aggregates telemetry across all active cluster nodes."""

    def __init__(self) -> None:
        self._samples: List[MetricSample] = []

    def ingest_sample(self, sample: MetricSample) -> None:
        """Store a received node metric sample."""
        self._samples.append(sample)

    def get_latest_node_samples(self) -> Dict[str, MetricSample]:
        """Return the latest sample for each node."""
        latest: Dict[str, MetricSample] = {}
        for s in self._samples:
            latest[s.node_id] = s
        return latest

    def get_cluster_average_cpu(self) -> float:
        """Calculate current average CPU usage across active nodes."""
        latest = self.get_latest_node_samples()
        if not latest:
            return 0.0
        return sum(s.cpu_percent for s in latest.values()) / len(latest)


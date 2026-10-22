"""Cluster-wide aggregated metrics collector.

TODO(Member 6): Add Prometheus exporter endpoint /metrics integration.
"""

from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, List, Optional

from pycoremark.models.metrics import MetricSample


class ClusterMetricsCollector:
    """Aggregates telemetry across all active cluster nodes."""

    def __init__(self, max_samples: int = 10_000) -> None:
        """Initialize collector.

        Args:
            max_samples: Batas atas jumlah sample yang disimpan (mencegah
                pertumbuhan memori tanpa batas). Sample tertua dibuang duluan.
        """
        self._samples: Deque[MetricSample] = deque(maxlen=max_samples)
        # Cache sample terbaru per node agar query O(1), bukan O(n).
        self._latest: Dict[str, MetricSample] = {}

    def ingest_sample(self, sample: MetricSample) -> None:
        """Store a received node metric sample."""
        self._samples.append(sample)
        current = self._latest.get(sample.node_id)
        if current is None or sample.timestamp >= current.timestamp:
            self._latest[sample.node_id] = sample

    def get_latest_node_samples(
        self,
        max_age_seconds: Optional[float] = None,
    ) -> Dict[str, MetricSample]:
        """Return the latest sample for each node.

        Args:
            max_age_seconds: Jika diisi, node yang sample terbarunya lebih tua
                dari batas ini dianggap tidak aktif dan tidak disertakan.
        """
        if max_age_seconds is None:
            return dict(self._latest)

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds)
        return {
            node_id: sample
            for node_id, sample in self._latest.items()
            if sample.timestamp >= cutoff
        }

    def get_cluster_average_cpu(
        self,
        max_age_seconds: Optional[float] = None,
    ) -> float:
        """Calculate current average CPU usage across active nodes.

        Args:
            max_age_seconds: Batas umur sample agar dianggap node aktif.
                Default None = semua node yang punya sample dihitung.
        """
        latest = self.get_latest_node_samples(max_age_seconds=max_age_seconds)
        if not latest:
            return 0.0
        return sum(s.cpu_percent for s in latest.values()) / len(latest)

    def get_all_samples(self) -> List[MetricSample]:
        """Return seluruh sample (untuk ekspor/debug)."""
        return list(self._samples)
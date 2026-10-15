"""Node-level telemetry metric collector.

TODO(Member 6): Integrate psutil for actual host CPU/RAM and disk I/O readings.
"""

from datetime import datetime, timezone
import random
from pycoremark.models.metrics import MetricSample


class NodeMetricsCollector:
    """Collects CPU and memory resource consumption metrics for a specific node."""

    def __init__(self, node_id: str) -> None:
        """Initialize collector for a node.

        Args:
            node_id: Identifier of the monitored node.
        """
        self.node_id = node_id

    def collect(self) -> MetricSample:
        """Collect current metric sample for this node.

        Returns:
            MetricSample containing telemetry measurements.
        """
        # TODO(Member 6): Replace simulation with psutil.cpu_percent() and psutil.virtual_memory()
        simulated_cpu = round(random.uniform(10.0, 85.0), 2)
        simulated_mem_used = round(random.uniform(512.0, 3072.0), 2)
        return MetricSample(
            node_id=self.node_id,
            timestamp=datetime.now(timezone.utc),
            cpu_percent=simulated_cpu,
            memory_used_mb=simulated_mem_used,
            memory_total_mb=4096.0,
            active_tasks=0,
        )

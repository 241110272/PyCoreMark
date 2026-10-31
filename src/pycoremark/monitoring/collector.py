"""Host telemetry collection backed by psutil."""

from datetime import datetime, timezone

try:
    import psutil
except ImportError:  # pragma: no cover - exercised on minimal installs
    psutil = None

from pycoremark.models.metrics import MetricSample


class NodeMetricsCollector:
    """Collect real CPU, memory, disk, and network counters for a node."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        if psutil is not None:
            # Prime the non-blocking CPU counter; its first result is always 0.
            psutil.cpu_percent(interval=None)

    def collect(self) -> MetricSample:
        """Return one real telemetry sample (never fabricated values)."""
        if psutil is None:
            return MetricSample(
                node_id=self.node_id,
                timestamp=datetime.now(timezone.utc),
                cpu_percent=None,
                memory_used_mb=None,
                memory_total_mb=None,
                active_tasks=0,
                additional_metrics={"telemetry_backend": "unavailable"},
            )

        memory = psutil.virtual_memory()
        disk = psutil.disk_io_counters()
        network = psutil.net_io_counters()
        additional = {"telemetry_backend": "psutil"}
        if disk is not None:
            additional["disk_read_bytes"] = disk.read_bytes
            additional["disk_write_bytes"] = disk.write_bytes
            additional["disk_read_count"] = disk.read_count
            additional["disk_write_count"] = disk.write_count
        return MetricSample(
            node_id=self.node_id,
            timestamp=datetime.now(timezone.utc),
            cpu_percent=float(psutil.cpu_percent(interval=None)),
            memory_used_mb=memory.used / (1024 * 1024),
            memory_total_mb=memory.total / (1024 * 1024),
            active_tasks=0,
            network_bytes_sent=network.bytes_sent if network else 0,
            network_bytes_received=network.bytes_recv if network else 0,
            additional_metrics=additional,
        )

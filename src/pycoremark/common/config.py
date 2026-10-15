"""Configuration schema and loader for PyCoreMark cluster nodes and services.

TODO(Member 1): Add schema validation and support for YAML/TOML configuration files.
"""

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class ClusterConfig:
    """Cluster-wide runtime configuration parameters."""

    coordinator_host: str = "127.0.0.1"
    coordinator_port: int = 8000
    worker_id: str = "worker-0"
    worker_cores: int = 4
    worker_memory_mb: int = 4096
    broker_url: str = "memory://"
    database_url: str = "sqlite:///pycoremark.db"
    heartbeat_interval: float = 2.0
    failure_timeout: float = 6.0
    max_retries: int = 3
    metrics_interval: float = 1.0
    log_level: str = "INFO"


def load_config_from_env() -> ClusterConfig:
    """Load configuration values from environment variables with fallback defaults.

    Returns:
        Populated immutable ClusterConfig instance.
    """
    return ClusterConfig(
        coordinator_host=os.getenv("PYCOREMARK_COORDINATOR_HOST", "127.0.0.1"),
        coordinator_port=int(os.getenv("PYCOREMARK_COORDINATOR_PORT", "8000")),
        worker_id=os.getenv("PYCOREMARK_WORKER_ID", "worker-0"),
        worker_cores=int(os.getenv("PYCOREMARK_WORKER_CORES", "4")),
        worker_memory_mb=int(os.getenv("PYCOREMARK_WORKER_MEMORY_MB", "4096")),
        broker_url=os.getenv("PYCOREMARK_BROKER_URL", "memory://"),
        database_url=os.getenv("PYCOREMARK_DATABASE_URL", "sqlite:///pycoremark.db"),
        heartbeat_interval=float(os.getenv("PYCOREMARK_HEARTBEAT_INTERVAL", "2.0")),
        failure_timeout=float(os.getenv("PYCOREMARK_FAILURE_TIMEOUT", "6.0")),
        max_retries=int(os.getenv("PYCOREMARK_MAX_RETRIES", "3")),
        metrics_interval=float(os.getenv("PYCOREMARK_METRICS_INTERVAL", "1.0")),
        log_level=os.getenv("PYCOREMARK_LOG_LEVEL", "INFO"),
    )


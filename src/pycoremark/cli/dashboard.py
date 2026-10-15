"""CLI entry point for launching the Dashboard service."""

from pycoremark.cluster.registry import InMemoryNodeRegistry
from pycoremark.common.config import load_config_from_env
from pycoremark.common.logging import get_logger, setup_logging
from pycoremark.dashboard.comparison import ExperimentComparisonService
from pycoremark.dashboard.service import DashboardService
from pycoremark.dashboard.topology import ClusterTopologyService
from pycoremark.storage.memory import InMemoryExperimentRepository


def main() -> None:
    """Initialize dependencies and start Dashboard service skeleton."""
    config = load_config_from_env()
    setup_logging(config.log_level)
    logger = get_logger("cli.dashboard")

    node_registry = InMemoryNodeRegistry()
    topology_service = ClusterTopologyService(node_registry)
    experiment_repo = InMemoryExperimentRepository()
    comparison_service = ExperimentComparisonService(experiment_repo)

    dashboard = DashboardService(
        topology_service=topology_service,
        comparison_service=comparison_service,
    )

    logger.info("Dashboard initialized successfully in skeleton mode.")
    status = dashboard.get_cluster_status()
    logger.info("Initial cluster status snapshot: total_nodes=%d", status["total_nodes"])


if __name__ == "__main__":
    main()


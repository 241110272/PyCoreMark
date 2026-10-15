"""Dashboard facade service providing API endpoints for UI clients.

TODO(Member 7): Integrate FastAPI / Flask / Streamlit web front-end.
"""

from typing import Any, Dict, List
from pycoremark.common.logging import get_logger
from pycoremark.dashboard.comparison import ExperimentComparisonService
from pycoremark.dashboard.topology import ClusterTopologyService

logger = get_logger("dashboard.service")


class DashboardService:
    """Facade for dashboard queries, cluster topology, and experiment metrics."""

    def __init__(
        self,
        topology_service: ClusterTopologyService,
        comparison_service: ExperimentComparisonService,
    ) -> None:
        """Initialize DashboardService with injected topology and comparison services."""
        self.topology_service = topology_service
        self.comparison_service = comparison_service

    def get_cluster_status(self) -> Dict[str, Any]:
        """Fetch current cluster topology snapshot."""
        return self.topology_service.get_topology_snapshot()

    def compare_experiments(self, experiment_ids: List[str]) -> Dict[str, Any]:
        """Compare results of specified experiments."""
        return self.comparison_service.compare(experiment_ids)


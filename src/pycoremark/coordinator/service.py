"""Coordinator service main runtime hosting coordination components.

TODO(Member 7): Add REST / gRPC API server for remote dashboard commands.
"""

from pycoremark.cluster.manager import ClusterManager
from pycoremark.cluster.registry import NodeRegistry
from pycoremark.common.config import ClusterConfig
from pycoremark.common.logging import get_logger
from pycoremark.coordinator.aggregator import ResultAggregator
from pycoremark.coordinator.controller import ExperimentController
from pycoremark.coordinator.dispatcher import TaskDispatcher
from pycoremark.models.experiment import ExperimentConfig, ExperimentResult
from pycoremark.models.node import Heartbeat, NodeInfo
from pycoremark.scheduling.base import BaseScheduler

logger = get_logger("coordinator.service")


class CoordinatorService:
    """Primary central coordination service in the PyCoreMark virtual cluster."""

    def __init__(
        self,
        config: ClusterConfig,
        cluster_manager: ClusterManager,
        scheduler: BaseScheduler,
        dispatcher: TaskDispatcher,
        aggregator: ResultAggregator,
        controller: ExperimentController,
    ) -> None:
        """Initialize CoordinatorService with constructor dependency injection.

        Args:
            config: Cluster configuration.
            cluster_manager: Cluster manager instance.
            scheduler: Scheduling strategy implementation.
            dispatcher: Task dispatcher instance.
            aggregator: Result aggregator instance.
            controller: Experiment controller instance.
        """
        self.config = config
        self.cluster_manager = cluster_manager
        self.scheduler = scheduler
        self.dispatcher = dispatcher
        self.aggregator = aggregator
        self.controller = controller
        self._is_running = False

    def start(self) -> None:
        """Start coordinator daemon loops."""
        logger.info(
            "CoordinatorService starting on %s:%d",
            self.config.coordinator_host,
            self.config.coordinator_port,
        )
        self._is_running = True

    def stop(self) -> None:
        """Stop coordinator service."""
        logger.info("CoordinatorService shutting down.")
        self._is_running = False

    def handle_register_node(self, node: NodeInfo) -> bool:
        """Handle incoming node registration request."""
        return self.cluster_manager.registration_service.register_worker(node)

    def handle_heartbeat(self, heartbeat: Heartbeat) -> bool:
        """Handle incoming heartbeat from worker."""
        return self.cluster_manager.heartbeat_service.process_heartbeat(heartbeat)

    def trigger_experiment(self, config: ExperimentConfig) -> ExperimentResult:
        """Trigger an experiment benchmark run."""
        return self.controller.run_experiment(config)


"""CLI entry point for launching the Coordinator service."""

from pycoremark.analysis.analyzer import PerformanceAnalyzer
from pycoremark.analysis.metrics_calc import (
    EfficiencyCalculator,
    LoadBalanceCalculator,
    SpeedupCalculator,
    ThroughputCalculator,
)
from pycoremark.analysis.overhead import OverheadCalculator
from pycoremark.cluster.detector import FailureDetector
from pycoremark.cluster.heartbeat import HeartbeatService
from pycoremark.cluster.manager import ClusterManager
from pycoremark.cluster.registration import NodeRegistrationService
from pycoremark.cluster.registry import InMemoryNodeRegistry
from pycoremark.common.config import load_config_from_env
from pycoremark.common.logging import get_logger, setup_logging
from pycoremark.communication.broker import InMemoryMessageBroker
from pycoremark.coordinator.aggregator import ResultAggregator
from pycoremark.coordinator.controller import ExperimentController
from pycoremark.coordinator.dispatcher import TaskDispatcher
from pycoremark.coordinator.service import CoordinatorService
from pycoremark.scheduling.dynamic import DynamicScheduler
from pycoremark.storage.memory import InMemoryExperimentRepository
from pycoremark.workloads.cpu import CPUWorkload
from pycoremark.workloads.registry import WorkloadRegistry


def main() -> None:
    """Initialize dependencies and start Coordinator service skeleton."""
    config = load_config_from_env()
    setup_logging(config.log_level)
    logger = get_logger("cli.coordinator")

    # Dependency Injection Setup
    node_registry = InMemoryNodeRegistry()
    registration_service = NodeRegistrationService(node_registry)
    heartbeat_service = HeartbeatService(node_registry)
    failure_detector = FailureDetector(node_registry, timeout_seconds=config.failure_timeout)
    cluster_manager = ClusterManager(
        registry=node_registry,
        registration_service=registration_service,
        heartbeat_service=heartbeat_service,
        failure_detector=failure_detector,
    )

    broker = InMemoryMessageBroker()
    dispatcher = TaskDispatcher(broker)
    aggregator = ResultAggregator()
    scheduler = DynamicScheduler()

    workload_registry = WorkloadRegistry()
    workload_registry.register("cpu_benchmark", CPUWorkload)

    analyzer = PerformanceAnalyzer(
        throughput_calc=ThroughputCalculator(),
        speedup_calc=SpeedupCalculator(),
        efficiency_calc=EfficiencyCalculator(),
        load_balance_calc=LoadBalanceCalculator(),
        overhead_calc=OverheadCalculator(),
    )
    experiment_repo = InMemoryExperimentRepository()

    controller = ExperimentController(
        cluster_manager=cluster_manager,
        scheduler=scheduler,
        dispatcher=dispatcher,
        aggregator=aggregator,
        workload_registry=workload_registry,
        analyzer=analyzer,
        experiment_repository=experiment_repo,
    )

    service = CoordinatorService(
        config=config,
        cluster_manager=cluster_manager,
        scheduler=scheduler,
        dispatcher=dispatcher,
        aggregator=aggregator,
        controller=controller,
    )

    service.start()
    logger.info("Coordinator initialized successfully in skeleton mode.")


if __name__ == "__main__":
    main()


"""CLI entry point for running a minimal dummy benchmark experiment."""

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
from pycoremark.models.experiment import ExperimentConfig
from pycoremark.models.node import NodeInfo, NodeResources, NodeStatus
from pycoremark.models.workload import WorkloadConfig, WorkloadType
from pycoremark.reporting.exporters import CSVExporter, JSONExporter
from pycoremark.reporting.generator import ReportGenerator
from pycoremark.reporting.validator import ResultValidator
from pycoremark.scheduling.dynamic import DynamicScheduler
from pycoremark.storage.memory import InMemoryExperimentRepository
from pycoremark.workloads.cpu import CPUWorkload
from pycoremark.workloads.registry import WorkloadRegistry


def main() -> None:
    """Run an end-to-end dummy simulation experiment."""
    config = load_config_from_env()
    setup_logging(config.log_level)
    logger = get_logger("cli.experiment")

    logger.info("Initializing components for dummy experiment...")

    # Set up cluster
    node_registry = InMemoryNodeRegistry()
    reg_service = NodeRegistrationService(node_registry)
    hb_service = HeartbeatService(node_registry)
    detector = FailureDetector(node_registry, timeout_seconds=config.failure_timeout)
    cluster_manager = ClusterManager(node_registry, reg_service, hb_service, detector)

    # Register 2 virtual worker nodes for testing
    for i in range(1, 3):
        node = NodeInfo(
            node_id=f"worker-{i}",
            hostname="localhost",
            port=8000 + i,
            resources=NodeResources(
                total_cores=4,
                available_cores=4,
                total_memory_mb=4096,
                available_memory_mb=4096,
            ),
            status=NodeStatus.READY,
        )
        reg_service.register_worker(node)

    # Communication & orchestration
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

    coordinator = CoordinatorService(
        config=config,
        cluster_manager=cluster_manager,
        scheduler=scheduler,
        dispatcher=dispatcher,
        aggregator=aggregator,
        controller=controller,
    )

    coordinator.start()

    # Trigger dummy experiment
    exp_config = ExperimentConfig(
        experiment_id="exp-dummy-001",
        name="Dummy CPU Benchmark",
        workload=WorkloadConfig(
            name="cpu_benchmark",
            workload_type=WorkloadType.CPU_BOUND,
            total_tasks=4,
            task_complexity=50,
        ),
        target_nodes=2,
    )

    result = coordinator.trigger_experiment(exp_config)
    logger.info("Dummy experiment result: %s", result)

    # Reporting
    generator = ReportGenerator(
        validator=ResultValidator(),
        json_exporter=JSONExporter(),
        csv_exporter=CSVExporter(),
    )
    json_report = generator.generate_json_report([result])
    logger.info("Generated JSON Report:\n%s", json_report)


if __name__ == "__main__":
    main()


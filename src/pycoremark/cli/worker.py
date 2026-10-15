"""CLI entry point for launching a Worker node service."""

from pycoremark.common.config import load_config_from_env
from pycoremark.common.logging import get_logger, setup_logging
from pycoremark.communication.broker import InMemoryMessageBroker
from pycoremark.monitoring.collector import NodeMetricsCollector
from pycoremark.worker.agents import WorkerHeartbeatAgent, WorkerMetricsAgent
from pycoremark.worker.executor import TaskExecutor
from pycoremark.worker.service import WorkerService
from pycoremark.workloads.cpu import CPUWorkload
from pycoremark.workloads.registry import WorkloadRegistry


def main() -> None:
    """Initialize dependencies and start Worker node service skeleton."""
    config = load_config_from_env()
    setup_logging(config.log_level)
    logger = get_logger("cli.worker")

    broker = InMemoryMessageBroker()
    workload_registry = WorkloadRegistry()
    workload_registry.register("cpu_benchmark", CPUWorkload)

    executor = TaskExecutor(node_id=config.worker_id, workload_registry=workload_registry)
    heartbeat_agent = WorkerHeartbeatAgent(
        node_id=config.worker_id,
        broker=broker,
        interval_seconds=config.heartbeat_interval,
    )
    metrics_collector = NodeMetricsCollector(node_id=config.worker_id)
    metrics_agent = WorkerMetricsAgent(
        collector=metrics_collector,
        broker=broker,
        interval_seconds=config.metrics_interval,
    )

    worker = WorkerService(
        config=config,
        executor=executor,
        heartbeat_agent=heartbeat_agent,
        metrics_agent=metrics_agent,
        broker=broker,
    )

    worker.start()
    logger.info("Worker node %s initialized successfully in skeleton mode.", config.worker_id)


if __name__ == "__main__":
    main()


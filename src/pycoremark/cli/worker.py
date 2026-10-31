"""CLI entry point for a worker that connects to a remote coordinator."""

import time
import socket
from dataclasses import replace

from pycoremark.common.config import load_config_from_env
from pycoremark.common.logging import get_logger, setup_logging
from pycoremark.communication.http_transport import HTTPMessageBroker
from pycoremark.communication.grpc_transport import GrpcWorkerBroker
from pycoremark.communication.redis_streams import RedisStreamsWorkerBroker
from pycoremark.monitoring.collector import NodeMetricsCollector
from pycoremark.worker.agents import WorkerHeartbeatAgent, WorkerMetricsAgent
from pycoremark.worker.executor import TaskExecutor
from pycoremark.worker.service import WorkerService
from pycoremark.workloads.cpu import CPUWorkload
from pycoremark.workloads.data import PartitionedDataWorkload
from pycoremark.workloads.mixed import MixedWorkload
from pycoremark.workloads.registry import WorkloadRegistry


def main() -> None:
    config = load_config_from_env()
    if config.worker_id.lower() in {"auto", "worker-auto"}:
        config = replace(config, worker_id=f"worker-{socket.gethostname()}")
    setup_logging(config.log_level)
    logger = get_logger("cli.worker")
    coordinator_url = f"http://{config.coordinator_host}:{config.coordinator_port}"
    if config.broker_url.startswith("redis://"):
        import redis
        client = redis.Redis.from_url(
            config.broker_url, decode_responses=True, health_check_interval=30
        )
        broker = RedisStreamsWorkerBroker(
            client,
            worker_id=config.worker_id,
            coordinator_host=config.coordinator_host,
            grpc_port=config.grpc_port,
            auth_token=config.registration_auth_token,
            poll_timeout_seconds=min(1.0, config.heartbeat_interval),
        )
    elif config.transport == "grpc":
        broker = GrpcWorkerBroker(
            config.coordinator_host,
            config.grpc_port,
            worker_id=config.worker_id,
            auth_token=config.registration_auth_token,
            timeout_seconds=max(2.0, config.heartbeat_interval + 1.0),
            simulated_latency_ms=config.simulated_network_delay_ms,
            simulated_failure_rate=config.network_failure_rate,
        )
    else:
        broker = HTTPMessageBroker(
            coordinator_url,
            poll_timeout=min(1.0, config.heartbeat_interval),
            auth_token=config.registration_auth_token,
            simulated_latency_ms=config.simulated_network_delay_ms,
            simulated_failure_rate=config.network_failure_rate,
        )
    workload_registry = WorkloadRegistry()
    workload_registry.register("cpu_benchmark", CPUWorkload)
    workload_registry.register("partitioned_data", PartitionedDataWorkload)
    workload_registry.register("mixed", MixedWorkload)
    executor = TaskExecutor(
        config.worker_id,
        workload_registry,
        failure_rate=config.worker_failure_rate,
        artificial_delay_ms=config.simulated_network_delay_ms,
    )
    worker = WorkerService(
        config=config,
        executor=executor,
        heartbeat_agent=WorkerHeartbeatAgent(
            config.worker_id, broker, interval_seconds=config.heartbeat_interval
        ),
        metrics_agent=WorkerMetricsAgent(
            NodeMetricsCollector(config.worker_id), broker,
            interval_seconds=config.metrics_interval,
        ),
        broker=broker,
    )
    try:
        for attempt in range(30):
            try:
                broker.register_node(worker.build_node_info())
                break
            except ConnectionError:
                if attempt == 29:
                    raise
                logger.warning("Coordinator is not reachable yet; retrying registration.")
                time.sleep(1.0)
        worker.start()
        logger.info("Worker %s connected to %s", config.worker_id, coordinator_url)
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Worker %s shutting down.", config.worker_id)
    finally:
        worker.stop()
        try:
            broker.deregister_node(config.worker_id)
        except (ConnectionError, ValueError):
            logger.warning("Worker could not send its final deregistration notice.")
        finally:
            close = getattr(broker, "close", None)
            if callable(close):
                close()


if __name__ == "__main__":
    main()

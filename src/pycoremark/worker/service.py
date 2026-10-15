"""Worker node service runtime.

TODO(Member 7): Add concurrent worker task queue polling thread pool.
"""

from pycoremark.common.config import ClusterConfig
from pycoremark.common.logging import get_logger
from pycoremark.communication.broker import MessageBroker
from pycoremark.models.node import NodeInfo, NodeResources, NodeStatus
from pycoremark.worker.agents import WorkerHeartbeatAgent, WorkerMetricsAgent
from pycoremark.worker.executor import TaskExecutor

logger = get_logger("worker.service")


class WorkerService:
    """Virtual cluster worker node daemon."""

    def __init__(
        self,
        config: ClusterConfig,
        executor: TaskExecutor,
        heartbeat_agent: WorkerHeartbeatAgent,
        metrics_agent: WorkerMetricsAgent,
        broker: MessageBroker,
    ) -> None:
        """Initialize WorkerService with dependency injection."""
        self.config = config
        self.executor = executor
        self.heartbeat_agent = heartbeat_agent
        self.metrics_agent = metrics_agent
        self.broker = broker
        self._is_running = False

    def build_node_info(self) -> NodeInfo:
        """Create NodeInfo descriptor for registration with coordinator."""
        return NodeInfo(
            node_id=self.config.worker_id,
            hostname="localhost",
            port=0,
            resources=NodeResources(
                total_cores=self.config.worker_cores,
                available_cores=self.config.worker_cores,
                total_memory_mb=self.config.worker_memory_mb,
                available_memory_mb=self.config.worker_memory_mb,
            ),
            status=NodeStatus.READY,
        )

    def start(self) -> None:
        """Start worker service listeners."""
        logger.info(
            "WorkerService '%s' starting with %d cores and %d MB RAM",
            self.config.worker_id,
            self.config.worker_cores,
            self.config.worker_memory_mb,
        )
        self._is_running = True
        # Subscribe to worker task queue
        topic = f"worker.{self.config.worker_id}.tasks"
        self.broker.subscribe(topic)
        # Emit initial heartbeat
        self.heartbeat_agent.send_heartbeat()

    def stop(self) -> None:
        """Stop worker service."""
        logger.info("WorkerService '%s' stopping", self.config.worker_id)
        self._is_running = False


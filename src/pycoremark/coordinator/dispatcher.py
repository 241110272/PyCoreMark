"""Task dispatcher responsible for sending tasks to worker nodes.

TODO(Member 7): Implement flow-control backpressure and acknowledgment timeouts.
"""

from pycoremark.communication.broker import MessageBroker
from pycoremark.common.logging import get_logger
from pycoremark.models.task import Task, TaskStatus

logger = get_logger("coordinator.dispatcher")


class TaskDispatcher:
    """Dispatches scheduled tasks to worker message queues or RPC transports."""

    def __init__(self, broker: MessageBroker) -> None:
        """Initialize dispatcher with injected MessageBroker.

        Args:
            broker: MessageBroker backend instance.
        """
        self._broker = broker

    def dispatch(self, task: Task, target_node_id: str) -> None:
        """Dispatch task to target worker's dedicated queue.

        Args:
            task: Task instance to dispatch.
            target_node_id: Destination worker node id.
        """
        # TODO(Member 7): Add dispatch timeout monitoring and delivery acknowledgments
        task.assigned_node_id = target_node_id
        task.status = TaskStatus.SCHEDULED
        topic = f"worker.{target_node_id}.tasks"
        logger.debug("Dispatching task %s to %s", task.task_id, topic)
        self._broker.publish(topic, task)


"""Task dispatcher responsible for sending tasks to worker nodes.

TODO(Member 7): Implement flow-control backpressure and acknowledgment timeouts.
"""

from pycoremark.communication.broker import MessageBroker
from pycoremark.communication.clock import LamportClock
from pycoremark.common.logging import get_logger
from pycoremark.models.event import ClusterEvent, ClusterEventType
from datetime import datetime, timezone
from pycoremark.models.task import Task, TaskStatus
from queue import Empty
from uuid import uuid4

logger = get_logger("coordinator.dispatcher")


class TaskDispatcher:
    """Dispatches scheduled tasks to worker message queues or RPC transports."""

    def __init__(self, broker: MessageBroker, clock: LamportClock | None = None) -> None:
        """Initialize dispatcher with injected MessageBroker.

        Args:
            broker: MessageBroker backend instance.
        """
        self._broker = broker
        self._result_queue = broker.subscribe("cluster.task_results")
        self._event_clock = clock or LamportClock()

    def dispatch(self, task: Task, target_node_id: str) -> None:
        """Dispatch task to target worker's dedicated queue.

        Args:
            task: Task instance to dispatch.
            target_node_id: Destination worker node id.
        """
        # TODO(Member 7): Add dispatch timeout monitoring and delivery acknowledgments
        task.assigned_node_id = target_node_id
        task.status = TaskStatus.SCHEDULED
        task.queued_at = datetime.now(timezone.utc)
        task.logical_clock = self.publish_event(
            ClusterEventType.TASK_SCHEDULED,
            {"task_id": task.task_id, "worker_id": target_node_id},
        )
        topic = f"worker.{target_node_id}.tasks"
        logger.debug("Dispatching task %s to %s", task.task_id, topic)

        self._broker.publish(topic, task)

    def publish_event(self, event_type: ClusterEventType, payload: dict) -> int:
        logical_clock = self._event_clock.tick()
        self._broker.publish("cluster.events", ClusterEvent(
            event_id=uuid4().hex,
            event_type=event_type,
            source_node_id="coordinator",
            payload=payload,
            logical_clock=logical_clock,
        ))
        return logical_clock

    def receive_result(self, timeout: float = 0.1):
        """Return the next worker result, or ``None`` when the wait expires."""
        try:
            message = self._result_queue.get(timeout=max(0.0, timeout))
        except Empty:
            return None
        return getattr(message, "payload", message)


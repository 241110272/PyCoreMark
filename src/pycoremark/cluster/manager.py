
import math
from typing import Callable, List, Optional

from pycoremark.cluster.detector import FailureDetector
from pycoremark.cluster.heartbeat import HeartbeatService
from pycoremark.cluster.registration import NodeRegistrationService
from pycoremark.cluster.registry import NodeRegistry
from pycoremark.common.exceptions import NodeNotFoundError
from pycoremark.common.logging import get_logger, traced
from pycoremark.models.node import Heartbeat, NodeInfo, NodeStatus

logger = get_logger("cluster.manager")


class ClusterManager:

    def __init__(
        self,
        registry: NodeRegistry,
        registration_service: NodeRegistrationService,
        heartbeat_service: HeartbeatService,
        failure_detector: FailureDetector,
        worker_factory: Optional[Callable[[], NodeInfo]] = None,
        scale_up_threshold: float = 0.8,
        scale_down_threshold: float = 0.2,
        min_node_count: int = 1,
        max_node_count: int = 10,
    ) -> None:
        self._validate_threshold(scale_up_threshold, "scale_up_threshold")
        self._validate_threshold(scale_down_threshold, "scale_down_threshold")

        if scale_down_threshold >= scale_up_threshold:
            raise ValueError(
                "scale_down_threshold must be lower than scale_up_threshold."
            )

        self._validate_node_count(min_node_count, "min_node_count")
        self._validate_node_count(max_node_count, "max_node_count")

        if max_node_count < min_node_count:
            raise ValueError(
                "max_node_count must be greater than or equal to min_node_count."
            )

        if worker_factory is not None and not callable(worker_factory):
            raise ValueError("worker_factory must be callable or None.")

        self.registry = registry
        self.registration_service = registration_service
        self.heartbeat_service = heartbeat_service
        self.failure_detector = failure_detector
        self.worker_factory = worker_factory
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.min_node_count = min_node_count
        self.max_node_count = max_node_count

    @staticmethod
    def _validate_threshold(value: float, name: str) -> None:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
        ):
            raise ValueError(
                f"{name} must be a finite nonnegative number."
            )

    @staticmethod
    def _validate_node_count(value: int, name: str) -> None:
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
        ):
            raise ValueError(
                f"{name} must be a nonnegative integer."
            )

    def get_active_nodes(self) -> List[NodeInfo]:
        return [
            node
            for node in self.registry.list_all()
            if node.status in (NodeStatus.READY, NodeStatus.BUSY)
        ]

    @traced
    def register_worker(
        self,
        node: NodeInfo,
        *,
        auth_token: Optional[str] = None,
        handshake_response: Optional[str] = None,
    ) -> bool:
        return self.registration_service.register_worker(
            node,
            auth_token=auth_token,
            handshake_response=handshake_response,
        )

    @traced
    def process_heartbeat(self, heartbeat: Heartbeat) -> bool:
        return self.heartbeat_service.process_heartbeat(heartbeat)

    @traced
    def drain_worker(self, node_id: str) -> bool:
        node = self.registry.get(node_id)

        if node is None:
            raise NodeNotFoundError(
                f"Node '{node_id}' is not registered."
            )

        if node.status != NodeStatus.DRAINING:
            self.registry.update_status(
                node_id,
                NodeStatus.DRAINING,
            )
            logger.info("Worker %s entered draining state.", node_id)

        node = self.registry.get(node_id)

        if node is None:
            raise NodeNotFoundError(
                f"Node '{node_id}' is no longer registered."
            )

        if node.active_tasks > 0:
            logger.info(
                "Worker %s is draining; waiting for %d active tasks.",
                node_id,
                node.active_tasks,
            )
            return False

        deregistered = self.registration_service.deregister_worker(node_id)

        if not deregistered:
            return False

        self.heartbeat_service.forget_node(node_id)
        logger.info("Worker %s drained and deregistered.", node_id)
        return True

    def deregister_worker(self, node_id: str) -> bool:
        return self.drain_worker(node_id)

    @traced
    def check_cluster_liveness(self) -> List[str]:
        changed_nodes = self.failure_detector.check_liveness()

        if changed_nodes:
            logger.warning(
                "Cluster liveness check changed node status for: %s",
                ", ".join(changed_nodes),
            )

        return changed_nodes

    def get_scaling_recommendation(self) -> int:
        active_nodes = self.get_active_nodes()
        current_count = len(active_nodes)

        if not active_nodes:
            return max(current_count, self.min_node_count)

        latest_loads = []

        for node in active_nodes:
            history = self.heartbeat_service.get_load_history(node.node_id)

            if history:
                latest_loads.append(history[-1])

        if not latest_loads:
            return current_count

        average_load = sum(latest_loads) / len(latest_loads)

        if average_load >= self.scale_up_threshold:
            return min(current_count + 1, self.max_node_count)

        if average_load <= self.scale_down_threshold:
            return max(current_count - 1, self.min_node_count)

        return current_count

    @traced
    def autoscale(self) -> int:
        desired_node_count = self.get_scaling_recommendation()
        current_node_count = len(self.get_active_nodes())

        if desired_node_count == current_node_count:
            return current_node_count

        logger.info(
            "Autoscaling policy selected target=%d from active=%d.",
            desired_node_count,
            current_node_count,
        )

        return self.scale_cluster(desired_node_count)

    @traced
    def scale_cluster(self, desired_node_count: int) -> int:
        self._validate_node_count(
            desired_node_count,
            "desired_node_count",
        )

        desired_node_count = max(
            self.min_node_count,
            min(desired_node_count, self.max_node_count),
        )

        active_nodes = self.get_active_nodes()
        current_count = len(active_nodes)

        logger.info(
            "Cluster scaling requested: desired=%d, active=%d.",
            desired_node_count,
            current_count,
        )

        if current_count > desired_node_count:
            for node in active_nodes[desired_node_count:]:
                self.drain_worker(node.node_id)

        for node in self.registry.list_all():
            if (
                node.status == NodeStatus.DRAINING
                and node.active_tasks == 0
            ):
                self.drain_worker(node.node_id)

        remaining_count = len(self.get_active_nodes())

        while (
            remaining_count < desired_node_count
            and self.worker_factory is not None
        ):
            node = self.worker_factory()

            if not isinstance(node, NodeInfo):
                raise TypeError(
                    "worker_factory must return a NodeInfo instance."
                )

            if self.registry.get(node.node_id) is not None:
                raise ValueError(
                    f"worker_factory returned an already registered node: "
                    f"{node.node_id}"
                )

            self.registration_service.register_worker(node)
            updated_count = len(self.get_active_nodes())

            if updated_count <= remaining_count:
                logger.warning(
                    "Worker factory did not increase the active node count."
                )
                break

            remaining_count = updated_count

        if remaining_count < desired_node_count:
            logger.warning(
                "Desired worker count is %d, but only %d workers "
                "are active. Configure worker_factory to provision "
                "additional workers.",
                desired_node_count,
                remaining_count,
            )

        return remaining_count

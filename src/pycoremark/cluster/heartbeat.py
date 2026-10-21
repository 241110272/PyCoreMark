
"""Heartbeat monitoring with sequence, load, latency, and jitter tracking."""

from collections import deque
from datetime import datetime, timezone
from math import isfinite
from threading import RLock
from time import monotonic
from typing import Deque, Dict, List

from pycoremark.cluster.registry import NodeRegistry
from pycoremark.common.logging import get_logger
from pycoremark.models.node import Heartbeat, NodeStatus

logger = get_logger("cluster.heartbeat")


def _as_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


class HeartbeatService:

    def __init__(
        self,
        registry: NodeRegistry,
        window_size: int = 20,
    ) -> None:
        if (
            isinstance(window_size, bool)
            or not isinstance(window_size, int)
            or window_size < 1
        ):
            raise ValueError("window_size must be a positive integer.")

        self._registry = registry
        self._lock = RLock()
        self._window_size = window_size
        self._last_sequence: Dict[str, int] = {}
        self._registration_times: Dict[str, datetime] = {}
        self._load_history: Dict[str, Deque[float]] = {}
        self._latency_history: Dict[str, Deque[float]] = {}
        self._jitter_history: Dict[str, Deque[float]] = {}
        self._last_received_monotonic: Dict[str, float] = {}
        self._last_sent_timestamp: Dict[str, datetime] = {}
        self._jitter_estimate: Dict[str, float] = {}

    def _reset_node_history(self, node_id: str) -> None:
        self._last_sequence.pop(node_id, None)
        self._registration_times.pop(node_id, None)
        self._load_history.pop(node_id, None)
        self._latency_history.pop(node_id, None)
        self._jitter_history.pop(node_id, None)
        self._last_received_monotonic.pop(node_id, None)
        self._last_sent_timestamp.pop(node_id, None)
        self._jitter_estimate.pop(node_id, None)

    def process_heartbeat(self, heartbeat: Heartbeat) -> bool:
        if (
            isinstance(heartbeat.sequence_number, bool)
            or not isinstance(heartbeat.sequence_number, int)
            or heartbeat.sequence_number < 0
        ):
            logger.warning(
                "Ignoring heartbeat with invalid sequence number from node %s.",
                heartbeat.node_id,
            )
            return False

        if (
            isinstance(heartbeat.active_tasks, bool)
            or not isinstance(heartbeat.active_tasks, int)
            or heartbeat.active_tasks < 0
        ):
            logger.warning(
                "Ignoring heartbeat with invalid active task count from node %s.",
                heartbeat.node_id,
            )
            return False

        if (
            isinstance(heartbeat.current_load, bool)
            or not isinstance(heartbeat.current_load, (int, float))
            or not isfinite(heartbeat.current_load)
            or heartbeat.current_load < 0
        ):
            logger.warning(
                "Ignoring heartbeat with invalid load from node %s.",
                heartbeat.node_id,
            )
            return False

        node = self._registry.get(heartbeat.node_id)

        if node is None:
            logger.warning(
                "Received heartbeat from unknown node: %s",
                heartbeat.node_id,
            )
            return False

        received_at = datetime.now(timezone.utc)
        received_monotonic = monotonic()
        sent_at = _as_utc(heartbeat.timestamp)
        node_id = heartbeat.node_id

        with self._lock:
            registration_time = _as_utc(node.registered_at)
            previous_registration = self._registration_times.get(node_id)

            if (
                previous_registration is not None
                and registration_time != previous_registration
            ):
                self._reset_node_history(node_id)
                logger.info(
                    "Reset heartbeat history for re-registered node %s.",
                    node_id,
                )

            self._registration_times[node_id] = registration_time

            previous_sequence = self._last_sequence.get(node_id)

            if (
                previous_sequence is not None
                and heartbeat.sequence_number <= previous_sequence
            ):
                logger.warning(
                    "Ignoring duplicate or stale heartbeat from node %s.",
                    node_id,
                )
                return False

            previous_status = node.status
            node.last_heartbeat_at = sent_at
            node.active_tasks = heartbeat.active_tasks

            if node.status != NodeStatus.DRAINING:
                node.status = heartbeat.status

            self._registry.update_node(node)
            self._last_sequence[node_id] = heartbeat.sequence_number

            load_history = self._load_history.setdefault(
                node_id,
                deque(maxlen=self._window_size),
            )
            load_history.append(float(heartbeat.current_load))

            latency = (received_at - sent_at).total_seconds()

            if latency < 0:
                logger.warning(
                    "Heartbeat timestamp from node %s is ahead "
                    "of the coordinator clock.",
                    node_id,
                )
                latency = 0.0

            latency_history = self._latency_history.setdefault(
                node_id,
                deque(maxlen=self._window_size),
            )
            latency_history.append(latency)

            previous_received = self._last_received_monotonic.get(node_id)
            previous_sent = self._last_sent_timestamp.get(node_id)
            jitter = self._jitter_estimate.get(node_id, 0.0)

            if previous_received is not None and previous_sent is not None:
                sender_interval = (sent_at - previous_sent).total_seconds()
                receiver_interval = received_monotonic - previous_received

                if sender_interval >= 0:
                    jitter_sample = abs(receiver_interval - sender_interval)
                    jitter += (jitter_sample - jitter) / 16.0

            self._jitter_estimate[node_id] = jitter

            jitter_history = self._jitter_history.setdefault(
                node_id,
                deque(maxlen=self._window_size),
            )
            jitter_history.append(jitter)

            self._last_received_monotonic[node_id] = received_monotonic
            self._last_sent_timestamp[node_id] = sent_at

        if previous_status in (
            NodeStatus.SUSPECTED,
            NodeStatus.DEAD,
            NodeStatus.OFFLINE,
        ):
            logger.info(
                "Node %s received a heartbeat after status %s; reported status is %s.",
                node_id,
                previous_status,
                heartbeat.status,
            )

        return True

    def forget_node(self, node_id: str) -> None:
        with self._lock:
            self._reset_node_history(node_id)

    def get_load_history(self, node_id: str) -> List[float]:
        with self._lock:
            history = self._load_history.get(node_id)
            return list(history) if history is not None else []

    def get_latency_history(self, node_id: str) -> List[float]:
        with self._lock:
            history = self._latency_history.get(node_id)
            return list(history) if history is not None else []

    def get_jitter_history(self, node_id: str) -> List[float]:
        with self._lock:
            history = self._jitter_history.get(node_id)
            return list(history) if history is not None else []

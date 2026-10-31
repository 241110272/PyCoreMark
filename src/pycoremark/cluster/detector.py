
"""Cluster node failure detector using Phi Accrual and timeout fallback."""

from collections import deque
from datetime import datetime, timezone
from math import isfinite, log10
from statistics import NormalDist, fmean, pstdev
from threading import RLock
from time import monotonic
from typing import Deque, Dict, List, Optional
from uuid import uuid4

from pycoremark.cluster.registry import NodeRegistry
from pycoremark.common.logging import get_logger
from pycoremark.communication.events import EventPublisher
from pycoremark.models.event import ClusterEvent, ClusterEventType
from pycoremark.models.node import NodeInfo, NodeStatus

logger = get_logger("cluster.detector")


def _as_utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


class FailureDetector:

    def __init__(
        self,
        registry: NodeRegistry,
        timeout_seconds: float = 6.0,
        phi_threshold: float = 8.0,
        suspect_threshold: float = 2.0,
        min_samples: int = 5,
        history_size: int = 100,
        grace_period_seconds: float = 0.0,
        event_publisher: Optional[EventPublisher] = None,
        event_topic: str = "cluster.events",
    ) -> None:
        self._validate_positive("timeout_seconds", timeout_seconds)
        self._validate_positive("phi_threshold", phi_threshold)
        self._validate_positive("suspect_threshold", suspect_threshold)
        self._validate_nonnegative(
            "grace_period_seconds",
            grace_period_seconds,
        )

        if suspect_threshold >= phi_threshold:
            raise ValueError(
                "suspect_threshold must be less than phi_threshold."
            )

        if (
            isinstance(min_samples, bool)
            or not isinstance(min_samples, int)
            or min_samples < 2
        ):
            raise ValueError("min_samples must be an integer of at least 2.")

        if (
            isinstance(history_size, bool)
            or not isinstance(history_size, int)
            or history_size < min_samples
        ):
            raise ValueError("history_size must be at least min_samples.")

        if not isinstance(event_topic, str) or not event_topic.strip():
            raise ValueError("event_topic must be a non-empty string.")

        if event_publisher is not None and not callable(
            getattr(event_publisher, "publish", None)
        ):
            raise ValueError(
                "event_publisher must provide a callable publish method."
            )

        self._registry = registry
        self._timeout_seconds = float(timeout_seconds)
        self._phi_threshold = float(phi_threshold)
        self._suspect_threshold = float(suspect_threshold)
        self._min_samples = min_samples
        self._history_size = history_size
        self._grace_period_seconds = float(grace_period_seconds)
        self._event_publisher = event_publisher
        self._event_topic = event_topic.strip()

        self._lock = RLock()
        self._interval_history: Dict[str, Deque[float]] = {}
        self._previous_heartbeat_at: Dict[str, datetime] = {}
        self._previous_registration_at: Dict[str, datetime] = {}
        self._last_heartbeat_monotonic: Dict[str, float] = {}
        self._first_seen_monotonic: Dict[str, float] = {}

    @staticmethod
    def _validate_positive(name: str, value: float) -> None:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{name} must be a positive finite number.")

    @staticmethod
    def _validate_nonnegative(name: str, value: float) -> None:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(value)
            or value < 0
        ):
            raise ValueError(f"{name} must be a nonnegative finite number.")

    def _reset_node_history(self, node_id: str) -> None:
        self._interval_history.pop(node_id, None)
        self._previous_heartbeat_at.pop(node_id, None)
        self._previous_registration_at.pop(node_id, None)
        self._last_heartbeat_monotonic.pop(node_id, None)
        self._first_seen_monotonic.pop(node_id, None)

    def _observe_node(
        self,
        node: NodeInfo,
        now_monotonic: float,
        now_utc: datetime,
    ) -> float:
        node_id = node.node_id
        heartbeat_at = _as_utc(node.last_heartbeat_at)
        registration_at = _as_utc(node.registered_at)

        previous_registration = self._previous_registration_at.get(node_id)

        if (
            previous_registration is not None
            and registration_at != previous_registration
        ):
            self._reset_node_history(node_id)

        self._previous_registration_at[node_id] = registration_at
        previous_heartbeat = self._previous_heartbeat_at.get(node_id)

        if previous_heartbeat is None:
            self._previous_heartbeat_at[node_id] = heartbeat_at

            initial_age = max(
                0.0,
                (now_utc - heartbeat_at).total_seconds(),
            )
            self._last_heartbeat_monotonic[node_id] = (
                now_monotonic - initial_age
            )
            self._first_seen_monotonic[node_id] = now_monotonic

        elif heartbeat_at > previous_heartbeat:
            interval = (heartbeat_at - previous_heartbeat).total_seconds()
            history = self._interval_history.setdefault(
                node_id,
                deque(maxlen=self._history_size),
            )

            if interval > 0 and isfinite(interval):
                history.append(interval)

            self._previous_heartbeat_at[node_id] = heartbeat_at
            self._last_heartbeat_monotonic[node_id] = now_monotonic

        elif heartbeat_at < previous_heartbeat:
            self._reset_node_history(node_id)
            self._previous_registration_at[node_id] = registration_at
            self._previous_heartbeat_at[node_id] = heartbeat_at

            initial_age = max(
                0.0,
                (now_utc - heartbeat_at).total_seconds(),
            )
            self._last_heartbeat_monotonic[node_id] = (
                now_monotonic - initial_age
            )
            self._first_seen_monotonic[node_id] = now_monotonic

        last_seen = self._last_heartbeat_monotonic[node_id]
        return max(0.0, now_monotonic - last_seen)

    @staticmethod
    def _calculate_phi(
        elapsed_seconds: float,
        intervals: List[float],
    ) -> float:
        if not intervals:
            return 0.0

        average_interval = fmean(intervals)
        deviation = pstdev(intervals)

        if deviation < 1e-6:
            deviation = max(average_interval * 0.1, 1e-3)

        distribution = NormalDist(
            mu=average_interval,
            sigma=deviation,
        )

        probability_alive = max(
            1.0 - distribution.cdf(elapsed_seconds),
            1e-16,
        )

        return -log10(probability_alive)

    def _publish_failure_event(self, event: ClusterEvent) -> None:
        if self._event_publisher is None:
            return

        try:
            self._event_publisher.publish(self._event_topic, event)
        except Exception:
            logger.exception(
                "Failed to publish failure event for node %s.",
                event.source_node_id,
            )

    def check_liveness(self) -> List[str]:
        """Return node IDs newly marked SUSPECTED or DEAD."""

        nodes = self._registry.list_all()
        current_node_ids = {node.node_id for node in nodes}
        changed_nodes: List[str] = []
        pending_events: List[ClusterEvent] = []
        now_monotonic = monotonic()
        now_utc = datetime.now(timezone.utc)

        with self._lock:
            tracked_ids = (
                set(self._interval_history)
                | set(self._previous_heartbeat_at)
                | set(self._previous_registration_at)
                | set(self._last_heartbeat_monotonic)
                | set(self._first_seen_monotonic)
            )

            for node_id in tracked_ids - current_node_ids:
                self._reset_node_history(node_id)

            for node in nodes:
                if node.status in (NodeStatus.DEAD, NodeStatus.OFFLINE):
                    continue

                previous_status = node.status

                elapsed = self._observe_node(
                    node,
                    now_monotonic,
                    now_utc,
                )

                first_seen = self._first_seen_monotonic[node.node_id]

                if (
                    now_monotonic - first_seen
                    < self._grace_period_seconds
                ):
                    continue

                intervals = list(
                    self._interval_history.get(node.node_id, ())
                )

                if len(intervals) < self._min_samples:
                    if elapsed >= self._timeout_seconds:
                        target_status = NodeStatus.OFFLINE
                    elif elapsed >= self._timeout_seconds / 2:
                        target_status = NodeStatus.SUSPECTED
                    else:
                        continue
                else:
                    phi = self._calculate_phi(elapsed, intervals)

                    if phi >= self._phi_threshold:
                        target_status = NodeStatus.OFFLINE
                    elif phi >= self._suspect_threshold:
                        target_status = NodeStatus.SUSPECTED
                    else:
                        continue

                if previous_status == target_status:
                    continue

                self._registry.update_status(
                    node.node_id,
                    target_status,
                )
                changed_nodes.append(node.node_id)

                logger.warning(
                    "Node %s changed from %s to %s "
                    "(elapsed=%.2fs, intervals=%d).",
                    node.node_id,
                    previous_status.value,
                    target_status.value,
                    elapsed,
                    len(intervals),
                )

                if (
                    target_status == NodeStatus.OFFLINE
                    and self._event_publisher is not None
                ):
                    pending_events.append(
                        ClusterEvent(
                            event_id=uuid4().hex,
                            event_type=ClusterEventType.NODE_FAILED,
                            source_node_id=node.node_id,
                            payload={
                                "previous_status": previous_status.value,
                                "status": target_status.value,
                                "elapsed_seconds": round(elapsed, 6),
                                "heartbeat_intervals_observed": len(intervals),
                            },
                        )
                    )

        for event in pending_events:
            self._publish_failure_event(event)

        return changed_nodes

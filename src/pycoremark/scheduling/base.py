"""Shared scheduling contract, telemetry snapshots, and local reservations."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from threading import RLock
from typing import Dict, List, Optional

from pycoremark.models.metrics import MetricSample
from pycoremark.models.node import NodeInfo, NodeStatus
from pycoremark.models.task import Task


@dataclass
class SchedulingDecision:


    task_id: str
    target_node_id: Optional[str]
    reason: str = ""
    estimated_queue_delay_ms: float = 0.0


class BaseScheduler(ABC):


    def __init__(self, queue_capacity: int = 100, metrics_max_age_seconds: float = 30.0) -> None:
        self._validate_count(queue_capacity, "queue_capacity")
        self._validate_number(metrics_max_age_seconds, "metrics_max_age_seconds")
        if queue_capacity == 0 or metrics_max_age_seconds == 0:
            raise ValueError("Queue capacity and metrics age must be positive")
        self.queue_capacity = queue_capacity
        self.metrics_max_age_seconds = metrics_max_age_seconds
        self._lock = RLock()
        self._metrics: Dict[str, _NodeMetrics] = {}
        self._reservations: Dict[str, _Reservation] = {}

    @staticmethod
    def _validate_number(value: float, name: str) -> None:
        """Reject negative, nonnumeric, boolean, and nonfinite inputs."""
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{name} must be a finite nonnegative number")
        if not isfinite(value) or value < 0:
            raise ValueError(f"{name} must be a finite nonnegative number")

    @staticmethod
    def _validate_count(value: int, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a nonnegative integer")

    def update_metrics(
        self,
        sample: MetricSample,
        acknowledged_task_ids: Optional[List[str]] = None,
    ) -> None:
        if sample.timestamp.tzinfo is None or sample.timestamp.utcoffset() is None:
            raise ValueError("Metric timestamp must be timezone-aware")
        if sample.cpu_percent is not None:
            self._validate_number(sample.cpu_percent, "cpu_percent")
            if sample.cpu_percent > 100:
                raise ValueError("cpu_percent must be at most 100")
        self._validate_count(sample.active_tasks, "active_tasks")
        queue = sample.additional_metrics.get("queue_length", 0)
        capacity = sample.additional_metrics.get("queue_capacity", self.queue_capacity)
        self._validate_count(queue, "queue_length")
        self._validate_count(capacity, "queue_capacity")
        if capacity == 0:
            raise ValueError("queue_capacity must be positive")
        used, total = sample.memory_used_mb, sample.memory_total_mb
        for name, value in (("memory_used_mb", used), ("memory_total_mb", total)):
            if value is not None:
                self._validate_number(value, name)
        if used is not None and total is not None and used > total:
            raise ValueError("memory_used_mb exceeds memory_total_mb")
        snapshot = _NodeMetrics(
            sample.timestamp, sample.cpu_percent, used, total,
            sample.active_tasks, queue, capacity,
        )
        with self._lock:
            age = (datetime.now(timezone.utc) - sample.timestamp).total_seconds()
            if not 0 <= age <= self.metrics_max_age_seconds:
                return
            previous = self._metrics.get(sample.node_id)
            if previous is not None and sample.timestamp < previous.timestamp:
                return
            self._metrics[sample.node_id] = snapshot
            for task_id in acknowledged_task_ids or []:
                reservation = self._reservations.get(task_id)
                if reservation is not None and reservation.node_id == sample.node_id:
                    reservation.acknowledged = True

    def release_task(self, task_id: str) -> bool:

        with self._lock:
            return self._reservations.pop(task_id, None) is not None

    def _metrics_for(self, node: NodeInfo) -> Optional["_NodeMetrics"]:
        metrics = self._metrics.get(node.node_id)
        if metrics is not None:
            age = (datetime.now(timezone.utc) - metrics.timestamp).total_seconds()
            if 0 <= age <= self.metrics_max_age_seconds:
                return metrics
        return None

    def _pending(self, node: NodeInfo) -> List["_Reservation"]:
        return [r for r in self._reservations.values()
                if r.node_id == node.node_id and not r.acknowledged]

    def _queue(self, node: NodeInfo) -> tuple[int, int]:
        metrics = self._metrics_for(node)
        # When telemetry expires, keep acknowledged outstanding work in the
        # local load estimate rather than silently treating that node as idle.
        pending_count = len(self._pending(node)) if metrics else sum(
            reservation.node_id == node.node_id
            for reservation in self._reservations.values()
        )
        return (
            (metrics.queue_length if metrics else 0) + pending_count,
            metrics.queue_capacity if metrics else self.queue_capacity,
        )

    def _load(self, node: NodeInfo) -> int:
        metrics = self._metrics_for(node)
        return (metrics.active_tasks if metrics else 0) + self._queue(node)[0]

    def schedule(self, task: Task, available_nodes: List[NodeInfo]) -> SchedulingDecision:

        self._validate_count(task.required_cores, "required_cores")
        self._validate_count(task.required_memory_mb, "required_memory_mb")
        with self._lock:
            if task.task_id in self._reservations:
                raise ValueError(f"Task {task.task_id!r} already has a reservation")
            candidates = []
            seen = set()
            for node in available_nodes:
                if node.node_id in seen:
                    continue
                seen.add(node.node_id)
                queued, capacity = self._queue(node)
                resources = node.resources
                task_fits = (
                    task.required_cores <= resources.total_cores
                    and task.required_memory_mb <= resources.total_memory_mb
                )
                if (
                    task_fits
                    and node.status in (NodeStatus.READY, NodeStatus.BUSY)
                    and queued < capacity
                ):
                    candidates.append(node)
            target = self._select_node(task, candidates)
            if target is None:
                return SchedulingDecision(
                    task.task_id, None,
                    "No available node: unavailable, queue full, or insufficient resources",
                )
            self._reservations[task.task_id] = _Reservation(
                target.node_id, task.required_cores, task.required_memory_mb,
            )
            return SchedulingDecision(
                task.task_id, target.node_id, f"{type(self).__name__} allocation",
            )

    def schedule_batch(
        self, tasks: List[Task], available_nodes: List[NodeInfo],
    ) -> List[SchedulingDecision]:

        with self._lock:
            task_ids = [task.task_id for task in tasks]
            if len(set(task_ids)) != len(task_ids):
                raise ValueError("Batch contains duplicate task IDs")
            if any(task_id in self._reservations for task_id in task_ids):
                raise ValueError("Batch contains an already reserved task")
            for task in tasks:
                self._validate_count(task.required_cores, "required_cores")
                self._validate_count(task.required_memory_mb, "required_memory_mb")
            return [self.schedule(task, available_nodes) for task in tasks]

    @abstractmethod
    def _select_node(self, task: Task, candidates: List[NodeInfo]) -> Optional[NodeInfo]:
        """Choose among available, non-full nodes while holding the lock."""
        raise NotImplementedError


@dataclass(frozen=True)
class _NodeMetrics:
    timestamp: datetime
    cpu_percent: Optional[float]
    memory_used_mb: Optional[float]
    memory_total_mb: Optional[float]
    active_tasks: int
    queue_length: int
    queue_capacity: int


@dataclass
class _Reservation:
    node_id: str
    cores: int
    memory_mb: int
    acknowledged: bool = False

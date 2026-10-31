
"""Worker task execution engine."""

import time
import math
import random
from datetime import datetime, timezone
from threading import Lock
from uuid import uuid4

from pycoremark.common.logging import get_logger
from pycoremark.fault_tolerance.idempotency import InMemoryIdempotencyStore
from pycoremark.models.task import Task, TaskAttempt, TaskResult, TaskStatus
from pycoremark.workloads.registry import WorkloadRegistry

logger = get_logger("worker.executor")


class TaskExecutor:
    """Executes tasks on a worker node."""

    def __init__(
        self,
        node_id: str,
        workload_registry: WorkloadRegistry,
        *,
        failure_rate: float = 0.0,
        artificial_delay_ms: float = 0.0,
    ) -> None:
        if (
            isinstance(failure_rate, bool)
            or not isinstance(failure_rate, (int, float))
            or not math.isfinite(failure_rate)
            or not 0 <= failure_rate <= 1
        ):
            raise ValueError("failure_rate must be in [0, 1]")
        if (
            isinstance(artificial_delay_ms, bool)
            or not isinstance(artificial_delay_ms, (int, float))
            or not math.isfinite(artificial_delay_ms)
            or artificial_delay_ms < 0
        ):
            raise ValueError("artificial_delay_ms must be finite and nonnegative")
        self.node_id = node_id
        self.workload_registry = workload_registry
        self.failure_rate = float(failure_rate)
        self.artificial_delay_ms = float(artificial_delay_ms)
        self._idempotency = InMemoryIdempotencyStore()
        self._task_locks: dict[str, Lock] = {}
        self._task_locks_lock = Lock()

    def _lock_for_task(self, task_id: str) -> Lock:
        with self._task_locks_lock:
            return self._task_locks.setdefault(task_id, Lock())

    def execute(self, task: Task) -> TaskResult:
        """Execute a task and return its result."""
        with self._lock_for_task(task.task_id):
            if self._idempotency.has_seen(task.task_id):
                cached = self._idempotency.get_result(task.task_id)
                if cached is not None:
                    return cached

            logger.info("Executing task %s on node %s", task.task_id, self.node_id)
            started = time.perf_counter()
            started_at = datetime.now(timezone.utc)
            attempt_number = task.retry_count + 1
            span_id = uuid4().hex
            attempt = TaskAttempt(
                attempt_number=attempt_number,
                worker_id=self.node_id,
                started_at=started_at,
                trace_id=task.trace_id,
            )
            task.attempts.append(attempt)
            queue_wait = (
                max(0.0, (started_at - task.queued_at).total_seconds())
                if task.queued_at is not None else 0.0
            )

            try:
                if self.artificial_delay_ms:
                    time.sleep(self.artificial_delay_ms / 1000.0)
                if random.random() < self.failure_rate:
                    attempt.finished_at = datetime.now(timezone.utc)
                    attempt.error_message = "Injected worker task failure"
                    return TaskResult(
                        task_id=task.task_id,
                        worker_id=self.node_id,
                        status=TaskStatus.FAILED,
                        error_message=attempt.error_message,
                        execution_time_seconds=round(time.perf_counter() - started, 6),
                        completed_at=attempt.finished_at,
                        queue_wait_seconds=queue_wait,
                        attempt_number=attempt_number,
                        trace_id=task.trace_id,
                        span_id=span_id,
                        parent_span_id=task.parent_span_id,
                        started_at=started_at,
                    )
                workload = self.workload_registry.create_for_task(task)
                output = workload.execute_task(task)
                duration = round(time.perf_counter() - started, 6)
                attempt.finished_at = datetime.now(timezone.utc)
                result = TaskResult(
                    task_id=task.task_id,
                    worker_id=self.node_id,
                    status=TaskStatus.COMPLETED,
                    output_data=output if isinstance(output, dict) else {"result": output},
                    execution_time_seconds=duration,
                    completed_at=attempt.finished_at,
                    queue_wait_seconds=queue_wait,
                    attempt_number=attempt_number,
                    trace_id=task.trace_id,
                    span_id=span_id,
                    parent_span_id=task.parent_span_id,
                    started_at=started_at,
                )
                self._idempotency.record(task.task_id, result)
                return result
            except Exception as exc:
                duration = round(time.perf_counter() - started, 6)
                attempt.finished_at = datetime.now(timezone.utc)
                attempt.error_message = str(exc)
                logger.exception("Task %s failed", task.task_id)
                return TaskResult(
                    task_id=task.task_id,
                    worker_id=self.node_id,
                    status=TaskStatus.FAILED,
                    error_message=str(exc),
                    execution_time_seconds=duration,
                    completed_at=attempt.finished_at,
                    queue_wait_seconds=queue_wait,
                    attempt_number=attempt_number,
                    trace_id=task.trace_id,
                    span_id=span_id,
                    parent_span_id=task.parent_span_id,
                    started_at=started_at,
                )

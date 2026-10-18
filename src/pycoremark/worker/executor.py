
"""Worker task execution engine."""

import time
from datetime import datetime, timezone

from pycoremark.common.logging import get_logger
from pycoremark.models.task import Task, TaskResult, TaskStatus
from pycoremark.workloads.registry import WorkloadRegistry

logger = get_logger("worker.executor")


class TaskExecutor:
    """Executes tasks on a worker node."""

    def __init__(self, node_id: str, workload_registry: WorkloadRegistry) -> None:
        self.node_id = node_id
        self.workload_registry = workload_registry

    def execute(self, task: Task) -> TaskResult:
        """Execute a task and return its result."""
        logger.info("Executing task %s on node %s", task.task_id, self.node_id)
        started = time.perf_counter()

        try:
            workload = self.workload_registry.get(task.workload_name)

            # Support workload implementations that expose a run() method
            # or are directly callable. Adapt this section if the project's
            # workload interface specifies a different execution contract.
            if hasattr(workload, "run") and callable(workload.run):
                output = workload.run()
            elif callable(workload):
                output = workload()
            else:
                raise TypeError(
                    f"Workload '{task.workload_name}' has no callable run() method"
                )

            duration = round(time.perf_counter() - started, 4)
            return TaskResult(
                task_id=task.task_id,
                worker_id=self.node_id,
                status=TaskStatus.COMPLETED,
                output_data={"result": output},
                execution_time_seconds=duration,
                completed_at=datetime.now(timezone.utc),
            )

        except Exception as exc:
            duration = round(time.perf_counter() - started, 4)
            logger.exception("Task %s failed", task.task_id)

            return TaskResult(
                task_id=task.task_id,
                worker_id=self.node_id,
                status=TaskStatus.FAILED,
                error_message=str(exc),
                execution_time_seconds=duration,
                completed_at=datetime.now(timezone.utc),
            )

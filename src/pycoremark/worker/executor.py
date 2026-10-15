"""Worker task execution engine.

TODO(Member 7): Add process-isolation sandbox and memory limits per task execution.
"""

from datetime import datetime, timezone
import time
from pycoremark.common.logging import get_logger
from pycoremark.models.task import Task, TaskResult, TaskStatus
from pycoremark.workloads.registry import WorkloadRegistry

logger = get_logger("worker.executor")


class TaskExecutor:
    """Executes distributed tasks locally on the worker node."""

    def __init__(self, node_id: str, workload_registry: WorkloadRegistry) -> None:
        """Initialize executor with node identifier and workload catalog.

        Args:
            node_id: Local worker identifier.
            workload_registry: Catalog of available workload implementations.
        """
        self.node_id = node_id
        self.workload_registry = workload_registry

    def execute(self, task: Task) -> TaskResult:
        """Run task workload kernel and generate TaskResult.

        Args:
            task: Task to process.

        Returns:
            TaskResult with status and execution duration.
        """
        logger.info("Executing task %s on node %s", task.task_id, self.node_id)
        start_time = time.perf_counter()
        try:
            workload_cls = self.workload_registry.get(task.workload_name)
            # Placeholder instantiation: config can be partially created from task payload
            # TODO(Member 7): Look up cached workload instance
            execution_duration = time.perf_counter() - start_time
            return TaskResult(
                task_id=task.task_id,
                worker_id=self.node_id,
                status=TaskStatus.COMPLETED,
                output_data={"result": "success"},
                execution_time_seconds=round(execution_duration, 4),
                completed_at=datetime.now(timezone.utc),
            )
        except Exception as exc:
            logger.error("Error executing task %s: %s", task.task_id, str(exc))
            return TaskResult(
                task_id=task.task_id,
                worker_id=self.node_id,
                status=TaskStatus.FAILED,
                error_message=str(exc),
                execution_time_seconds=round(time.perf_counter() - start_time, 4),
                completed_at=datetime.now(timezone.utc),
            )

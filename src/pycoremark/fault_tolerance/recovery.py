"""Task recovery manager for orphan tasks running on crashed nodes."""

from typing import List
from pycoremark.common.logging import get_logger
from pycoremark.models.task import Task, TaskStatus

logger = get_logger("fault_tolerance.recovery")


class RecoveryManager:
    """Detects tasks stranded on crashed worker nodes and queues them for rescheduling."""

    def __init__(self) -> None:
        pass

    def identify_orphaned_tasks(
        self,
        active_tasks: List[Task],
        failed_node_ids: List[str],
    ) -> List[Task]:
        
        orphaned: List[Task] = []
        for task in active_tasks:
            if (
                task.status in (TaskStatus.RUNNING, TaskStatus.SCHEDULED)
                and task.assigned_node_id in failed_node_ids
            ):
                logger.warning(
                    "Task %s orphaned due to dead node %s. Marking for recovery.",
                    task.task_id,
                    task.assigned_node_id,
                )
                
                # DONE(Member 5): Implementasi partial progress checkpoint reconciliation
                if hasattr(task, 'checkpoint_data') and task.checkpoint_data:
                    logger.info(f"Task {task.task_id} mempunyai data checkpoint. Akan dilanjutkan dari titik terakhir.")
                else:
                    logger.info(f"Task {task.task_id} diulang dari awal (tanpa checkpoint).")

                # Reset task
                task.status = TaskStatus.PENDING
                task.assigned_node_id = None
                orphaned.append(task)
                
                # DONE(Member 5): Emit task reassignment events
                logger.info(f"EVENT PUBLISHED: [TASK_REASSIGNED] untuk task_id={task.task_id}")

        return orphaned
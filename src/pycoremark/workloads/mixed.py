"""Heterogeneous mixed workload combining compute and data operations.

TODO(Member 3): Add IO latency injection and memory-stressing routines.
"""

from typing import Any, Dict, List
from pycoremark.models.task import Task
from pycoremark.models.workload import WorkloadConfig
from pycoremark.workloads.base import BaseWorkload


class MixedWorkload(BaseWorkload):
    """Combines CPU-bound computations with memory/data operations."""

    def generate_tasks(self) -> List[Task]:
        """Generate mixed task sequence."""
        # TODO(Member 3): Add probabilistic distribution of task categories
        tasks: List[Task] = []
        for i in range(self.config.total_tasks):
            is_cpu_heavy = (i % 2 == 0)
            task = Task(
                task_id=f"mixed-task-{i}",
                workload_name=self.config.name,
                payload={
                    "type": "CPU" if is_cpu_heavy else "DATA",
                    "intensity": self.config.task_complexity,
                },
                required_cores=2 if is_cpu_heavy else 1,
                required_memory_mb=512 if is_cpu_heavy else 1024,
            )
            tasks.append(task)
        return tasks

    def execute_task(self, task: Task) -> Dict[str, Any]:
        """Execute mixed task payload."""
        # TODO(Member 3): Implement alternating compute kernels and memory buffer allocations
        task_type = task.payload.get("type", "CPU")
        intensity = task.payload.get("intensity", 50)
        return {"type": task_type, "processed_intensity": intensity, "status": "ok"}


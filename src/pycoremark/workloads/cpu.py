"""CPU-intensive synthetic benchmark workload.

TODO(Member 3): Implement realistic mathematical benchmarks (matrix multiplication, hashing, Monte Carlo).
"""

import math
from typing import Any, Dict, List
from pycoremark.models.task import Task
from pycoremark.models.workload import WorkloadConfig
from pycoremark.workloads.base import BaseWorkload


class CPUWorkload(BaseWorkload):
    """Generates and executes compute-heavy CPU-bound benchmark tasks."""

    def generate_tasks(self) -> List[Task]:
        """Generate CPU-bound calculation tasks based on config."""
        # TODO(Member 3): Support variable complexity distributions (e.g. Pareto, uniform)
        tasks: List[Task] = []
        for i in range(self.config.total_tasks):
            task = Task(
                task_id=f"cpu-task-{i}",
                workload_name=self.config.name,
                payload={"iterations": self.config.task_complexity, "index": i},
                required_cores=1,
                required_memory_mb=256,
            )
            tasks.append(task)
        return tasks

    def execute_task(self, task: Task) -> Dict[str, Any]:
        """Execute compute task (placeholder prime calculation or math loop)."""
        # TODO(Member 3): Replace dummy calculation with configurable mathematical kernel
        iterations = task.payload.get("iterations", 100)
        checksum = 0.0
        for i in range(1, iterations + 1):
            checksum += math.sqrt(i)
        return {"checksum": checksum, "computed_iterations": iterations}


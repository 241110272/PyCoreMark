"""Partitioned prime-number benchmark — Filbert Matthew, Anggota 2."""

from math import isqrt
from typing import Any, Dict, List
from uuid import uuid4

from pycoremark.models.task import Task
from pycoremark.workloads.base import BaseWorkload
from pycoremark.workloads.partitioner import DataPartitioner


class CPUWorkload(BaseWorkload):
    """Count and sum primes in disjoint intervals; payloads are self-contained."""

    def generate_tasks(self) -> List[Task]:
        start = self.config.parameters.get("start", 0)
        stop = self.config.parameters.get(
            "stop", start + self.config.total_tasks * self.config.task_complexity
        )
        if start < 0 or stop < start:
            raise ValueError("Expected 0 <= start <= stop")
        if self.config.task_complexity < 0:
            raise ValueError("task_complexity must be >= 0")
        ranges = DataPartitioner.partition_range(stop - start, self.config.total_tasks)
        run_id = uuid4().hex
        return [Task(
            task_id=f"cpu-{run_id}-{index}",
            workload_name=self.config.name,
            payload={"start": start + left, "stop": start + right},
            required_cores=1,
            required_memory_mb=16,
        ) for index, (left, right) in enumerate(ranges)]

    def execute_task(self, task: Task) -> Dict[str, Any]:
        """Use integer trial division; bounds follow Python's [start, stop) convention."""
        start, stop = task.payload["start"], task.payload["stop"]
        count = total = 0
        for number in range(max(2, start), stop):
            if number == 2 or (
                number % 2 and all(number % divisor for divisor in range(3, isqrt(number) + 1, 2))
            ):
                count += 1
                total += number
        return {"prime_count": count, "prime_sum": total,
                "processed_numbers": stop - start}

    @staticmethod
    def aggregate_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Reduce accepted partial results; result deduplication belongs to coordinator."""
        return {key: sum(result[key] for result in results)
                for key in ("prime_count", "prime_sum", "processed_numbers")}

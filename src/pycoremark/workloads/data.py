"""Partitioned word count — Filbert Matthew, Anggota 2."""

from collections import Counter
from pathlib import Path
import re
from typing import Any, Dict, List
from uuid import uuid4

from pycoremark.models.task import Task
from pycoremark.workloads.base import BaseWorkload
from pycoremark.workloads.partitioner import DataPartitioner


class PartitionedDataWorkload(BaseWorkload):
    """Map complete text records to word counts; reduce without depending on arrival order."""

    def generate_tasks(self) -> List[Task]:
        """Load UTF-8 lines or inline records and include each partition in its payload."""
        params = self.config.parameters
        if "dataset_path" in params:
            records = Path(params["dataset_path"]).read_text(encoding="utf-8").splitlines()
        elif "records" in params:
            records = params["records"]
            if not isinstance(records, list) or not all(isinstance(row, str) for row in records):
                raise ValueError("records must be a list of strings")
        else:
            if self.config.dataset_size_records < 0:
                raise ValueError("dataset_size_records must be >= 0")
            records = [f"record {index} pycoremark" for index in range(self.config.dataset_size_records)]
        partitions = DataPartitioner.partition_items(records, self.config.partitions)
        run_id = uuid4().hex
        return [Task(
            task_id=f"data-{run_id}-{index}",
            workload_name=self.config.name,
            payload={"partition_id": index, "records": chunk},
            required_cores=1,
            required_memory_mb=64,
        ) for index, chunk in enumerate(partitions)]

    def execute_task(self, task: Task) -> Dict[str, Any]:
        """Case-insensitive Unicode word count; record boundaries never split tokens."""
        counts: Counter[str] = Counter()
        records = task.payload["records"]
        for record in records:
            counts.update(re.findall(r"\w+", record.casefold()))
        return {"processed_records": len(records), "word_counts": dict(sorted(counts.items()))}

    @staticmethod
    def aggregate_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Combine map outputs; coordinator must pass only accepted unique results."""
        counts: Counter[str] = Counter()
        processed = 0
        for result in results:
            counts.update(result["word_counts"])
            processed += result["processed_records"]
        return {"processed_records": processed, "word_counts": dict(sorted(counts.items()))}

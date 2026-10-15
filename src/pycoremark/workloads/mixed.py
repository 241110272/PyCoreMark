"""File, compute, and HTTP pipeline — Filbert Matthew, Anggota 2."""

import hashlib
from itertools import islice
import json
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

from pycoremark.models.task import Task
from pycoremark.workloads.base import BaseWorkload
from pycoremark.workloads.partitioner import DataPartitioner


class MixedWorkload(BaseWorkload):
    """Read shared UTF-8 partitions, transform/hash, save locally, POST a summary."""

    def generate_tasks(self) -> List[Task]:
        params = self.config.parameters
        dataset = str(Path(params["dataset_path"]).resolve())
        endpoint = params["result_url"]
        if urlsplit(endpoint).scheme not in ("http", "https"):
            raise ValueError("result_url must use http or https")
        rounds = self.config.task_complexity
        timeout = params.get("network_timeout_seconds", 10)
        if rounds < 1 or timeout <= 0:
            raise ValueError("task_complexity and network_timeout_seconds must be > 0")
        with open(dataset, encoding="utf-8") as source:
            count = sum(1 for _ in source)
        ranges = DataPartitioner.partition_range(count, self.config.partitions)
        run_id = uuid4().hex
        return [Task(
            task_id=f"mixed-{run_id}-{index}",
            workload_name=self.config.name,
            payload={"dataset_path": dataset, "start": start, "stop": stop,
                     "hash_rounds": rounds, "result_url": endpoint,
                     "output_dir": str(params["output_dir"]), "timeout": timeout},
            required_cores=1,
            required_memory_mb=64,
        ) for index, (start, stop) in enumerate(ranges)]

    def execute_task(self, task: Task) -> Dict[str, Any]:
        """Return only after HTTP acceptance; file/network errors propagate to caller."""
        payload = task.payload
        directory = Path(payload["output_dir"])
        directory.mkdir(parents=True, exist_ok=True)
        # Hash the ID so even remotely supplied IDs cannot escape output_dir.
        output = directory / (hashlib.sha256(task.task_id.encode()).hexdigest() + ".jsonl")
        partition_hash = hashlib.sha256()
        count = 0
        with open(payload["dataset_path"], encoding="utf-8") as source, output.open(
            "w", encoding="utf-8", newline="\n"
        ) as target:
            for line in islice(source, payload["start"], payload["stop"]):
                transformed = " ".join(line.casefold().split())
                digest = transformed.encode("utf-8")
                for _ in range(payload["hash_rounds"]):
                    digest = hashlib.sha256(digest).digest()
                row = json.dumps({"text": transformed, "sha256": digest.hex()},
                                 ensure_ascii=False, sort_keys=True) + "\n"
                target.write(row)
                partition_hash.update(row.encode("utf-8"))
                count += 1
        summary = {"task_id": task.task_id, "processed_records": count,
                   "checksum": partition_hash.hexdigest(), "output_path": str(output),
                   "output_bytes": output.stat().st_size}
        body = json.dumps(summary, sort_keys=True).encode("utf-8")
        request = Request(payload["result_url"], data=body,
                          headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(request, timeout=payload["timeout"]) as response:
            if not 200 <= response.status < 300:
                raise OSError(f"Result endpoint rejected task: HTTP {response.status}")
        return summary

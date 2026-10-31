"""Sequential baseline execution and canonical output checksums."""

from dataclasses import dataclass
import hashlib
import json
import time


@dataclass(frozen=True)
class SequentialBaseline:
    duration_seconds: float
    checksum: str
    output: object


def consistency_checksum(value: object) -> str:
    """Hash JSON-compatible results deterministically across task order."""
    def normalize(item):
        if isinstance(item, dict):
            return {str(key): normalize(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [normalize(child) for child in item]
        if isinstance(item, float) and item.is_integer():
            return int(item)
        return item

    canonical = json.dumps(
        normalize(value), sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def run_sequential_baseline(workload) -> SequentialBaseline:
    """Execute the same partitions sequentially and return their reduction."""
    tasks = workload.generate_tasks()
    started = time.perf_counter()
    partials = [workload.execute_task(task) for task in tasks]
    duration = time.perf_counter() - started
    reducer = getattr(workload, "aggregate_results", None)
    output = reducer(partials) if callable(reducer) else partials
    return SequentialBaseline(duration, consistency_checksum(output), output)


def measure_sequential_baseline(workload) -> float:
    """Run the workload's generated tasks sequentially and return wall time.

    The generated baseline tasks have the same partitioning and payload shape
    as the distributed run. Their outputs are intentionally discarded.
    """
    return run_sequential_baseline(workload).duration_seconds

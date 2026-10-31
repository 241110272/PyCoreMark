"""Fixed-window, real multi-process CPU scaling and consistency benchmark."""

from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import multiprocessing
import time
from typing import Iterable


@dataclass(frozen=True)
class CoreScalingSample:
    workers: int
    requested_duration_seconds: float
    actual_duration_seconds: float
    duration_overshoot_seconds: float
    longest_task_batch_seconds: float
    rounds_completed: int
    tasks_completed: int
    throughput_tasks_per_second: float
    speedup: float
    efficiency: float
    processed_numbers: int
    prime_count_per_round: int
    prime_sum_per_round: int
    expected_checksum: str
    output_matches_sequential: bool

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class CoreScalingReport:
    test_numbers: int
    task_count_per_round: int
    task_complexity: int
    samples: tuple[CoreScalingSample, ...]
    consistency_valid: bool

    def to_dict(self) -> dict:
        return {
            "test_numbers": self.test_numbers,
            "task_count_per_round": self.task_count_per_round,
            "task_complexity": self.task_complexity,
            "consistency_valid": self.consistency_valid,
            "samples": [sample.to_dict() for sample in self.samples],
        }


def _prime_range(bounds: tuple[int, int]) -> dict[str, int]:
    """Picklable worker function; each process owns a disjoint half-open range."""
    from math import isqrt

    start, stop = bounds
    count = total = 0
    for number in range(max(2, start), stop):
        if number == 2 or (
            number % 2
            and all(number % divisor for divisor in range(3, isqrt(number) + 1, 2))
        ):
            count += 1
            total += number
    return {
        "prime_count": count,
        "prime_sum": total,
        "processed_numbers": stop - start,
    }


def _reduce(partials: Iterable[dict[str, int]]) -> dict[str, int]:
    values = list(partials)
    return {
        name: sum(part[name] for part in values)
        for name in ("prime_count", "prime_sum", "processed_numbers")
    }


def _checksum(value: dict[str, int]) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def benchmark_cpu_scaling(
    *,
    duration_seconds: float = 2.0,
    worker_counts: Iterable[int] = (1, 2, 4),
    task_count: int = 16,
    task_complexity: int = 10_000,
    maximum_workers: int = 32,
) -> CoreScalingReport:
    """Run identical prime partitions for a minimum wall-time per worker count.

    One worker is the sequential single-core reference. Counts above one use
    separate OS processes (not Python threads), so CPU-bound work can execute
    on multiple CPU cores. Every iteration's reduced result is checked against
    the sequential result. The actual sample lasts at least the requested
    duration; any overshoot is bounded by its final task-batch duration.
    """
    if (
        isinstance(duration_seconds, bool)
        or not isinstance(duration_seconds, (int, float))
        or not math.isfinite(duration_seconds)
        or not 0.05 <= duration_seconds <= 300
    ):
        raise ValueError("duration_seconds must be between 0.05 and 300 seconds")
    for value, name, minimum, maximum in (
        (task_count, "task_count", 1, 256),
        (task_complexity, "task_complexity", 1, 5_000_000),
        (maximum_workers, "maximum_workers", 1, 64),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
            raise ValueError(f"{name} must be an integer in [{minimum}, {maximum}]")
    requested_workers = list(dict.fromkeys(worker_counts))
    if not requested_workers or 1 not in requested_workers:
        raise ValueError("worker_counts must include the single-core baseline (1)")
    if any(
        isinstance(workers, bool)
        or not isinstance(workers, int)
        or not 1 <= workers <= maximum_workers
        for workers in requested_workers
    ):
        raise ValueError(f"worker_counts must be integers from 1 to {maximum_workers}")

    total_numbers = task_count * task_complexity
    if total_numbers > 20_000_000:
        raise ValueError("task_count * task_complexity cannot exceed 20,000,000 numbers")
    partitions = [
        (index * task_complexity, (index + 1) * task_complexity)
        for index in range(task_count)
    ]
    expected = _reduce(_prime_range(bounds) for bounds in partitions)
    expected_checksum = _checksum(expected)
    ordered_workers = [1] + [workers for workers in requested_workers if workers != 1]
    samples: list[CoreScalingSample] = []
    single_core_throughput = 0.0
    all_consistent = True

    for workers in ordered_workers:
        executor = None
        if workers > 1:
            executor = ProcessPoolExecutor(
                max_workers=workers,
                mp_context=multiprocessing.get_context("spawn"),
            )
            # Exclude process startup from the measured window.
            list(executor.map(_prime_range, [(0, 2)] * workers))

        rounds = 0
        max_round_seconds = 0.0
        started = time.perf_counter()
        consistent = True
        try:
            while True:
                round_started = time.perf_counter()
                if executor is None:
                    partials = [_prime_range(bounds) for bounds in partitions]
                else:
                    partials = list(executor.map(_prime_range, partitions))
                actual = _reduce(partials)
                consistent = actual == expected and _checksum(actual) == expected_checksum
                if not consistent:
                    all_consistent = False
                    break
                rounds += 1
                max_round_seconds = max(
                    max_round_seconds, time.perf_counter() - round_started
                )
                if time.perf_counter() - started >= duration_seconds:
                    break
            elapsed = time.perf_counter() - started
        finally:
            if executor is not None:
                executor.shutdown(wait=True, cancel_futures=True)

        if not consistent:
            raise AssertionError(
                f"{workers}-worker output does not match the sequential checksum"
            )
        tasks_completed = rounds * task_count
        throughput = tasks_completed / max(elapsed, 1e-12)
        if workers == 1:
            single_core_throughput = throughput
        speedup = throughput / single_core_throughput if single_core_throughput else 0.0
        samples.append(CoreScalingSample(
            workers=workers,
            requested_duration_seconds=float(duration_seconds),
            actual_duration_seconds=elapsed,
            duration_overshoot_seconds=max(0.0, elapsed - duration_seconds),
            longest_task_batch_seconds=max_round_seconds,
            rounds_completed=rounds,
            tasks_completed=tasks_completed,
            throughput_tasks_per_second=throughput,
            speedup=speedup,
            efficiency=speedup / workers,
            processed_numbers=rounds * total_numbers,
            prime_count_per_round=expected["prime_count"],
            prime_sum_per_round=expected["prime_sum"],
            expected_checksum=expected_checksum,
            output_matches_sequential=consistent,
        ))
    return CoreScalingReport(
        test_numbers=total_numbers,
        task_count_per_round=task_count,
        task_complexity=task_complexity,
        samples=tuple(samples),
        consistency_valid=all_consistent,
    )

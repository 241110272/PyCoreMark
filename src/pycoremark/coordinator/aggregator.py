"""Task execution result aggregator.

Provides thread-safe result collection, streaming progress, and callbacks.
"""

from threading import RLock
from typing import Callable, Dict, List, Optional

from pycoremark.models.task import TaskResult

ProgressCallback = Callable[[int, Optional[int], TaskResult], None]


class ResultAggregator:
    """Collect and organize results for an active experiment."""

    def __init__(self) -> None:
        self._results: Dict[str, TaskResult] = {}
        self._lock = RLock()
        self._callbacks: List[ProgressCallback] = []
        self._expected_total: Optional[int] = None

    def set_expected_total(self, expected_total: Optional[int]) -> None:
        if expected_total is not None and expected_total < 0:
            raise ValueError("expected_total must be non-negative")
        with self._lock:
            self._expected_total = expected_total

    def clear(self, expected_total: Optional[int] = None) -> None:
        """Start a fresh collection window for the next experiment."""
        if expected_total is not None and expected_total < 0:
            raise ValueError("expected_total must be non-negative")
        with self._lock:
            self._results.clear()
            self._expected_total = expected_total

    def add_progress_callback(self, callback: ProgressCallback) -> None:
        if not callable(callback):
            raise TypeError("callback must be callable")
        with self._lock:
            if callback not in self._callbacks:
                self._callbacks.append(callback)

    def remove_progress_callback(self, callback: ProgressCallback) -> None:
        with self._lock:
            if callback in self._callbacks:
                self._callbacks.remove(callback)

    def add_result(self, result: TaskResult) -> bool:
        """Accept the first result for a task and reject duplicate deliveries."""
        with self._lock:
            if result.task_id in self._results:
                return False
            self._results[result.task_id] = result
            count = len(self._results)
            expected = self._expected_total
            callbacks = tuple(self._callbacks)

        # Invoke observers outside the lock so callbacks cannot block ingestion.
        for callback in callbacks:
            try:
                callback(count, expected, result)
            except Exception:
                # A monitoring callback must not break result aggregation.
                continue
        return True

    def get_result(self, task_id: str) -> Optional[TaskResult]:
        with self._lock:
            return self._results.get(task_id)

    def get_all_results(self) -> List[TaskResult]:
        with self._lock:
            return list(self._results.values())

    def get_completed_count(self) -> int:
        with self._lock:
            return len(self._results)

    def get_progress(
        self, expected_total: Optional[int] = None
    ) -> Dict[str, Optional[float]]:
        with self._lock:
            expected = (
                expected_total
                if expected_total is not None
                else self._expected_total
            )
            completed = len(self._results)

        fraction = (
            min(completed / expected, 1.0)
            if expected is not None and expected > 0
            else (1.0 if expected == 0 else None)
        )
        return {
            "completed": completed,
            "expected": expected,
            "fraction": fraction,
        }

    def is_complete(self, expected_total: int) -> bool:
        with self._lock:
            return len(self._results) >= expected_total

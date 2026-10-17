"""Dead-letter queue (DLQ) for permanently failed tasks."""

from abc import ABC, abstractmethod
from typing import List, Dict
from collections import Counter
from pycoremark.models.task import Task, TaskStatus


class DeadLetterQueue(ABC):
    """Abstract interface defining dead-letter queue operations."""

    @abstractmethod
    def push(self, task: Task, reason: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def list_all(self) -> List[tuple[Task, str]]:
        raise NotImplementedError


class InMemoryDeadLetterQueue(DeadLetterQueue):
    """In-memory storage for dead-lettered tasks."""

    def __init__(self) -> None:
        self._dlq: List[tuple[Task, str]] = []

    def push(self, task: Task, reason: str) -> None:
        task.status = TaskStatus.DEAD_LETTERED
        self._dlq.append((task, reason))

    def list_all(self) -> List[tuple[Task, str]]:
        return list(self._dlq)
        
    # DONE(Member 5): Implementasi Post-mortem diagnostics
    def analyze_failures(self) -> Dict[str, Any]:
        """Menganalisis dan mengelompokkan alasan kenapa task bisa gagal masuk DLQ."""
        reasons = [reason for _, reason in self._dlq]
        reason_counts = dict(Counter(reasons))
        
        return {
            "total_dead_tasks": len(self._dlq),
            "reasons_breakdown": reason_counts
        }
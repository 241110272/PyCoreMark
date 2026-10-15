"""Unit tests verifying Fault Tolerance components."""

from pycoremark.fault_tolerance.dlq import InMemoryDeadLetterQueue
from pycoremark.fault_tolerance.idempotency import InMemoryIdempotencyStore
from pycoremark.fault_tolerance.retry import RetryManager
from pycoremark.models.event import RetryPolicy
from pycoremark.models.task import Task, TaskStatus


def test_retry_manager_backoff() -> None:
    """Verify retry policy backoff calculation and limit."""
    policy = RetryPolicy(max_retries=2, initial_backoff_seconds=1.0, backoff_multiplier=2.0)
    manager = RetryManager(policy)
    task = Task(task_id="t-fail", workload_name="dummy", payload={}, max_retries=2)

    assert manager.can_retry(task) is True
    backoff1 = manager.prepare_retry(task)
    assert task.retry_count == 1
    assert task.status == TaskStatus.RETRYING
    assert backoff1 == 1.0

    assert manager.can_retry(task) is True
    backoff2 = manager.prepare_retry(task)
    assert task.retry_count == 2
    assert backoff2 == 2.0

    # Max retries reached
    assert manager.can_retry(task) is False


def test_idempotency_store() -> None:
    """Verify recording and querying idempotency keys."""
    store = InMemoryIdempotencyStore()
    key = "tx-12345"

    assert store.has_seen(key) is False
    store.record(key, {"status": "ok"})
    assert store.has_seen(key) is True
    assert store.get_result(key) == {"status": "ok"}


def test_dead_letter_queue() -> None:
    """Verify pushing permanently failed task to DLQ."""
    dlq = InMemoryDeadLetterQueue()
    task = Task(task_id="t-dead", workload_name="dummy", payload={})

    dlq.push(task, reason="Max retries exceeded")
    assert task.status == TaskStatus.DEAD_LETTERED

    records = dlq.list_all()
    assert len(records) == 1
    assert records[0][0].task_id == "t-dead"
    assert records[0][1] == "Max retries exceeded"


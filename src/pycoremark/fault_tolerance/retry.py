"""Task retry policy enforcement and backoff calculation.

TODO(Member 5): Add randomized jitter to avoid thundering herd on recovery.
"""

import random
from typing import Optional
from pycoremark.models.event import RetryPolicy
from pycoremark.models.task import Task, TaskStatus


class RetryManager:
    """Manages retry limits and backoff calculations for failed tasks."""

    def __init__(self, default_policy: Optional[RetryPolicy] = None) -> None:
        """Initialize RetryManager with default retry policy.

        Args:
            default_policy: Optional RetryPolicy configuration.
        """
        self.policy = default_policy or RetryPolicy()

    def can_retry(self, task: Task) -> bool:
        """Evaluate if the task has remaining retry attempts.

        Args:
            task: Task under evaluation.

        Returns:
            True if retry count < max allowed retries.
        """
        return task.retry_count < min(task.max_retries, self.policy.max_retries)

    def prepare_retry(self, task: Task) -> float:
        """Increment retry count and compute backoff duration in seconds.

        Args:
            task: Task to retry.

        Returns:
            Delay duration before re-dispatching.
        """
        task.retry_count += 1
        task.status = TaskStatus.RETRYING
        
        # Exponential backoff calculation
        base_backoff = self.policy.initial_backoff_seconds * (self.policy.backoff_multiplier ** (task.retry_count - 1))
        backoff = min(base_backoff, self.policy.max_backoff_seconds)
        
        # DONE(Member 5): Ditambahkan randomized jitter (waktu acak 0.1 - 1.0 detik)
        jitter = random.uniform(0.1, 1.0)
        
        return backoff + jitter
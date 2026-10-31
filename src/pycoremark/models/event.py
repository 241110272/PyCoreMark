"""Cluster event, retry policy, and validation models.

Member 1:
- Added schema validation for ClusterEvent.
- Added schema validation for RetryPolicy.
- Added schema validation for ValidationResult.
- Added configurable jitter settings to RetryPolicy.
"""

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _validate_nonempty_string(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")


def _validate_nonnegative_integer(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a nonnegative integer.")


def _validate_finite_nonnegative(value: float, field_name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError(
            f"{field_name} must be a finite nonnegative number."
        )


class ClusterEventType(str, Enum):
    NODE_REGISTERED = "NODE_REGISTERED"
    NODE_DEREGISTERED = "NODE_DEREGISTERED"
    NODE_HEARTBEAT = "NODE_HEARTBEAT"
    NODE_FAILED = "NODE_FAILED"
    NODE_RECOVERED = "NODE_RECOVERED"
    TASK_SCHEDULED = "TASK_SCHEDULED"
    TASK_REASSIGNED = "TASK_REASSIGNED"
    TASK_STARTED = "TASK_STARTED"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"
    EXPERIMENT_STARTED = "EXPERIMENT_STARTED"
    EXPERIMENT_COMPLETED = "EXPERIMENT_COMPLETED"


@dataclass
class ClusterEvent:
    event_id: str
    event_type: ClusterEventType
    source_node_id: str
    timestamp: datetime = field(default_factory=_utc_now)
    payload: Dict[str, Any] = field(default_factory=dict)
    logical_clock: int = 0

    def __post_init__(self) -> None:
        _validate_nonempty_string(self.event_id, "event_id")
        _validate_nonempty_string(self.source_node_id, "source_node_id")

        if not isinstance(self.event_type, ClusterEventType):
            try:
                self.event_type = ClusterEventType(self.event_type)
            except (ValueError, TypeError):
                raise ValueError(
                    "event_type must be a valid ClusterEventType."
                ) from None

        if (
            not isinstance(self.timestamp, datetime)
            or self.timestamp.tzinfo is None
            or self.timestamp.utcoffset() is None
        ):
            raise ValueError(
                "timestamp must be a timezone-aware datetime."
            )

        if not isinstance(self.payload, dict):
            raise ValueError("payload must be a dictionary.")

        _validate_nonnegative_integer(
            self.logical_clock,
            "logical_clock",
        )


@dataclass
class RetryPolicy:
    max_retries: int = 3
    initial_backoff_seconds: float = 0.5
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 10.0
    retry_on_exceptions: tuple[type[Exception], ...] = (Exception,)
    jitter_enabled: bool = True
    jitter_min_seconds: float = 0.1
    jitter_max_seconds: float = 1.0

    def __post_init__(self) -> None:
        _validate_nonnegative_integer(
            self.max_retries,
            "max_retries",
        )

        _validate_finite_nonnegative(
            self.initial_backoff_seconds,
            "initial_backoff_seconds",
        )

        _validate_finite_nonnegative(
            self.max_backoff_seconds,
            "max_backoff_seconds",
        )

        if (
            isinstance(self.backoff_multiplier, bool)
            or not isinstance(self.backoff_multiplier, (int, float))
            or not math.isfinite(self.backoff_multiplier)
            or self.backoff_multiplier <= 0
        ):
            raise ValueError(
                "backoff_multiplier must be a finite positive number."
            )

        if not isinstance(self.jitter_enabled, bool):
            raise ValueError("jitter_enabled must be a boolean.")

        _validate_finite_nonnegative(
            self.jitter_min_seconds,
            "jitter_min_seconds",
        )

        _validate_finite_nonnegative(
            self.jitter_max_seconds,
            "jitter_max_seconds",
        )

        if self.jitter_min_seconds > self.jitter_max_seconds:
            raise ValueError(
                "jitter_min_seconds cannot exceed jitter_max_seconds."
            )

        if (
            not isinstance(self.retry_on_exceptions, tuple)
            or any(
                not isinstance(exc, type)
                or not issubclass(exc, Exception)
                for exc in self.retry_on_exceptions
            )
        ):
            raise ValueError(
                "retry_on_exceptions must be a tuple of exception classes."
            )


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.is_valid, bool):
            raise ValueError("is_valid must be a boolean.")

        if not isinstance(self.errors, list) or not all(
            isinstance(item, str) for item in self.errors
        ):
            raise ValueError("errors must be a list of strings.")

        if not isinstance(self.warnings, list) or not all(
            isinstance(item, str) for item in self.warnings
        ):
            raise ValueError("warnings must be a list of strings.")

        if not isinstance(self.metadata, dict):
            raise ValueError("metadata must be a dictionary.")

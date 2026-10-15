"""Abstract storage repository interfaces for persistence operations.

TODO(Member 7): Add SQLite / PostgreSQL / DuckDB persistent backend drivers.
"""

from abc import ABC, abstractmethod
from typing import List, Optional
from pycoremark.models.experiment import ExperimentConfig, ExperimentResult
from pycoremark.models.metrics import MetricSample
from pycoremark.models.node import NodeInfo
from pycoremark.models.task import Task, TaskResult


class ExperimentRepository(ABC):
    """Abstract persistence interface for benchmark experiments."""

    @abstractmethod
    def save_config(self, config: ExperimentConfig) -> None:
        """Persist experiment configuration."""
        raise NotImplementedError

    @abstractmethod
    def get_config(self, experiment_id: str) -> Optional[ExperimentConfig]:
        """Fetch experiment configuration by id."""
        raise NotImplementedError

    @abstractmethod
    def save_result(self, result: ExperimentResult) -> None:
        """Persist experiment outcome result."""
        raise NotImplementedError

    @abstractmethod
    def get_result(self, experiment_id: str) -> Optional[ExperimentResult]:
        """Fetch experiment result by id."""
        raise NotImplementedError

    @abstractmethod
    def list_all(self) -> List[ExperimentResult]:
        """List all completed experiment results."""
        raise NotImplementedError


class NodeRepository(ABC):
    """Abstract persistence interface for node history."""

    @abstractmethod
    def save_node(self, node: NodeInfo) -> None:
        """Persist node state."""
        raise NotImplementedError

    @abstractmethod
    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Fetch node state."""
        raise NotImplementedError

    @abstractmethod
    def list_nodes(self) -> List[NodeInfo]:
        """List all tracked nodes."""
        raise NotImplementedError


class TaskRepository(ABC):
    """Abstract persistence interface for task states and results."""

    @abstractmethod
    def save_task(self, task: Task) -> None:
        """Save task record."""
        raise NotImplementedError

    @abstractmethod
    def get_task(self, task_id: str) -> Optional[Task]:
        """Retrieve task by id."""
        raise NotImplementedError

    @abstractmethod
    def save_result(self, result: TaskResult) -> None:
        """Save task execution result."""
        raise NotImplementedError

    @abstractmethod
    def get_result(self, task_id: str) -> Optional[TaskResult]:
        """Retrieve task result by id."""
        raise NotImplementedError


class MetricsRepository(ABC):
    """Abstract persistence interface for time-series telemetry samples."""

    @abstractmethod
    def save_sample(self, sample: MetricSample) -> None:
        """Append metric sample."""
        raise NotImplementedError

    @abstractmethod
    def get_samples_for_node(self, node_id: str) -> List[MetricSample]:
        """Retrieve all metric samples for a node."""
        raise NotImplementedError


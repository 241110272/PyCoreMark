"""In-memory concrete repository implementations for local simulation and testing.

TODO(Member 7): Add file-based persistence or relational schema mapping.
"""

from typing import Dict, List, Optional
from pycoremark.models.experiment import ExperimentConfig, ExperimentResult
from pycoremark.models.metrics import MetricSample
from pycoremark.models.node import NodeInfo
from pycoremark.models.task import Task, TaskResult
from pycoremark.storage.base import (
    ExperimentRepository,
    MetricsRepository,
    NodeRepository,
    TaskRepository,
)


class InMemoryExperimentRepository(ExperimentRepository):
    """In-memory store for experiment configs and results."""

    def __init__(self) -> None:
        self._configs: Dict[str, ExperimentConfig] = {}
        self._results: Dict[str, ExperimentResult] = {}

    def save_config(self, config: ExperimentConfig) -> None:
        self._configs[config.experiment_id] = config

    def get_config(self, experiment_id: str) -> Optional[ExperimentConfig]:
        return self._configs.get(experiment_id)

    def save_result(self, result: ExperimentResult) -> None:
        self._results[result.experiment_id] = result

    def get_result(self, experiment_id: str) -> Optional[ExperimentResult]:
        return self._results.get(experiment_id)

    def list_all(self) -> List[ExperimentResult]:
        return list(self._results.values())


class InMemoryNodeRepository(NodeRepository):
    """In-memory store for node history."""

    def __init__(self) -> None:
        self._nodes: Dict[str, NodeInfo] = {}

    def save_node(self, node: NodeInfo) -> None:
        self._nodes[node.node_id] = node

    def get_node(self, node_id: str) -> Optional[NodeInfo]:
        return self._nodes.get(node_id)

    def list_nodes(self) -> List[NodeInfo]:
        return list(self._nodes.values())


class InMemoryTaskRepository(TaskRepository):
    """In-memory store for tasks and results."""

    def __init__(self) -> None:
        self._tasks: Dict[str, Task] = {}
        self._results: Dict[str, TaskResult] = {}

    def save_task(self, task: Task) -> None:
        self._tasks[task.task_id] = task

    def get_task(self, task_id: str) -> Optional[Task]:
        return self._tasks.get(task_id)

    def save_result(self, result: TaskResult) -> None:
        self._results[result.task_id] = result

    def get_result(self, task_id: str) -> Optional[TaskResult]:
        return self._results.get(task_id)


class InMemoryMetricsRepository(MetricsRepository):
    """In-memory store for metric samples."""

    def __init__(self) -> None:
        self._samples: List[MetricSample] = []

    def save_sample(self, sample: MetricSample) -> None:
        self._samples.append(sample)

    def get_samples_for_node(self, node_id: str) -> List[MetricSample]:
        return [s for s in self._samples if s.node_id == node_id]


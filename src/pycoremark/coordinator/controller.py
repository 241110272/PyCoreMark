"""Experiment execution controller managing lifecycle transitions.

TODO(Member 7): Implement concurrent experiment queueing and resource pre-allocation.
"""

from datetime import datetime, timezone
import time
from typing import List, Optional
from pycoremark.analysis.analyzer import PerformanceAnalyzer
from pycoremark.cluster.manager import ClusterManager
from pycoremark.common.logging import get_logger
from pycoremark.coordinator.aggregator import ResultAggregator
from pycoremark.coordinator.dispatcher import TaskDispatcher
from pycoremark.models.experiment import ExperimentConfig, ExperimentResult, ExperimentStatus
from pycoremark.models.task import Task
from pycoremark.scheduling.base import BaseScheduler
from pycoremark.storage.base import ExperimentRepository
from pycoremark.workloads.registry import WorkloadRegistry

logger = get_logger("coordinator.controller")


class ExperimentController:
    """Orchestrates an end-to-end benchmark run across the cluster."""

    def __init__(
        self,
        cluster_manager: ClusterManager,
        scheduler: BaseScheduler,
        dispatcher: TaskDispatcher,
        aggregator: ResultAggregator,
        workload_registry: WorkloadRegistry,
        analyzer: PerformanceAnalyzer,
        experiment_repository: ExperimentRepository,
    ) -> None:
        """Initialize controller with required collaborative services."""
        self.cluster_manager = cluster_manager
        self.scheduler = scheduler
        self.dispatcher = dispatcher
        self.aggregator = aggregator
        self.workload_registry = workload_registry
        self.analyzer = analyzer
        self.experiment_repository = experiment_repository

    def run_experiment(self, config: ExperimentConfig) -> ExperimentResult:
        """Execute experiment: generate tasks, schedule, dispatch, aggregate, and analyze.

        Args:
            config: ExperimentConfig describing workload and test setup.

        Returns:
            ExperimentResult with aggregated performance statistics.
        """
        logger.info("Starting experiment: %s (%s)", config.name, config.experiment_id)
        self.experiment_repository.save_config(config)

        # 1. Instantiate workload and generate tasks
        workload = self.workload_registry.create(config.workload)
        tasks = workload.generate_tasks()
        logger.info("Generated %d tasks for experiment %s", len(tasks), config.experiment_id)

        # 2. Get active nodes from cluster manager
        nodes = self.cluster_manager.get_active_nodes()
        if not nodes:
            logger.error("No active nodes available to run experiment %s", config.experiment_id)
            result = ExperimentResult(
                experiment_id=config.experiment_id,
                status=ExperimentStatus.FAILED,
                total_tasks=len(tasks),
                successful_tasks=0,
                failed_tasks=len(tasks),
                start_time=datetime.now(timezone.utc),
            )
            self.experiment_repository.save_result(result)
            return result

        # 3. Schedule and dispatch
        start_time = time.perf_counter()
        decisions = self.scheduler.schedule_batch(tasks, nodes)
        for task, decision in zip(tasks, decisions):
            if decision.target_node_id:
                self.dispatcher.dispatch(task, decision.target_node_id)
            else:
                logger.warning("Task %s could not be scheduled", task.task_id)

        # 4. In skeleton, return initial result placeholder
        duration = time.perf_counter() - start_time
        # TODO(Member 7): Poll broker for incoming TaskResult objects until completion or timeout
        results = self.aggregator.get_all_results()
        analysis = self.analyzer.analyze(config, results, duration)
        self.experiment_repository.save_result(analysis)
        logger.info("Experiment %s concluded with status %s", config.experiment_id, analysis.status)
        return analysis

"""Experiment execution controller managing lifecycle transitions.

TODO(Member 7): Implement concurrent experiment queueing and resource pre-allocation.
"""

from datetime import datetime, timezone
from dataclasses import asdict, replace
import statistics
import time
from threading import Lock, RLock
from typing import Callable, Iterable
from pycoremark.analysis.analyzer import PerformanceAnalyzer
from pycoremark.analysis.baseline import consistency_checksum
from pycoremark.cluster.manager import ClusterManager
from pycoremark.common.logging import get_logger
from pycoremark.fault_tolerance.dlq import InMemoryDeadLetterQueue
from pycoremark.fault_tolerance.retry import RetryManager
from pycoremark.coordinator.aggregator import ResultAggregator
from pycoremark.coordinator.dispatcher import TaskDispatcher
from pycoremark.models.event import ClusterEventType, RetryPolicy
from pycoremark.models.experiment import ExperimentConfig, ExperimentResult, ExperimentStatus
from pycoremark.models.task import Task, TaskResult, TaskStatus
from pycoremark.monitoring.tracer import TraceManager
from pycoremark.scheduling.base import BaseScheduler
from pycoremark.scheduling.dynamic import DynamicScheduler
from pycoremark.scheduling.resource_aware import ResourceAwareScheduler
from pycoremark.scheduling.static import StaticScheduler
from pycoremark.storage.base import ExperimentRepository
from pycoremark.workloads.registry import WorkloadRegistry

logger = get_logger("coordinator.controller")


def _experiment_config_summary(config: ExperimentConfig) -> dict:
    workload = asdict(config.workload)
    workload_type = workload.get("workload_type")
    workload["workload_type"] = getattr(workload_type, "value", workload_type)
    parameters = {
        key: value for key, value in config.parameters.items()
        if not any(secret in key.lower() for secret in ("token", "password", "secret"))
        and key not in {"baseline_checksum", "baseline_single_node_duration"}
    }
    return {
        "experiment_id": config.experiment_id,
        "name": config.name,
        "scheduler_type": config.scheduler_type,
        "target_nodes": config.target_nodes,
        "repeat_count": config.repeat_count,
        "description": config.description,
        "workload": workload,
        "parameters": parameters,
    }


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
        result_timeout_seconds: float = 60.0,
        metrics_provider: Callable[[], Iterable] | None = None,
    ) -> None:
        """Initialize controller with required collaborative services."""
        self.cluster_manager = cluster_manager
        self.scheduler = scheduler
        self._scheduler_instances: dict[str, BaseScheduler] = {
            type(scheduler).__name__.upper().replace("SCHEDULER", ""): scheduler
        }
        self._scheduler_instances_lock = RLock()
        self.dispatcher = dispatcher
        self.aggregator = aggregator
        self.workload_registry = workload_registry
        self.analyzer = analyzer
        self.experiment_repository = experiment_repository
        if result_timeout_seconds <= 0:
            raise ValueError("result_timeout_seconds must be positive")
        self.result_timeout_seconds = result_timeout_seconds
        self.metrics_provider = metrics_provider or (lambda: [])
        self._run_lock = Lock()
        self._trace_manager = TraceManager()
        self.retry_manager = RetryManager(RetryPolicy(
            max_retries=3,
            initial_backoff_seconds=0.02,
            backoff_multiplier=2.0,
            max_backoff_seconds=0.2,
            jitter_enabled=True,
            jitter_min_seconds=0.01,
            jitter_max_seconds=0.03,
        ))
        self.dead_letter_queue = InMemoryDeadLetterQueue()

    def _scheduler_for(self, requested_type: str) -> BaseScheduler:
        key = (requested_type or "DYNAMIC").upper().replace("-", "_")
        key = {
            "ROUND_ROBIN": "STATIC",
            "LEAST_LOADED": "DYNAMIC",
        }.get(key, key)
        constructors = {
            "STATIC": StaticScheduler,
            "DYNAMIC": DynamicScheduler,
            "RESOURCE_AWARE": ResourceAwareScheduler,
        }
        if key not in constructors:
            raise ValueError(f"Unsupported scheduler type: {requested_type}")
        with self._scheduler_instances_lock:
            if key not in self._scheduler_instances:
                self._scheduler_instances[key] = constructors[key]()
            return self._scheduler_instances[key]

    def update_metrics(self, sample) -> None:
        """Keep every scheduling strategy current with node telemetry."""
        with self._scheduler_instances_lock:
            schedulers = tuple(self._scheduler_instances.values())
        for scheduler in schedulers:
            scheduler.update_metrics(sample)

    def _publish_event(self, event_type: ClusterEventType, payload: dict) -> None:
        publish = getattr(self.dispatcher, "publish_event", None)
        if callable(publish):
            try:
                publish(event_type, payload)
            except Exception:
                logger.exception("Unable to publish experiment event")

    def _fail_task(self, task: Task, result: TaskResult) -> None:
        task.status = TaskStatus.FAILED
        reason = result.error_message or "Task failed permanently"
        self.dead_letter_queue.push(task, reason)
        dead_letter = getattr(self.dispatcher, "dead_letter", None)
        if callable(dead_letter):
            dead_letter(task, reason)
        self.aggregator.add_result(result)

    def _retry_task(self, task: Task, failed_worker_id: str, scheduler) -> bool:
        if not self.retry_manager.can_retry(task):
            return False
        scheduler.release_task(task.task_id)
        candidates = [
            node for node in self.cluster_manager.get_active_nodes()
            if node.node_id != failed_worker_id
        ]
        if not candidates:
            candidates = self.cluster_manager.get_active_nodes()
        delay = self.retry_manager.prepare_retry(task)
        time.sleep(delay)
        decision = scheduler.schedule(task, candidates)
        if decision.target_node_id is None:
            logger.warning("Unable to retry task %s: %s", task.task_id, decision.reason)
            return False
        self._publish_event(
            ClusterEventType.TASK_REASSIGNED,
            {
                "task_id": task.task_id,
                "worker_id": decision.target_node_id,
                "attempt": task.retry_count,
            },
        )
        try:
            self.dispatcher.dispatch(task, decision.target_node_id)
        except Exception:
            scheduler.release_task(task.task_id)
            logger.exception("Retry dispatch failed for task %s", task.task_id)
            return False
        logger.warning(
            "Retrying task %s on %s (%d/%d).",
            task.task_id,
            decision.target_node_id,
            task.retry_count,
            task.max_retries,
        )
        return True

    def run_experiment(self, config: ExperimentConfig) -> ExperimentResult:
        """Execute experiment: generate tasks, schedule, dispatch, aggregate, and analyze.

        Args:
            config: ExperimentConfig describing workload and test setup.

        Returns:
            ExperimentResult with aggregated performance statistics.
        """
        with self._run_lock:
            if config.repeat_count > 1:
                return self._run_repeated(config)
            return self._run_experiment(config)

    def _run_repeated(self, config: ExperimentConfig) -> ExperimentResult:
        trial_results = []
        for trial_index in range(config.repeat_count):
            trial_config = replace(
                config,
                experiment_id=f"{config.experiment_id}-trial-{trial_index + 1}",
                repeat_count=1,
            )
            trial_results.append(self._run_experiment(trial_config))

        def mean(attribute: str) -> float:
            return statistics.fmean(getattr(result, attribute) for result in trial_results)

        completed = all(result.status == ExperimentStatus.COMPLETED for result in trial_results)
        consistency_checks = [
            result.metrics_summary.get("consistency", {}).get("valid")
            for result in trial_results
        ]
        aggregate = ExperimentResult(
            experiment_id=config.experiment_id,
            status=ExperimentStatus.COMPLETED if completed else ExperimentStatus.FAILED,
            total_tasks=sum(result.total_tasks for result in trial_results),
            successful_tasks=sum(result.successful_tasks for result in trial_results),
            failed_tasks=sum(result.failed_tasks for result in trial_results),
            start_time=min(result.start_time for result in trial_results),
            end_time=max(
                (result.end_time or result.start_time) for result in trial_results
            ),
            duration_seconds=mean("duration_seconds"),
            throughput_tasks_per_sec=mean("throughput_tasks_per_sec"),
            speedup=mean("speedup"),
            efficiency=mean("efficiency"),
            average_latency_seconds=mean("average_latency_seconds"),
            communication_overhead_seconds=mean("communication_overhead_seconds"),
            metrics_summary={
                "experiment_config": _experiment_config_summary(config),
                "repeat_count": config.repeat_count,
                "calculation_inputs": {
                    "scheduler_type": config.scheduler_type,
                    "workload": config.workload.name,
                    "target_nodes": config.target_nodes,
                    "workers_that_completed_tasks": statistics.fmean(
                        result.metrics_summary.get("calculation_inputs", {}).get(
                            "workers_that_completed_tasks", 0
                        )
                        for result in trial_results
                    ),
                },
                "trial_ids": [result.experiment_id for result in trial_results],
                "trial_durations_seconds": [result.duration_seconds for result in trial_results],
                "duration_stddev_seconds": statistics.pstdev(
                    result.duration_seconds for result in trial_results
                ),
                "reliability": {
                    "retry_count": sum(
                        result.metrics_summary.get("reliability", {}).get("retry_count", 0)
                        for result in trial_results
                    ),
                    "retry_rate": (
                        sum(result.metrics_summary.get("reliability", {}).get("retry_count", 0)
                            for result in trial_results)
                        / max(1, sum(result.total_tasks for result in trial_results))
                    ),
                    "task_failure_rate": (
                        sum(result.failed_tasks for result in trial_results)
                        / max(1, sum(result.total_tasks for result in trial_results))
                    ),
                    "dead_letter_count": sum(
                        result.metrics_summary.get("reliability", {}).get("dead_letter_count", 0)
                        for result in trial_results
                    ),
                },
                "consistency": {
                    "valid": all(value is True for value in consistency_checks)
                    if any(value is not None for value in consistency_checks) else None,
                    "trial_checks": consistency_checks,
                },
                "trials": [result.metrics_summary for result in trial_results],
            },
        )
        self.experiment_repository.save_config(config)
        self.experiment_repository.save_result(aggregate)
        return aggregate

    def _run_experiment(self, config: ExperimentConfig) -> ExperimentResult:
        scheduler = self._scheduler_for(config.scheduler_type)
        logger.info("Starting experiment: %s (%s)", config.name, config.experiment_id)
        self._publish_event(
            ClusterEventType.EXPERIMENT_STARTED,
            {"experiment_id": config.experiment_id, "name": config.name},
        )
        self.experiment_repository.save_config(config)

        # 1. Instantiate workload and generate tasks
        workload = self.workload_registry.create(config.workload)
        tasks = workload.generate_tasks()
        logger.info("Generated %d tasks for experiment %s", len(tasks), config.experiment_id)

        # 2. Get active nodes from cluster manager
        active_nodes = self.cluster_manager.get_active_nodes()
        nodes = active_nodes[:max(0, config.target_nodes)]
        insufficient_nodes = len(active_nodes) < max(0, config.target_nodes)
        if not nodes or not tasks or insufficient_nodes:
            reason = (
                f"Requested {config.target_nodes} workers but only {len(active_nodes)} are active"
                if insufficient_nodes else "No active worker nodes or workload tasks"
            )
            logger.error("Cannot run experiment %s: %s", config.experiment_id, reason)
            now = datetime.now(timezone.utc)
            for task in tasks:
                self.dead_letter_queue.push(task, reason)
            result = ExperimentResult(
                experiment_id=config.experiment_id,
                status=ExperimentStatus.FAILED,
                total_tasks=len(tasks),
                successful_tasks=0,
                failed_tasks=len(tasks),
                start_time=now,
                end_time=now,
                metrics_summary={
                    "experiment_config": _experiment_config_summary(config),
                    "reliability": {
                        "retry_count": 0,
                        "retry_rate": 0.0,
                        "task_failure_rate": 1.0 if tasks else 0.0,
                        "dead_letter_count": len(tasks),
                        "mean_recovery_time_seconds": 0.0,
                    },
                    "cluster_validation": {
                        "requested_workers": max(0, config.target_nodes),
                        "available_workers": len(active_nodes),
                        "reason": reason,
                    },
                },
            )
            self.experiment_repository.save_result(result)
            self._publish_event(
                ClusterEventType.EXPERIMENT_COMPLETED,
                {"experiment_id": config.experiment_id, "status": result.status.value},
            )
            return result

        # 3. Schedule and dispatch
        wall_started = time.perf_counter()
        started_at = datetime.now(timezone.utc)
        trace_context = self._trace_manager.start_trace()
        experiment_span = self._trace_manager.start_span(
            "experiment", trace_context, "coordinator"
        )
        for task in tasks:
            task.experiment_id = config.experiment_id
            task.trace_id = trace_context.trace_id
            task.parent_span_id = experiment_span.span_id
        expected_ids = {task.task_id for task in tasks}
        tasks_by_id = {task.task_id: task for task in tasks}
        self.aggregator.clear(expected_total=len(tasks))
        decisions = scheduler.schedule_batch(tasks, nodes)
        for task, decision in zip(tasks, decisions):
            if decision.target_node_id:
                try:
                    self.dispatcher.dispatch(task, decision.target_node_id)
                except Exception as exc:
                    failed_result = TaskResult(
                        task_id=task.task_id,
                        worker_id="coordinator",
                        status=TaskStatus.FAILED,
                        error_message=f"Dispatch failed: {exc}",
                    )
                    self._fail_task(task, failed_result)
                    scheduler.release_task(task.task_id)
            else:
                logger.warning("Task %s could not be scheduled", task.task_id)

                failed_result = TaskResult(
                    task_id=task.task_id,
                    worker_id="coordinator",
                    status=TaskStatus.FAILED,
                    error_message=decision.reason or "No worker was available",
                )
                self._fail_task(task, failed_result)

        receive_result = getattr(self.dispatcher, "receive_result", None)
        deadline = time.monotonic() + (
            self.result_timeout_seconds if callable(receive_result) else 0.0
        )
        first_failure_at: dict[str, float] = {}
        recovery_times: list[float] = []
        while not self.aggregator.is_complete(len(tasks)):
            while time.monotonic() < deadline and not self.aggregator.is_complete(len(tasks)):
                remaining = deadline - time.monotonic()
                result = receive_result(min(0.1, remaining)) if callable(receive_result) else None
                if result is None or not isinstance(result, TaskResult) or result.task_id not in expected_ids:
                    continue
                task = tasks_by_id[result.task_id]
                if result.status == TaskStatus.FAILED:
                    first_failure_at.setdefault(task.task_id, time.monotonic())
                    if self._retry_task(task, result.worker_id, scheduler):
                        deadline = max(deadline, time.monotonic() + self.result_timeout_seconds)
                        continue
                    self._fail_task(task, result)
                else:
                    if task.task_id in first_failure_at:
                        recovery_times.append(time.monotonic() - first_failure_at[task.task_id])
                    self.aggregator.add_result(result)
                scheduler.release_task(result.task_id)

            if self.aggregator.is_complete(len(tasks)):
                break

            # Reassign tasks that produced no result, for example after a worker
            # crash or a lost result message. The attempt budget stays bounded.
            received_ids = {result.task_id for result in self.aggregator.get_all_results()}
            missing_ids = expected_ids - received_ids
            retried = False
            for task_id in missing_ids:
                task = tasks_by_id[task_id]
                first_failure_at.setdefault(task_id, time.monotonic())
                if self._retry_task(task, task.assigned_node_id or "", scheduler):
                    retried = True
                else:
                    failed_result = TaskResult(
                        task_id=task_id,
                        worker_id="coordinator",
                        status=TaskStatus.FAILED,
                        error_message="Timed out waiting for worker result; retries exhausted",
                    )
                    self._fail_task(task, failed_result)
                    scheduler.release_task(task_id)
            if retried:
                deadline = time.monotonic() + self.result_timeout_seconds
                continue

        duration = time.perf_counter() - wall_started
        results = self.aggregator.get_all_results()
        actual_workers = len({r.worker_id for r in results if r.status == TaskStatus.COMPLETED})
        analysis = self.analyzer.analyze(
            config,
            results,
            duration,
            baseline_single_node_duration=float(
                config.parameters.get("baseline_single_node_duration", 0.0)
            ),
            num_workers=actual_workers,
            start_time=started_at,
        )
        analysis.end_time = datetime.now(timezone.utc)
        self._trace_manager.finish_span(experiment_span.span_id)

        completed = [result for result in results if result.status == TaskStatus.COMPLETED]
        aggregate = getattr(workload, "aggregate_results", None)
        if completed and callable(aggregate):
            try:
                distributed_output = aggregate(
                    [result.output_data for result in completed]
                )
                analysis.metrics_summary["workload"] = distributed_output
                baseline_checksum = config.parameters.get("baseline_checksum")
                if baseline_checksum:
                    distributed_checksum = consistency_checksum(distributed_output)
                    consistent = distributed_checksum == baseline_checksum
                    analysis.metrics_summary["consistency"] = {
                        "valid": consistent,
                        "baseline_checksum": baseline_checksum,
                        "distributed_checksum": distributed_checksum,
                    }
                    if not consistent:
                        analysis.status = ExperimentStatus.FAILED
                        logger.error(
                            "Experiment %s output differs from sequential baseline.",
                            config.experiment_id,
                        )
            except (KeyError, TypeError, ValueError):
                logger.exception("Could not aggregate workload-specific outputs")

        analysis.metrics_summary["trace"] = {
            "trace_id": trace_context.trace_id,
            "spans": [
                {
                    "span_id": experiment_span.span_id,
                    "parent_span_id": None,
                    "name": experiment_span.name,
                    "node_id": experiment_span.node_id,
                    "start_time": experiment_span.start_time.isoformat(),
                    "end_time": experiment_span.end_time.isoformat()
                    if experiment_span.end_time else None,
                },
                *[
                    {
                        "span_id": result.span_id,
                        "parent_span_id": result.parent_span_id,
                        "name": "task-execution",
                        "task_id": result.task_id,
                        "node_id": result.worker_id,
                        "start_time": result.started_at.isoformat()
                        if result.started_at else None,
                        "end_time": result.completed_at.isoformat(),
                        "attempt": result.attempt_number,
                    }
                    for result in results if result.span_id
                ],
            ],
        }
        analysis.metrics_summary["experiment_config"] = _experiment_config_summary(config)
        retry_count = sum(task.retry_count for task in tasks)
        analysis.metrics_summary["reliability"] = {
            "retry_count": retry_count,
            "retry_rate": retry_count / len(tasks) if tasks else 0.0,
            "task_failure_rate": analysis.failed_tasks / len(tasks) if tasks else 0.0,
            "dead_letter_count": sum(
                task.status == TaskStatus.DEAD_LETTERED for task in tasks
            ),
            "mean_recovery_time_seconds": (
                sum(recovery_times) / len(recovery_times) if recovery_times else 0.0
            ),
        }
        analysis.metrics_summary["per_node_metrics"] = self._per_node_metrics(
            started_at, analysis.end_time, results
        )

        self.experiment_repository.save_result(analysis)
        self._publish_event(
            ClusterEventType.EXPERIMENT_COMPLETED,
            {
                "experiment_id": config.experiment_id,
                "status": analysis.status.value,
                "duration_seconds": duration,
            },
        )
        logger.info("Experiment %s concluded with status %s", config.experiment_id, analysis.status)
        return analysis

    def _per_node_metrics(self, start_time, end_time, results) -> dict:
        samples = [
            sample for sample in self.metrics_provider()
            if start_time <= sample.timestamp <= end_time
        ]
        by_node: dict[str, list] = {}
        for sample in samples:
            by_node.setdefault(sample.node_id, []).append(sample)
        tasks_by_node: dict[str, int] = {}
        for result in results:
            if result.status == TaskStatus.COMPLETED:
                tasks_by_node[result.worker_id] = tasks_by_node.get(result.worker_id, 0) + 1
        output = {}
        for node_id in set(by_node) | set(tasks_by_node):
            node_samples = by_node.get(node_id, [])
            cpu = [sample.cpu_percent for sample in node_samples if sample.cpu_percent is not None]
            memory = [
                sample.memory_used_mb for sample in node_samples
                if sample.memory_used_mb is not None
            ]
            heartbeat_delays = self.cluster_manager.heartbeat_service.get_latency_history(node_id)
            output[node_id] = {
                "tasks_completed": tasks_by_node.get(node_id, 0),
                "average_cpu_percent": round(sum(cpu) / len(cpu), 3) if cpu else None,
                "peak_memory_used_mb": round(max(memory), 3) if memory else None,
                "network_bytes_sent_delta": (
                    max(sample.network_bytes_sent for sample in node_samples)
                    - min(sample.network_bytes_sent for sample in node_samples)
                ) if len(node_samples) > 1 else 0,
                "network_bytes_received_delta": (
                    max(sample.network_bytes_received for sample in node_samples)
                    - min(sample.network_bytes_received for sample in node_samples)
                ) if len(node_samples) > 1 else 0,
                "average_heartbeat_delay_seconds": (
                    round(sum(heartbeat_delays) / len(heartbeat_delays), 6)
                    if heartbeat_delays else None
                ),
                "telemetry_samples": len(node_samples),
            }
        return output

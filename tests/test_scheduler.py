"""Unit tests verifying scheduler interface and basic scheduling decisions."""

from pycoremark.models.node import NodeInfo, NodeResources, NodeStatus
from pycoremark.models.task import Task
from pycoremark.scheduling.dynamic import DynamicScheduler
from pycoremark.scheduling.resource_aware import ResourceAwareScheduler
from pycoremark.scheduling.static import StaticScheduler


def _create_mock_nodes() -> list[NodeInfo]:
    return [
        NodeInfo(
            node_id="node-1",
            hostname="host1",
            port=8001,
            resources=NodeResources(total_cores=4, available_cores=4, total_memory_mb=4096, available_memory_mb=4096),
            status=NodeStatus.READY,
        ),
        NodeInfo(
            node_id="node-2",
            hostname="host2",
            port=8002,
            resources=NodeResources(total_cores=2, available_cores=2, total_memory_mb=2048, available_memory_mb=2048),
            status=NodeStatus.READY,
        ),
    ]


def test_static_scheduler_round_robin() -> None:
    """Verify round-robin static scheduling."""
    nodes = _create_mock_nodes()
    scheduler = StaticScheduler()

    task1 = Task(task_id="t1", workload_name="dummy", payload={})
    task2 = Task(task_id="t2", workload_name="dummy", payload={})
    task3 = Task(task_id="t3", workload_name="dummy", payload={})

    d1 = scheduler.schedule(task1, nodes)
    d2 = scheduler.schedule(task2, nodes)
    d3 = scheduler.schedule(task3, nodes)

    assert d1.target_node_id == "node-1"
    assert d2.target_node_id == "node-2"
    assert d3.target_node_id == "node-1"


def test_dynamic_scheduler_distribution() -> None:
    """Verify dynamic scheduler balances load."""
    nodes = _create_mock_nodes()
    scheduler = DynamicScheduler()

    task1 = Task(task_id="t1", workload_name="dummy", payload={})
    task2 = Task(task_id="t2", workload_name="dummy", payload={})

    d1 = scheduler.schedule(task1, nodes)
    d2 = scheduler.schedule(task2, nodes)

    assert d1.target_node_id == "node-1"
    assert d2.target_node_id == "node-2"


def test_resource_aware_scheduler() -> None:
    """Verify resource constraint matching."""
    nodes = _create_mock_nodes()
    scheduler = ResourceAwareScheduler()

    # Requires 3 cores: only node-1 has 4 cores, node-2 has 2
    task_heavy = Task(task_id="t-heavy", workload_name="dummy", payload={}, required_cores=3)
    decision = scheduler.schedule(task_heavy, nodes)
    assert decision.target_node_id == "node-1"

    # Requires 10 cores: neither node has enough
    task_impossible = Task(task_id="t-impossible", workload_name="dummy", payload={}, required_cores=10)
    decision_none = scheduler.schedule(task_impossible, nodes)
    assert decision_none.target_node_id is None


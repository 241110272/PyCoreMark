"""Comprehensive unit tests for Member 4: Communication and Coordination.

Author: Julius Einstein Chandra (241110272)
"""

import time
import pytest
from pycoremark.common.exceptions import BrokerConnectionError
from pycoremark.communication.broker import InMemoryMessageBroker
from pycoremark.communication.clock import LamportClock, VectorClock
from pycoremark.communication.events import EventConsumer, EventPublisher
from pycoremark.communication.rpc import InMemoryRPCClient, InMemoryRPCServer
from pycoremark.models.event import ClusterEvent, ClusterEventType


# ============================================================================
# MessageBroker Tests
# ============================================================================

def test_broker_basic_publish_poll() -> None:
    """Test basic queue publish and polling."""
    broker = InMemoryMessageBroker()
    broker.publish("test.queue", {"key": "value"})

    msg = broker.poll("test.queue", timeout=0.1)
    assert msg == {"key": "value"}

    empty = broker.poll("test.queue", timeout=0.01)
    assert empty is None


def test_broker_wildcard_pattern_matching() -> None:
    """Test MQTT-style wildcard routing (* for single token, # for multi token)."""
    broker = InMemoryMessageBroker()

    # Subscriber with * wildcard
    worker_sub = broker.subscribe("worker.*.tasks")
    # Subscriber with # wildcard
    cluster_sub = broker.subscribe("cluster.#")

    broker.publish("worker.node1.tasks", "task-payload-1")
    broker.publish("worker.node2.tasks", "task-payload-2")
    broker.publish("cluster.events.node.down", "node-down-event")

    # worker_sub receives worker.node1.tasks and worker.node2.tasks
    assert worker_sub.get_nowait().payload == "task-payload-1"
    assert worker_sub.get_nowait().payload == "task-payload-2"
    assert worker_sub.empty() is True

    # cluster_sub receives cluster.events.node.down
    assert cluster_sub.get_nowait().payload == "node-down-event"


def test_broker_ttl_and_dlq_routing() -> None:
    """Test message expiration and automatic routing to DLQ."""
    broker = InMemoryMessageBroker(dead_letter_topic="custom.dlq")

    # Publish message with tiny TTL (1ms)
    broker.publish("ephemeral.queue", "expired-data", ttl_seconds=0.001)
    time.sleep(0.01)  # wait for expiration

    # Polling the expired message should drop it and route to DLQ
    result = broker.poll("ephemeral.queue", timeout=0.05)
    assert result is None
    assert broker.dropped_count == 1

    # Check DLQ has received the expired payload
    dlq_msg = broker.poll("custom.dlq", timeout=0.1)
    assert dlq_msg == "expired-data"


# ============================================================================
# RPC Client and Server Tests
# ============================================================================

def test_rpc_successful_call() -> None:
    """Test direct RPC execution and response return."""
    server = InMemoryRPCServer(node_id="worker-1")
    server.register_handler("compute_sum", lambda a, b: a + b)
    server.start()

    client = InMemoryRPCClient(registry={"worker-1": server})
    result = client.call("worker-1", "compute_sum", a=10, b=25)
    assert result == 35


def test_rpc_unreachable_node_raises_error() -> None:
    """Test calling non-existent or stopped server raises ConnectionError."""
    server = InMemoryRPCServer(node_id="worker-stopped")
    server.register_handler("ping", lambda: "pong")
    # Not started

    client = InMemoryRPCClient(registry={"worker-stopped": server})
    with pytest.raises(ConnectionError, match="unreachable or stopped"):
        client.call("worker-stopped", "ping")

    with pytest.raises(ConnectionError, match="unreachable or stopped"):
        client.call("unknown-node", "ping")


def test_rpc_unimplemented_method_raises_error() -> None:
    """Test invoking method that was not registered on server."""
    server = InMemoryRPCServer(node_id="worker-2")
    server.start()

    client = InMemoryRPCClient(registry={"worker-2": server})
    with pytest.raises(AttributeError, match="Remote method 'non_existent' not implemented"):
        client.call("worker-2", "non_existent")


def test_rpc_latency_and_timeout() -> None:
    """Test timeout enforcement when simulated latency exceeds deadline."""
    server = InMemoryRPCServer(node_id="worker-slow")
    server.register_handler("slow_calc", lambda: 42)
    server.start()

    # Client with 100ms artificial latency
    slow_client = InMemoryRPCClient(
        registry={"worker-slow": server},
        simulated_latency_ms=100.0,
    )

    # Calling with timeout=0.02s should raise TimeoutError
    with pytest.raises(TimeoutError, match="timed out"):
        slow_client.call("worker-slow", "slow_calc", timeout=0.02)


def test_rpc_simulated_network_failure() -> None:
    """Test failure injection simulates dropped packets."""
    server = InMemoryRPCServer(node_id="worker-unstable")
    server.register_handler("ping", lambda: "pong")
    server.start()

    # 100% failure rate
    failing_client = InMemoryRPCClient(
        registry={"worker-unstable": server},
        simulated_failure_rate=1.0,
    )
    with pytest.raises(BrokerConnectionError, match="Simulated network partition"):
        failing_client.call("worker-unstable", "ping")


# ============================================================================
# Event Publisher, Consumer & Clock Tests
# ============================================================================

def test_event_publisher_and_consumer_with_filtering() -> None:
    """Test publishing typed cluster events and filtering on consumption."""
    broker = InMemoryMessageBroker()
    clock_pub = LamportClock()
    clock_sub = LamportClock()

    publisher = EventPublisher(broker=broker, clock=clock_pub)
    consumer = EventConsumer(
        broker=broker,
        topic="cluster.events",
        filter_types=[ClusterEventType.NODE_FAILED],
        clock=clock_sub,
    )

    ev_registered = ClusterEvent(
        event_id="e1",
        event_type=ClusterEventType.NODE_REGISTERED,
        source_node_id="node-1",
    )
    ev_failed = ClusterEvent(
        event_id="e2",
        event_type=ClusterEventType.NODE_FAILED,
        source_node_id="node-1",
    )

    publisher.publish("cluster.events", ev_registered)
    publisher.publish("cluster.events", ev_failed)

    # Consumer should ignore ev_registered and only receive ev_failed
    polled = consumer.poll(timeout=0.1)
    assert polled is not None
    assert polled.event_id == "e2"
    assert polled.event_type == ClusterEventType.NODE_FAILED
    # Check that consumer clock was synchronized
    assert clock_sub.get_time() >= polled.logical_clock


def test_vector_clock_causality() -> None:
    """Test vector clock precedence and concurrency detection."""
    vc_a = VectorClock(local_node_id="A")
    vc_b = VectorClock(local_node_id="B")

    # Event on node A
    v_a1 = vc_a.tick()  # {"A": 1}
    # Event on node A sends to B
    v_b1 = vc_b.update(v_a1)  # {"A": 1, "B": 1}

    # v_a1 happened before v_b1
    assert VectorClock.is_causally_before(v_a1, v_b1) is True
    assert VectorClock.is_causally_before(v_b1, v_a1) is False
    assert VectorClock.is_concurrent(v_a1, v_b1) is False

    # Independent concurrent event on A
    v_a2 = vc_a.tick()  # {"A": 2}
    # Independent concurrent event on B
    v_b2 = vc_b.tick()  # {"A": 1, "B": 2}

    # v_a2 and v_b2 are concurrent (neither knows about the other)
    assert VectorClock.is_concurrent(v_a2, v_b2) is True


"""Unit tests verifying Lamport Logical Clock operations."""

from pycoremark.communication.clock import LamportClock


def test_lamport_clock_ticks() -> None:
    """Verify local event increments clock."""
    clock = LamportClock()
    assert clock.get_time() == 0

    t1 = clock.tick()
    assert t1 == 1
    assert clock.get_time() == 1

    t2 = clock.tick()
    assert t2 == 2


def test_lamport_clock_remote_update() -> None:
    """Verify remote message receipt updates clock to max(local, remote) + 1."""
    clock_a = LamportClock(initial_value=5)
    clock_b = LamportClock(initial_value=2)

    # Node A sends message at tick
    send_time = clock_a.tick()  # 6
    assert send_time == 6

    # Node B receives message
    received_time = clock_b.update(send_time)  # max(2, 6) + 1 = 7
    assert received_time == 7
    assert clock_b.get_time() == 7


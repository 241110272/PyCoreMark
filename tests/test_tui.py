from pycoremark.cli.tui import PyCoreMarkApp


def test_tui_sparkline_handles_empty_and_progress_samples() -> None:
    assert PyCoreMarkApp._sparkline([]) == "—"
    assert len(PyCoreMarkApp._sparkline([0, 1, 2, 3])) == 4


def test_event_feed_summarizes_task_payload_instead_of_dumping_it() -> None:
    formatted = PyCoreMarkApp._format_event({
        "logical_clock": 17,
        "source_node_id": "worker-1",
        "event_type": "TASK_COMPLETED",
        "payload": {
            "task_id": "cpu-1234567890abcdef",
            "attempt": 2,
            "large_result": "this should not fill the terminal",
        },
    })

    assert "TASK_COMPLETED" in formatted
    assert "attempt 2" in formatted
    assert "large_result" not in formatted

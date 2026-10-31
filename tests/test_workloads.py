"""Behavioral coverage for Filbert Matthew's distributed workloads."""

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Thread
from urllib.error import HTTPError

import pytest

from pycoremark.models.task import Task
from pycoremark.models.workload import WorkloadConfig, WorkloadType
from pycoremark.workloads import CPUWorkload, DataPartitioner, MixedWorkload, PartitionedDataWorkload


@pytest.mark.parametrize("size,parts", [(11, 4), (2, 5), (0, 3)])
def test_partition_coverage_and_balance(size, parts):
    ranges = DataPartitioner.partition_range(size, parts)
    assert [i for start, stop in ranges for i in range(start, stop)] == list(range(size))
    sizes = [stop - start for start, stop in ranges]
    assert max(sizes) - min(sizes) <= 1


@pytest.mark.parametrize("size,parts", [(-1, 2), (3, 0), (3, -1)])
def test_invalid_partition_bounds(size, parts):
    with pytest.raises(ValueError):
        DataPartitioner.partition_range(size, parts)


def test_prime_partials_match_known_sequential_answer():
    workload = CPUWorkload(WorkloadConfig("cpu", WorkloadType.CPU_BOUND, 7,
                                         parameters={"start": 0, "stop": 30}))
    tasks = workload.generate_tasks()
    worker = CPUWorkload(WorkloadConfig("cpu", WorkloadType.CPU_BOUND, 1))
    # Payloads survive JSON transport and do not depend on worker configuration.
    outputs = [worker.execute_task(Task(task.task_id, task.workload_name,
                                        json.loads(json.dumps(task.payload)))) for task in tasks]
    assert workload.aggregate_results(outputs[::-1]) == {
        "prime_count": 10, "prime_sum": 129, "processed_numbers": 30}
    assert {task.task_id for task in tasks}.isdisjoint(
        task.task_id for task in workload.generate_tasks())


def test_word_count_file_remainder_unicode_and_empty_partitions(tmp_path):
    source = tmp_path / "dataset.txt"
    source.write_text("Python python!\nStraße STRASSE\nlast\n", encoding="utf-8")
    workload = PartitionedDataWorkload(WorkloadConfig(
        "words", WorkloadType.DATA_PARTITIONED, 1, partitions=5,
        parameters={"dataset_path": str(source)}))
    outputs = [workload.execute_task(task) for task in workload.generate_tasks()]
    assert workload.aggregate_results(outputs[::-1]) == {
        "processed_records": 3, "word_counts": {"last": 1, "python": 2, "strasse": 2}}


def test_inline_dataset_matches_file_processing():
    workload = PartitionedDataWorkload(WorkloadConfig(
        "words", WorkloadType.DATA_PARTITIONED, 1, partitions=2,
        parameters={"records": ["one two", "two three", "three"]}))
    assert workload.aggregate_results([workload.execute_task(t) for t in workload.generate_tasks()]) == {
        "processed_records": 3, "word_counts": {"one": 1, "three": 2, "two": 2}}


def test_mixed_real_file_hash_and_http_failure(tmp_path):
    received = []

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(503 if self.path == "/fail" else 200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        source = tmp_path / "input.txt"
        source.write_text("  HELLO   World \nSecond\nLast\n", encoding="utf-8")
        url = f"http://127.0.0.1:{server.server_port}"
        workload = MixedWorkload(WorkloadConfig(
            "mixed", WorkloadType.MIXED, 1, task_complexity=2, partitions=2,
            parameters={"dataset_path": str(source), "output_dir": str(tmp_path / "outputs"),
                        "result_url": url}))
        tasks = workload.generate_tasks()
        results = [workload.execute_task(task) for task in tasks]
        assert received == results
        assert sum(result["processed_records"] for result in results) == 3
        reduced = workload.aggregate_results(results)
        assert reduced == workload.aggregate_results(results[::-1])
        assert reduced["processed_records"] == 3
        output = tmp_path / "outputs" / (hashlib.sha256(tasks[0].task_id.encode()).hexdigest() + ".jsonl")
        data = output.read_bytes()
        first = json.loads(data.splitlines()[0])
        assert first == {"text": "hello world", "sha256": hashlib.sha256(
            hashlib.sha256(b"hello world").digest()).hexdigest()}
        assert results[0]["checksum"] == hashlib.sha256(data).hexdigest()
        tasks[0].payload["result_url"] = url + "/fail"
        with pytest.raises(HTTPError):
            workload.execute_task(tasks[0])
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

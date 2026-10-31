"""Safe, bounded local storage performance benchmark."""

from dataclasses import asdict, dataclass
import os
from pathlib import Path
import random
import shutil
import statistics
import tempfile
import time
from typing import Any


MIB = 1024 * 1024


@dataclass(frozen=True)
class StorageBenchmarkResult:
    directory: str
    test_size_mib: int
    sequential_write_mib_s: float
    sequential_read_mib_s: float
    random_read_iops: float
    random_write_iops: float
    random_latency_p50_ms: float
    random_latency_p95_ms: float
    write_fsync_ms: float
    random_operations: int
    data_removed: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * percentile))
    return ordered[index]


def benchmark_storage(
    directory: str | Path | None = None,
    *,
    size_mib: int = 64,
    random_operations: int = 500,
    block_size: int = 4096,
) -> StorageBenchmarkResult:
    """Measure sequential and random I/O using a temporary disposable file.

    The benchmark never writes to existing user files. Its uniquely named
    temporary directory is removed after the run, including on exceptions.
    Sequential writes are flushed with ``fsync``; read results may still be
    influenced by the operating-system cache.
    """
    if isinstance(size_mib, bool) or not isinstance(size_mib, int) or not 1 <= size_mib <= 512:
        raise ValueError("size_mib must be an integer between 1 and 512")
    if isinstance(random_operations, bool) or not isinstance(random_operations, int) or not 1 <= random_operations <= 10000:
        raise ValueError("random_operations must be an integer between 1 and 10000")
    if isinstance(block_size, bool) or not isinstance(block_size, int) or not 512 <= block_size <= 1024 * 1024:
        raise ValueError("block_size must be between 512 bytes and 1 MiB")

    parent = Path(directory).expanduser().resolve() if directory is not None else None
    if parent is not None:
        parent.mkdir(parents=True, exist_ok=True)
        if not parent.is_dir():
            raise NotADirectoryError(parent)
        required = size_mib * MIB + max(16 * MIB, size_mib // 10 * MIB)
        if shutil.disk_usage(parent).free < required:
            raise OSError(f"Not enough free space in {parent} for a {size_mib} MiB test")

    root = None
    data_dir = None
    try:
        root = tempfile.TemporaryDirectory(prefix="pycoremark-storage-", dir=str(parent) if parent else None)
        data_dir = Path(root.name)
        test_file = data_dir / "io-test.bin"
        total_bytes = size_mib * MIB
        chunk_size = min(1024 * 1024, max(block_size, 64 * 1024))
        block = os.urandom(chunk_size)

        start = time.perf_counter()
        with test_file.open("wb", buffering=0) as handle:
            remaining = total_bytes
            while remaining:
                chunk = block[:min(len(block), remaining)]
                handle.write(chunk)
                remaining -= len(chunk)
            flush_started = time.perf_counter()
            os.fsync(handle.fileno())
            fsync_ms = (time.perf_counter() - flush_started) * 1000
        write_elapsed = max(time.perf_counter() - start, 1e-9)

        start = time.perf_counter()
        with test_file.open("rb", buffering=0) as handle:
            while handle.read(chunk_size):
                pass
        read_elapsed = max(time.perf_counter() - start, 1e-9)

        random_reads: list[float] = []
        random_writes: list[float] = []
        total_blocks = max(1, total_bytes // block_size)
        rng = random.SystemRandom()
        with test_file.open("r+b", buffering=0) as handle:
            for index in range(random_operations):
                offset = rng.randrange(total_blocks) * block_size
                is_write = index % 2 == 1
                operation_started = time.perf_counter()
                handle.seek(offset)
                if is_write:
                    handle.write(block[:block_size])
                    random_writes.append(time.perf_counter() - operation_started)
                else:
                    handle.read(block_size)
                    random_reads.append(time.perf_counter() - operation_started)
            handle.flush()
            os.fsync(handle.fileno())

        return StorageBenchmarkResult(
            directory=str(parent or Path(tempfile.gettempdir()).resolve()),
            test_size_mib=size_mib,
            sequential_write_mib_s=round(size_mib / write_elapsed, 2),
            sequential_read_mib_s=round(size_mib / max(read_elapsed, 1e-9), 2),
            random_read_iops=round(len(random_reads) / max(sum(random_reads), 1e-9), 2),
            random_write_iops=round(len(random_writes) / max(sum(random_writes), 1e-9), 2),
            random_latency_p50_ms=round(_percentile(
                [(value * 1000) for value in random_reads + random_writes], 0.50
            ), 3),
            random_latency_p95_ms=round(_percentile(
                [(value * 1000) for value in random_reads + random_writes], 0.95
            ), 3),
            write_fsync_ms=round(fsync_ms, 3),
            random_operations=random_operations,
        )
    finally:
        if root is not None:
            root.cleanup()

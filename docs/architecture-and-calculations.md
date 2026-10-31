# PyCoreMark architecture and calculations

This note describes the implementation and its benchmark math. PyCoreMark is a
**distributed worker-cluster benchmark**, not a machine-learning clustering
package.

## How a run works

1. A `WorkloadConfig` creates independent `Task` objects. CPU tasks carry
   disjoint integer ranges; partitioned-data tasks carry their record chunks.
2. The scheduler selects active workers and reserves each task against the
   worker's current active-task and queue load.
3. The dispatcher publishes tasks to a worker queue. Workers construct the
   registered workload with the task's self-contained payload and call
   `execute_task(task)`.
4. Workers publish a `TaskResult`. The controller collects each expected task
   once, waits for completion (up to its timeout), turns missing results into
   failures, retries explicit failures up to the task's `max_retries` when a
   worker can be selected, releases reservations, and analyzes the full run.
5. Experiment configurations and results are stored in SQLite. Local mode
   stores the database under `%LOCALAPPDATA%\PyCoreMark\`; the networked
   coordinator uses `PYCOREMARK_DATABASE_URL`.

### Local and networked operation

- `pycoremark` opens the Textual dashboard with a local virtual cluster and
  two worker services sharing an in-process broker.
- `pycoremark-coordinator` starts the FastAPI control API and a gRPC
  `ClusterControl` service for registration, heartbeat, health, node lookup,
  cancellation, metrics, events, and result submission.
- Docker Compose runs Redis Streams for `pycoremark:tasks:<worker-id>`,
  `pycoremark:results`, `pycoremark:metrics`, `pycoremark:events`, and
  `pycoremark:dead-letter`. Task messages remain pending until a result is
  published and stale pending tasks can be reclaimed. `memory://` remains the
  lightweight in-process mode.
- Worker IDs join and leave dynamically. Compose replicas use their container
  hostnames when `PYCOREMARK_WORKER_ID=auto`.
- `pycoremark dashboard --coordinator http://HOST:8000` monitors and runs
  experiments on a remote coordinator. `pycoremark run --coordinator ...`
  is the non-interactive equivalent.
- The coordinator's current REST endpoints serve worker registration,
  heartbeats, metrics, task polling/dispatch, results, status, history, and
  experiment runs. Workers poll rather than accept inbound connections.

The FastAPI API and gRPC service use cleartext HTTP/gRPC by default. Set the same non-empty
`PYCOREMARK_REGISTRATION_AUTH_TOKEN` on the coordinator, workers, and remote
CLI/TUI clients to require bearer authentication and a per-registration
challenge. This protects API access but **does not encrypt traffic**; use a
trusted network or TLS termination for deployments outside a trusted network.

`PartitionedDataWorkload` and `MixedWorkload` pass file paths in task payloads.
For separate machines, those paths must refer to data available at the same
location on each worker (for example, a shared volume); PyCoreMark does not
transfer file contents for those workloads.

## CLI and interactive dashboard

Install the project and runtime dependencies with:

```powershell
python -m pip install -e .
```

Then run:

```powershell
python -m pycoremark
python -m pycoremark run --workload cpu --tasks 8 --complexity 25000 --nodes 2
python -m pycoremark run --workload data --records 1000 --nodes 2 --scheduler static --repeats 5
python -m pycoremark scaling --duration 2 --workers 1 2 4 6 8 --tasks 16 --complexity 10000
python -m pycoremark storage --path $env:TEMP --size-mib 64 --random-ops 500
python -m pycoremark dashboard --coordinator http://127.0.0.1:8000
python -m pycoremark web
```

The `pycoremark` console command is also installed; if PowerShell cannot find
it, add Python's `Scripts` directory to `PATH` or use `python -m pycoremark`.

The Textual interface supports keyboard navigation and mouse clicks in modern
Windows Terminal/PowerShell terminals. It switches from side-by-side panels to
a stacked layout on narrow windows. The dashboard shows node state, CPU and
memory telemetry when available, topology links, recent experiment results,
and actions to run CPU, word-count, and storage tests. Storage tests run from
the local dashboard host, not on remote workers.

For separate processes, start the coordinator first with
`python -m pycoremark coordinator`, then set each worker's
`PYCOREMARK_WORKER_ID`, `PYCOREMARK_COORDINATOR_HOST`, `PYCOREMARK_GRPC_PORT`,
and `PYCOREMARK_TRANSPORT=grpc` before running `python -m pycoremark worker`.
In Compose, Redis carries queue/event streams and `worker` replicas use
`PYCOREMARK_WORKER_ID=auto`.

The Streamlit/Plotly dashboard runs with `python -m pycoremark web` on port
8501. The Textual dashboard is the terminal alternative. To start the full
container cluster and attach the web dashboard, run
`docker compose up --build --scale worker=4`, then open `http://localhost:8501`.
For separate PowerShell windows, bind the coordinator to `0.0.0.0`, set each
worker's coordinator host to the coordinator machine's LAN address, and use
unique worker IDs.

## Experiment calculations

For an experiment, let `T` be its wall-clock distributed-run duration,
`C` its completed task count, `N` the number of workers that completed at
least one task, and `B` the sequential baseline duration.

| Metric | Calculation | Meaning |
|---|---|---|
| Throughput | `C / T` | Completed tasks per second. |
| Speedup | `B / T` | Sequential baseline time divided by distributed wall time. |
| Efficiency | `speedup / N` | Speedup per worker that completed work. |
| Task load imbalance | `max(tasks per worker) / average(tasks per worker)` | `1.0` is evenly distributed; values above it indicate skew. |
| Average task latency | arithmetic mean of successful task execution times | Does not include task queue wait. |
| Latency percentiles | nearest-rank p50, p95, and p99 of successful task times | Tail behavior at task execution level. |
| Communication/orchestration overhead estimate | `max(0, T - sum(task execution times) / N)` | An estimate, not a direct network measurement. |

The CLI measures a sequential baseline by executing the same generated work
partitions one at a time before the distributed run. In remote mode this
baseline is measured on the CLI/client host, while `T` is measured by the
coordinator; on heterogeneous machines this is a comparison between those
hosts, not a same-host scaling factor. Baseline time is excluded from `T`. If a
caller supplies no positive baseline, speedup and efficiency
are reported as `0`, rather than presenting a misleading `1.0x` baseline.
Runs with zero completed workers use one as the divisor for safe overhead
calculation and report the actual worker count in `metrics_summary`.
The controller hashes canonical JSON output from the sequential reducer and
the distributed reducer. A checksum mismatch marks the experiment `FAILED`,
and the report validator calls it out. The task-count invariant is
`total = completed + permanently_failed + unfinished`.

`--repeats N` runs independent trials and returns mean duration/throughput/
speedup/efficiency plus each trial's result and duration standard deviation.
Each trial persists separately and the aggregate experiment is persisted under
the requested ID.

### Fixed-window single-core versus multi-core test

`pycoremark scaling` runs the same partitioned prime-number ranges using a
sequential one-process baseline and `ProcessPoolExecutor` workers for N>1.
It is intentionally separate from the in-process virtual-worker benchmark so
the multi-core test uses real OS processes rather than GIL-bound threads.
Each worker-count sample runs for **at least** the requested `--duration`; the
last task batch may cause a small, reported overshoot. Every round's prime
count, sum, processed-number total, and checksum must match the sequential
reference. A mismatch fails the command rather than producing a misleading
speedup. Throughput is compared at the same fixed test duration.

The overhead estimate assumes worker compute time can be approximated by total
task execution time divided by worker count. It is not a measurement of bytes,
serialization cost, network RTT, or queue delay; those require separate spans
and transport instrumentation. The storage benchmark reports its own I/O
metrics and is not folded into these task formulas.

## Cluster behavior

- The failure detector learns heartbeat intervals. Before its configured
  minimum sample count it uses timeout-based suspicion/offline; afterward it
  uses a phi-accrual score based on the normal distribution of observed
  intervals: `phi = -log10(max(1 - CDF(elapsed), 1e-16))`.
- The autoscaler averages each active worker's latest reported `current_load`
  and compares it with the configured scale-up/down thresholds. Automatic
  provisioning still requires a configured `worker_factory`; without one the
  manager returns a recommendation but cannot create machines.
- The dynamic scheduler selects the least-loaded eligible worker, subject to
  queue capacity and the task's declared core/memory requirements. It uses
  fresh telemetry when available and local task reservations otherwise.
- Host CPU/memory/disk/network counters come from `psutil`. If telemetry is
  unavailable, the UI shows `N/A` rather than simulated host readings.

## Storage benchmark methodology and safety

`benchmark_storage()` writes a unique temporary file in the selected directory,
flushes it with `fsync`, reads it sequentially, then performs bounded random
block reads/writes. It reports:

- sequential write and read throughput in MiB/s;
- random read and write IOPS;
- p50 and p95 random-operation latency;
- sequential write `fsync` time.

The test size is limited to 1–512 MiB and the operation count to 1–10,000.
The benchmark checks free space, never overwrites a user-selected file, and
removes its temporary directory even if a run fails. Sequential read throughput
may be inflated by the operating-system cache. Random IOPS use the measured
per-operation latency; random-write flush time is not included in that IOPS
figure. For comparable hardware results, close other disk-heavy applications,
use the same size and directory, and report whether the target is SSD, HDD, or
network storage.

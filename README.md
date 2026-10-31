# PyCoreMark

**PyCoreMark** adalah aplikasi benchmarking untuk mensimulasikan cluster sistem
terdistribusi. Coordinator mengelola membership, membagi workload menjadi task,
menjadwalkan task ke worker, mengumpulkan dan memvalidasi hasil, mendeteksi
kegagalan, serta menyimpan hasil eksperimen. Worker dapat berjalan sebagai
proses Python terpisah atau container Docker.

Proyek ini menguji trade-off distributed computing: penambahan worker dapat
meningkatkan throughput, tetapi juga menambah biaya komunikasi, koordinasi,
resource, dan risiko kegagalan.

## Fitur utama

- Dynamic worker registration, heartbeat, failure detection, node status, dan
  perubahan membership.
- CPU prime benchmark, partitioned word-count, dan mixed file/compute/HTTP
  workload.
- Static, dynamic, dan resource-aware scheduling.
- FastAPI coordinator API, gRPC worker control, Redis Streams queue, serta
  mode lokal berbasis in-memory broker.
- Retry terbatas dengan exponential backoff dan jitter, result deduplication,
  task recovery setelah timeout, dan dead-letter stream.
- Per-node CPU/memory/disk/network telemetry melalui `psutil`, Lamport event
  ordering, task trace IDs/spans, throughput/speedup/efficiency, queue wait,
  load balance, dan failure/retry metrics.
- Dashboard terminal Textual yang dapat diklik serta dashboard web
  Streamlit/Plotly.
- SQLite experiment history, checksum validation terhadap sequential baseline,
  JSON/CSV/Markdown/HTML reporting, dan benchmark storage yang menggunakan
  temporary files.
- Fixed-window single-core vs multi-core benchmark: baseline berjalan di satu
  proses; N>1 menggunakan proses OS terpisah dan memeriksa konsistensi hasil
  pada setiap putaran.

## Arsitektur dan alur task

```text
                  ┌──────────────────────────────┐
                  │ FastAPI coordinator API      │
                  │ membership, status, run, DB  │
                  └───────────┬──────────────────┘
                              │ gRPC control / Redis Streams
                    ┌─────────┴──────────┐
                    │                    │
               Worker A              Worker B ...
                    │                    │
                    └──── task results ──┘
                              │
                 validate + aggregate + SQLite
                              │
                    Streamlit / Textual UI
```

Alur utama: worker mendaftar dengan identitas dan resource/workload yang
didukung → mengirim heartbeat dan telemetry → coordinator mempartisi workload
menjadi task unik → scheduler memilih worker → worker menjalankan task dan
mengirim result/event → coordinator menolak duplicate result, retry task yang
gagal/timeout, memvalidasi checksum, lalu menyimpan eksperimen.

### Transport yang digunakan

- **FastAPI / HTTP JSON** di port `8000`: dashboard, status, history, dan
  permintaan eksperimen.
- **gRPC / protobuf `Struct`** di port `50051`: registration, heartbeat,
  health check, node lookup, task polling, metrics, event, result, dan queued
  task cancellation. RPC schema ada di `proto/cluster.proto`.
- **Redis Streams** pada mode Compose:
  - `pycoremark:tasks:<worker-id>`
  - `pycoremark:results`
  - `pycoremark:metrics`
  - `pycoremark:events`
  - `pycoremark:dead-letter`

Mode lokal memakai broker in-memory. Mode proses terpisah dapat memakai HTTP
atau gRPC untuk worker transport; Compose menggunakan gRPC + Redis Streams.

## Tujuh anggota dan tiga area kerja per anggota

| Anggota | Area | Tiga fokus fitur | Konsep Sistem Terdistribusi |
|---|---|---|---|
| **Angelica Anastasia** | Cluster dan node management | Dynamic registration; heartbeat/failure detector; scaling/membership | Service discovery, identity, partial failure, horizontal scaling |
| **Filbert Matthew** | Distributed workloads | CPU prime partitioning; data word-count/map-reduce; mixed file/compute/network | Partitioning, remote execution, partial results, data locality |
| **Evan Adhyaksa Baskara** | Scheduling/load balancing | Static; dynamic queue; resource-aware CPU/RAM/queue scheduling | Coordinator-worker scheduling, producer-consumer, heterogeneous nodes |
| **Julius Einstein Chandra** | Communication/coordination | gRPC RPC; Redis Streams; Lamport event ordering | RPC, asynchronous messaging, causal event order |
| **Michael Christoffer Chu** | Fault tolerance/consistency | Failure injection; retry/recovery/DLQ; idempotency/deduplication | Crash/communication failure, at-least-once delivery, consistency |
| **Stan Kovic** | Observability/analysis | Per-node metrics; trace IDs/spans; distributed performance/recovery analysis | Telemetry, critical path, scalability, communication overhead |
| **Zakky Pratama** | Dashboard/reporting | Topology dashboard; comparison charts; result validation/export | Membership visualization, reproducibility, audit trail |

Pembagian fitur menjelaskan ownership proyek; sejumlah fitur bekerja melalui
kontrak bersama lintas modul, bukan sebagai implementasi terisolasi.

## Instalasi dan menjalankan aplikasi

Gunakan Python 3.11+.

```powershell
# Create and activate a project environment (recommended)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install the package and development/test dependencies into that same Python
python -m pip install -e ".[dev]"
```

PyCoreMark uses a `src/` package layout, so install it before running commands
from a fresh checkout. Keep the environment active (or use that environment's
Python executable) for every `python -m pycoremark` command; otherwise Python
may report `No module named pycoremark`.

### Mode lokal

```powershell
# Dashboard terminal interaktif (mouse + keyboard)
python -m pycoremark

# CPU/data experiment; `--repeats` menjalankan independent trials
python -m pycoremark run --workload cpu --tasks 16 --complexity 10000 `
  --nodes 2 --scheduler dynamic --repeats 5
python -m pycoremark run --workload data --records 1000 --nodes 2
python -m pycoremark run --workload mixed --dataset-path .\data\input.txt `
  --output-dir .\outputs --result-url http://127.0.0.1:9000/results `
  --tasks 4 --complexity 2 --nodes 2

# Single-core baseline vs proses multi-core, minimal durasi per worker-count
python -m pycoremark scaling --duration 2 --workers 1 2 4 6 8 `
  --tasks 16 --complexity 10000 --json

# Benchmark storage lokal; test file selalu dibuang setelah pengukuran
python -m pycoremark storage --path $env:TEMP --size-mib 64 --random-ops 500
python -m pycoremark report --format detailed-json --output report.json
```

Jika perintah console `pycoremark` belum ada di `PATH`, gunakan
`python -m pycoremark` dari environment yang sudah diaktifkan. Tanpa environment
virtual, jalankan `.\.venv\Scripts\python.exe -m pycoremark`.

### Mode Docker Compose

Docker Desktop harus aktif. Dari root repository:

```powershell
docker compose up --build --scale worker=4
```

Setelah services siap, buka dashboard di `http://localhost:8501`. Coordinator
API ada di `http://localhost:8000`; gRPC worker service ada di port `50051`.
Jumlah worker dapat diuji dengan `--scale worker=1`, `2`, `4`, `6`, atau `8`.
Setiap replica memakai hostname container untuk menghasilkan worker ID unik.
SQLite coordinator dan Redis AOF berada pada named volumes Compose.

Compose replicas bersifat dinamis dari sudut pandang coordinator, tetapi
coordinator tidak mengendalikan Docker Engine untuk membuat container sendiri.
Ubah jumlahnya dengan `docker compose up --scale worker=N`; TUI local memiliki
tombol `+ Worker`/`− Worker` untuk virtual worker in-process. Dashboard web
menampilkan jumlah worker aktif dan memvalidasi target experiment terhadap
membership yang tersedia.

Opsional, sebelum Compose, buat `.env` berdasarkan `.env.example` dan isi
`PYCOREMARK_REGISTRATION_AUTH_TOKEN` dengan secret yang sama untuk semua
services.

Eksperimen fault/network injection dapat diatur pada worker sebelum Compose:
`PYCOREMARK_WORKER_FAILURE_RATE` (`0`–`1`) menyimulasikan task failure,
`PYCOREMARK_SIMULATED_NETWORK_DELAY_MS` menambahkan delay per task, dan
`PYCOREMARK_NETWORK_FAILURE_RATE` (`0`–`1`) menjatuhkan RPC/message secara
acak. Nilai ini cocok untuk eksperimen terkontrol; packet loss dapat membuat
task menunggu timeout sebelum retry/recovery.
Untuk crash nyata pada Compose, ambil satu ID dari `docker compose ps -q worker`
lalu jalankan `docker kill <container-id>` saat benchmark berjalan; failure
detector menandai worker offline dan pending task akan dicoba ulang pada worker
aktif lain setelah timeout. Task simulasi yang
terus gagal mencapai retry limit akan masuk DLQ.

### Coordinator dan worker terpisah

Terminal coordinator:

```powershell
$env:PYCOREMARK_COORDINATOR_HOST="0.0.0.0"
$env:PYCOREMARK_TRANSPORT="grpc"
python -m pycoremark coordinator
```

Terminal worker (ganti host dengan alamat coordinator):

```powershell
$env:PYCOREMARK_COORDINATOR_HOST="127.0.0.1"
$env:PYCOREMARK_WORKER_ID="worker-01"
$env:PYCOREMARK_TRANSPORT="grpc"
python -m pycoremark worker
```

Dashboard terminal remote:

```powershell
python -m pycoremark dashboard --coordinator http://127.0.0.1:8000
```

Dashboard web:

```powershell
python -m pycoremark web
```

`PYCOREMARK_BROKER_URL=redis://HOST:6379/0` mengaktifkan Redis Streams untuk
task/result/metrics/event; jika tidak diset, worker gRPC polls task langsung
melalui coordinator API. Dataset path pada `PartitionedDataWorkload`/`MixedWorkload`
harus tersedia di path yang sama pada seluruh worker, misalnya shared volume.

## Eksperimen dan kalkulasi

### Fixed-window single-core vs multi-core consistency test

`pycoremark scaling` memakai input prima identik untuk semua worker count. Mode
1 menghitung sequential; mode 2+ memakai `ProcessPoolExecutor` sehingga proses
Python terpisah dapat berjalan pada core berbeda. Setiap iterasi membandingkan
`prime_count`, `prime_sum`, `processed_numbers`, dan SHA-256 canonical result
dengan baseline sequential.

Setiap worker count berjalan **setidaknya** selama `--duration`. Batas bawah
durasi diterapkan pada setiap sample; karena eksekusi per batch tidak
dihentikan di tengah task, durasi aktual dapat melewati target sebesar satu
batch terakhir. Report menyimpan target, durasi aktual/overshoot, rounds,
checksum, throughput, speedup, dan efficiency. Nilai `consistency_valid=false`
atau checksum berbeda menggagalkan test.

### Rumus eksperimen task

| Metrik | Rumus |
|---|---|
| Throughput | task selesai / distributed wall time |
| Speedup | sequential baseline time / distributed wall time |
| Efficiency | speedup / jumlah worker yang menyelesaikan task |
| Load imbalance | max(task per worker) / average(task per worker) |
| Queue wait | waktu task dibuat/dimasukkan queue sampai worker mulai, diperkirakan dari timestamp; clock skew remote dapat memengaruhi |
| Communication overhead | `max(0, wall time − total task execution time / worker count)`; estimasi koordinasi, bukan network RTT murni |
| Retry rate | total retry / task count |
| Recovery time | waktu result sukses setelah failure result pertama terdeteksi |

Per-experiment `metrics_summary` menyimpan task latency p50/p95/p99, queue
wait, per-node average CPU/peak memory/network byte delta/heartbeat delay,
load distribution, retry/failure rate, DLQ count, trace spans, dan checksum
comparison. Jika checksum distributed berbeda dari sequential baseline,
experiment ditandai `FAILED` walaupun seluruh task sempat berstatus completed.

Remote-mode sequential baseline diukur pada CLI/dashboard host, sedangkan
distributed duration diukur coordinator. Pada host yang berbeda, speedup
membandingkan mesin tersebut; untuk perbandingan paling adil jalankan client
dan coordinator pada host yang sama atau catat perbedaan hardware.

### Workload dan scheduler

- `cpu_benchmark`: prime count/sum pada disjoint integer ranges.
- `partitioned_data`: deterministic atau UTF-8 records, word count case-folded,
  reducer menggabungkan count setiap partition.
- `mixed`: file range → normalisasi → hash → JSONL lokal → HTTP POST hasil.
- `static`: pembagian round-robin tetap; `dynamic`: least-loaded queue;
  `resource-aware`: CPU/RAM/queue capacity dan kebutuhan resource task.

Mixed workload memerlukan `--dataset-path`, `--output-dir`, dan
`--result-url`; endpoint penerima HTTP POST disediakan oleh eksperimen/layanan
pengguna, bukan oleh PyCoreMark. Pada worker terpisah, dataset harus di-mount
pada path yang sama; output file berada pada filesystem worker.

Contoh:

```powershell
python -m pycoremark run --workload cpu --nodes 4 --tasks 400 `
  --complexity 10000 --scheduler static --repeats 5
python -m pycoremark run --workload cpu --nodes 4 --tasks 400 `
  --complexity 10000 --scheduler dynamic --repeats 5
```

### Storage benchmark

`benchmark_storage()` mengukur sequential write/read MiB/s, random read/write
IOPS, p50/p95 latency, dan fsync. Test size dibatasi 1–512 MiB dan random
operation 1–10.000. File dibuat dalam temporary directory unik, free space
diperiksa, file existing tidak ditimpa, dan test directory dihapus setelah run.
Sequential read dapat terbantu OS cache; catat tipe storage dan ukuran test
saat membandingkan hasil.

## Validasi, retry, dan keamanan

- `total_tasks = successful_tasks + failed_tasks + unfinished_tasks`;
  validator memeriksa seluruh task agar accounted.
- Result aggregator hanya menerima result pertama per `task_id`. Worker
  menggunakan TTL idempotency store untuk mengembalikan hasil completed jika
  task yang sama diterima ulang.
- Retry maksimum mengikuti `Task.max_retries`/`RetryPolicy`, dengan exponential
  backoff+jitter. Timeout/failure permanen dimasukkan ke DLQ. Redis mode
  menyediakan pending-message recovery untuk task yang diambil worker tetapi
  belum di-ack oleh result.
- gRPC dan FastAPI menggunakan cleartext transport dalam konfigurasi default.
  Shared token memerlukan autentikasi API dan registration challenge, tetapi
  **tidak mengenkripsi** trafik; gunakan jaringan tepercaya/TLS di luar
  lingkungan pengembangan.

## Verifikasi dan output

```powershell
python -m pytest -q
python -m compileall -q src tests
git diff --check
```

Output utama tersimpan pada SQLite coordinator. Report generator mengekspor
JSON, CSV, Markdown, dan HTML; Streamlit juga menyediakan CSV comparison
download. Konfigurasi Redis, database, transport, worker resources, heartbeat,
failure rate, dan simulated network delay dapat diatur lewat
`PYCOREMARK_*` environment variables.

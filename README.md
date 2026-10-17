# PyCoreMark: Virtual Cluster Benchmarking for Distributed Systems

PyCoreMark adalah kerangka kerja simulasi *virtual cluster* terdistribusi yang dirancang untuk mengukur, menganalisis, dan membandingkan performa penjadwalan (*scheduling*), beban kerja (*workload*), toleransi kesalahan (*fault tolerance*), serta metrik sistem terdistribusi (seperti *throughput*, *speedup*, *efficiency*, dan *overhead*).

Repository ini berisi **arsitektur modular skeleton** siap pakai yang dirancang untuk dikembangkan secara kolaboratif oleh **tujuh (7) anggota tim** secara independen tanpa saling bertabrakan (*circular dependency free*).

---

## 🏛️ Arsitektur Sistem

PyCoreMark mengadopsi model terdistribusi terkoordinasi (*coordinator-worker pattern*) dengan pertukaran pesan asinkron dan RPC:

1. **Coordinator**: Mengatur keanggotaan cluster, memantau *liveness* node, menjadwalkan task ke antrean worker, mengontrol siklus eksperimen, dan mengumpulkan hasil.
2. **Workers**: Virtual node yang meregistrasikan diri, mengirimkan *heartbeat* secara periodik, menerima unit task, menjalankan beban kerja komputasi/data, dan melaporkan telemetri metrik.
3. **Message Broker**: Abstraksi antrean pesan (*message queue*) dan *pub/sub event bus* untuk transmisi task dan event cluster.
4. **Fault Tolerance & Simulator**: Manajemen percobaan ulang (*retry*), deteksi duplikasi (*idempotency*), penanganan kegagalan (*recovery/DLQ*), dan simulator gangguan kekacauan (*chaos/failure injection*).
5. **Monitoring & Tracing**: Perekaman CPU, memori, latensi, dan *distributed trace spans* berbasis konteks penelusuran.
6. **Analysis & Metrics**: Mesin kalkulasi terpusat untuk *Throughput*, *Speedup*, *Efficiency*, *Load Imbalance*, dan *Communication Overhead*.
7. **Dashboard & Reporting**: Layanan topologi cluster, pembanding riwayat eksperimen, validator data, dan eksportir laporan (JSON & CSV).

---

## 👥 Pembagian Tanggung Jawab Anggota (7 Tim)

Setiap anggota kelompok bertanggung jawab atas modul tertentu dengan penanda komentar `TODO(Member X)` di dalam kode sumber:

| Anggota | Area Tanggung Jawab | Modul Terkait | Komponen Kunci |
|---|---|---|---|
| **Member 1** | **Core Infrastructure & Cluster Membership** | `common/`, `cluster/` | `NodeRegistry`, `NodeRegistrationService`, `HeartbeatService`, `FailureDetector`, `ClusterManager` |
| **Member 2 — Filbert Matthew** | **Distributed Workloads & Partitioning** | `workloads/` | `BaseWorkload`, `CPUWorkload`, `PartitionedDataWorkload`, `MixedWorkload`, `WorkloadRegistry`, `DataPartitioner` |
| **Member 3 - Evan Adhyaksa Baskara** | **Scheduling & Load Balancing** | `scheduling/` | `BaseScheduler`, `StaticScheduler` (Round-Robin), `DynamicScheduler` (Least-Loaded), `ResourceAwareScheduler` |
| **Member 4** | **Communication & Coordination** | `communication/` | `MessageBroker`, `RPCClient`, `RPCServer`, `EventPublisher`, `EventConsumer`, `LamportClock` |
| **Member 5** | **Fault Tolerance & Reliability** | `fault_tolerance/` | `FailureSimulator`, `RetryManager`, `RecoveryManager`, `IdempotencyStore`, `DeadLetterQueue` |
| **Member 6** | **Monitoring, Tracing & Performance Analysis** | `monitoring/`, `analysis/` | `NodeMetricsCollector`, `ClusterMetricsCollector`, `TraceManager`, `PerformanceAnalyzer`, `OverheadCalculator` |
| **Member 7** | **Coordinator, Worker Runtime, Storage & Reporting**| `coordinator/`, `worker/`, `storage/`, `dashboard/`, `reporting/` | `CoordinatorService`, `WorkerService`, `TaskExecutor`, `ExperimentRepository`, `DashboardService`, `ReportGenerator` |

### Implementasi Anggota 2 — Filbert Matthew

Tiga workload di `src/pycoremark/workloads/` sudah menjalankan operasi nyata, bukan hasil dummy. Tidak ada dependency tambahan di luar Python standard library.

| Fitur | Implementasi | Konfigurasi |
|---|---|---|
| CPU-bound | Menghitung jumlah dan penjumlahan bilangan prima pada rentang `[start, stop)` | `total_tasks` menentukan jumlah task; `parameters.start` default 0; `parameters.stop` default `start + total_tasks * task_complexity` |
| Dataset / MapReduce | Word count Unicode, case-insensitive, per record; penggabungan hasil parsial | `partitions` menentukan jumlah task; `parameters.records` berupa list string atau `parameters.dataset_path` berupa file UTF-8, satu record per baris |
| Mixed workload | Membaca rentang baris file, normalisasi teks, SHA-256 berulang, menulis JSONL lokal, HTTP POST ringkasan | Wajib `parameters.dataset_path`, `parameters.output_dir`, dan `parameters.result_url`; `task_complexity` = jumlah putaran hash; `partitions` = jumlah task; timeout jaringan default 10 detik |

Tanpa dataset eksplisit, workload data menghasilkan record deterministik `record <index> pycoremark` sebanyak `dataset_size_records`. Jika path dan records sama-sama diberikan, file diprioritaskan. Pembagian menyertakan sisa record, tidak overlap, dan selisih ukuran partisi maksimal satu. Partisi kosong tetap menghasilkan hasil valid.

Contoh pemakaian untuk coordinator dan worker:

```python
from pycoremark.models.workload import WorkloadConfig, WorkloadType
from pycoremark.workloads import CPUWorkload, WorkloadRegistry

config = WorkloadConfig(
    name="cpu_benchmark",
    workload_type=WorkloadType.CPU_BOUND,
    total_tasks=4,
    parameters={"start": 0, "stop": 1000},
)
registry = WorkloadRegistry()
registry.register(config.name, CPUWorkload)
workload = registry.create(config)
tasks = workload.generate_tasks()  # coordinator: kirim setiap Task lewat transport tim
partials = [workload.execute_task(task) for task in tasks]  # contoh lokal, bukan cluster
result = workload.aggregate_results(partials)
assert result == {"prime_count": 168, "prime_sum": 76127, "processed_numbers": 1000}
```

`PartitionedDataWorkload.aggregate_results()` menggabungkan `processed_records` dan `word_counts`. Untuk baseline sequential, jalankan input identik dengan satu task CPU atau satu partisi data, lalu bandingkan hasil agregat. Reducer tidak melakukan deduplikasi: coordinator harus meneruskan hasil unik yang telah diterima. Setiap pemanggilan `generate_tasks()` membuat namespace ID baru; retry harus memakai Task yang sama.

**Kontrak mixed workload:** `dataset_path` harus menunjuk file UTF-8 yang sama dan tidak berubah pada semua worker, misalnya melalui bind mount dengan path container identik. `output_dir` adalah penyimpanan lokal worker. Endpoint `result_url` harus dapat menerima HTTP POST JSON dan mengembalikan status 2xx setelah menerima hasil. Body berisi `task_id`, `processed_records`, `checksum` SHA-256 file JSONL, `output_path`, dan `output_bytes`. Path keluaran bukan URL unduhan. Error file, timeout, atau HTTP gagal diteruskan kepada caller; tidak ada simulasi sukses maupun retry internal. Pengiriman ulang dapat terjadi saat retry; deduplikasi endpoint tetap tanggung jawab bagian fault tolerance. Input path dan URL hanya untuk konfigurasi cluster tepercaya, bukan input publik tanpa pembatasan.

**Batas integrasi:** runtime `worker/executor.py` masih mengembalikan hasil dummy dan belum memanggil kernel workload. Anggota runtime perlu menghubungkan `execute_task(task)` ke `TaskResult.output_data`; anggota komunikasi tetap menangani gRPC dan Redis Streams. Implementasi anggota 2 tidak mengubah modul tersebut. HTTP pada mixed workload adalah operasi network I/O benchmark, bukan pengganti transport cluster. Hasil mixed menyimpan checksum per partisi; validasi end-to-end dan laporan tetap bagian anggota 7.

Verifikasi bagian ini:

```bash
python -m pytest tests/test_workloads.py -v
```

Smoke scenario telah menjalankan CPU dan word count melalui dua proses worker terpisah, serta mixed workload melalui dua proses dengan file sementara dan HTTP server lokal sungguhan. Ini membuktikan kernel lintas proses dan network I/O lokal, bukan deployment Docker/gRPC/Redis.

---

## 📁 Struktur Folder Proyek

```text
pycoremark/
├── README.md                  # Dokumentasi panduan arsitektur dan eksekusi
├── pyproject.toml             # Konfigurasi build sistem dan dependencies
├── .env.example               # Template environment variable konfigurasi cluster
├── .gitignore                 # Filter file sementara dan environment
├── compose.yaml               # Docker Compose multi-container virtual cluster
├── src/
│   └── pycoremark/
│       ├── __init__.py        # Versi dan metadata package
│       ├── common/            # Logging, config loader, dan domain exceptions
│       │   ├── __init__.py
│       │   ├── config.py
│       │   ├── exceptions.py
│       │   └── logging.py
│       ├── models/            # Domain model dataclasses & Enums (Kontrak Bersama)
│       │   ├── __init__.py
│       │   ├── node.py
│       │   ├── task.py
│       │   ├── workload.py
│       │   ├── experiment.py
│       │   ├── metrics.py
│       │   └── event.py
│       ├── cluster/           # Manajemen node cluster, heartbeat, & kegagalan
│       │   ├── __init__.py
│       │   ├── registry.py
│       │   ├── registration.py
│       │   ├── heartbeat.py
│       │   ├── detector.py
│       │   └── manager.py
│       ├── communication/     # Abstraksi broker, RPC, event bus, dan Lamport clock
│       │   ├── __init__.py
│       │   ├── broker.py
│       │   ├── rpc.py
│       │   ├── events.py
│       │   └── clock.py
│       ├── workloads/         # Generator beban kerja (CPU, data partition, mixed)
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── cpu.py
│       │   ├── data.py
│       │   ├── mixed.py
│       │   ├── registry.py
│       │   └── partitioner.py
│       ├── scheduling/        # Algoritma penjadwalan dan load balancing
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── static.py
│       │   ├── dynamic.py
│       │   └── resource_aware.py
│       ├── fault_tolerance/   # Retry policy, recovery, simulator chaos, DLQ
│       │   ├── __init__.py
│       │   ├── simulator.py
│       │   ├── retry.py
│       │   ├── recovery.py
│       │   ├── idempotency.py
│       │   └── dlq.py
│       ├── monitoring/        # Koleksi telemetri metrik dan distributed tracing
│       │   ├── __init__.py
│       │   ├── collector.py
│       │   ├── cluster_collector.py
│       │   ├── tracer.py
│       │   └── publisher.py
│       ├── analysis/          # Kalkulator throughput, speedup, efisiensi, overhead
│       │   ├── __init__.py
│       │   ├── analyzer.py
│       │   ├── metrics_calc.py
│       │   └── overhead.py
│       ├── storage/           # Antarmuka repositori & in-memory store
│       │   ├── __init__.py
│       │   ├── base.py
│       │   └── memory.py
│       ├── coordinator/       # Orkestrator pusat cluster, dispatcher & aggregator
│       │   ├── __init__.py
│       │   ├── service.py
│       │   ├── controller.py
│       │   ├── dispatcher.py
│       │   └── aggregator.py
│       ├── worker/            # Runtime node virtual worker & background agents
│       │   ├── __init__.py
│       │   ├── service.py
│       │   ├── executor.py
│       │   └── agents.py
│       ├── dashboard/         # Layanan visualisasi topologi & perbandingan hasil
│       │   ├── __init__.py
│       │   ├── service.py
│       │   ├── topology.py
│       │   └── comparison.py
│       ├── reporting/         # Validator hasil dan eksportir data (JSON/CSV)
│       │   ├── __init__.py
│       │   ├── validator.py
│       │   ├── exporters.py
│       │   └── generator.py
│       └── cli/               # CLI Entry point layanan aplikasi
│           ├── __init__.py
│           ├── coordinator.py
│           ├── worker.py
│           ├── dashboard.py
│           └── experiment.py
└── tests/                     # Unit test & import validation suite
    ├── __init__.py
    ├── test_imports.py
    ├── test_models.py
    ├── test_scheduler.py
    ├── test_lamport_clock.py
    └── test_fault_tolerance.py
```

---

## 🚀 Panduan Memulai (*Quickstart*)

### 1. Prasyarat Lingkungan
- Python 3.11 atau lebih baru.
- Virtual environment disarankan.

### 2. Instalasi Dependensi
```bash
# Buat dan aktifkan virtual environment (opsional tetapi disarankan)
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1

# Install package dalam mode editable beserta development tool (pytest)
python -m pip install -e .
python -m pip install pytest
```

### 3. Menjalankan Pengujian (*Tests*)
Pastikan semua modul terintegrasi tanpa error import atau circular dependency:
```bash
python -m pytest tests/ -v
```

### 4. Menjalankan Simulasi Eksperimen Dummy
Uji coba alur kerja lengkap (registrasi node virtual, penjadwalan, eksekusi, analisis, dan ekspor laporan):
```bash
# Menggunakan modul Python:
python -m pycoremark.cli.experiment

# Atau via executable CLI script (jika di-install via pip):
pycoremark-run-dummy
```

### 5. Menjalankan Service Individual
```bash
# Menjalankan Coordinator
python -m pycoremark.cli.coordinator

# Menjalankan Worker
python -m pycoremark.cli.worker

# Menjalankan Dashboard
python -m pycoremark.cli.dashboard
```

### 6. Simulasi Docker Compose
```bash
docker compose up --build
```


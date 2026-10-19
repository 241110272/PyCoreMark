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
| **Member 3 — Evan Adhyaksa Baskara** | **Scheduling & Load Balancing** | `scheduling/` | `BaseScheduler`, `StaticScheduler` (Round-Robin), `DynamicScheduler` (Least-Loaded), `ResourceAwareScheduler` |
| **Member 4** | **Communication & Coordination** | `communication/` | `MessageBroker`, `RPCClient`, `RPCServer`, `EventPublisher`, `EventConsumer`, `LamportClock` |
| **Member 5 — Michael Christoffer Chu** | **Fault Tolerance & Reliability** | `fault_tolerance/` | `FailureSimulator`, `RetryManager`, `RecoveryManager`, `IdempotencyStore`, `DeadLetterQueue` |
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
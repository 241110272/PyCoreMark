# PyCoreMark: Virtual Cluster Benchmarking for Distributed Systems
PyCoreMark adalah kerangka kerja simulasi *virtual cluster* terdistribusi yang dirancang untuk mengukur, menganalisis, dan membandingkan performa penjadwalan (*scheduling*), beban kerja (*workload*), toleransi kesalahan (*fault tolerance*), serta metrik sistem terdistribusi (seperti *throughput*, *speedup*, *efficiency*, dan *overhead*).
Repository ini berisi **arsitektur modular skeleton** siap pakai yang dirancang untuk dikembangkan secara kolaboratif oleh **tujuh (7) anggota tim** secara independen tanpa saling bertabrakan (*circular dependency free*).
---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
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
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---
## 👥 Pembagian Tanggung Jawab Anggota (7 Tim)
Setiap anggota kelompok bertanggung jawab atas modul tertentu dengan penanda komentar `TODO(Member X)` di dalam kode sumber:
| Anggota | Area Tanggung Jawab | Modul Terkait | Komponen Kunci |
|---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---|---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---|---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---|---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---|
| **Member 1** | **Core Infrastructure & Cluster Membership** | `common/`, `cluster/` | `NodeRegistry`, `NodeRegistrationService`, `HeartbeatService`, `FailureDetector`, `ClusterManager` |
| **Member 2 — Filbert Matthew** | **Distributed Workloads & Partitioning** | `workloads/` | `BaseWorkload`, `CPUWorkload`, `PartitionedDataWorkload`, `MixedWorkload`, `WorkloadRegistry`, `DataPartitioner` |
| **Member 3 - Evan Adhyaksa Baskara** | **Scheduling & Load Balancing** | `scheduling/` | `BaseScheduler`, `StaticScheduler` (Round-Robin), `DynamicScheduler` (Least-Loaded), `ResourceAwareScheduler` |
| **Member 4 — Julius Einstein Chandra (241110272)** | **Communication & Coordination** | `communication/` | `MessageBroker`, `RPCClient`, `RPCServer`, `EventPublisher`, `EventConsumer`, `LamportClock`, `VectorClock` |
| **Member 5** | **Fault Tolerance & Reliability** | `fault_tolerance/` | `FailureSimulator`, `RetryManager`, `RecoveryManager`, `IdempotencyStore`, `DeadLetterQueue` |
| **Member 6** | **Monitoring, Tracing & Performance Analysis** | `monitoring/`, `analysis/` | `NodeMetricsCollector`, `ClusterMetricsCollector`, `TraceManager`, `PerformanceAnalyzer`, `OverheadCalculator` |
| **Member 7** | **Coordinator, Worker Runtime, Storage & Reporting**| `coordinator/`, `worker/`, `storage/`, `dashboard/`, `reporting/` | `CoordinatorService`, `WorkerService`, `TaskExecutor`, `ExperimentRepository`, `DashboardService`, `ReportGenerator` |
### Implementasi Anggota 2 — Filbert Matthew
Tiga workload di `src/pycoremark/workloads/` sudah menjalankan operasi nyata, bukan hasil dummy. Tidak ada dependency tambahan di luar Python standard library.
| Fitur | Implementasi | Konfigurasi |
|---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---|---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---|---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---|
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
tasks = workload.generate_tasks()  # coordinator: kirim setiap Task lewat transport tim
partials = [workload.execute_task(task) for task in tasks]  # contoh lokal, bukan cluster
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
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---
### Implementasi Anggota 5 — Michael Christoffer Chu

Modul `fault_tolerance` menangani kegagalan sistem secara *gracefully* melalui pencegahan duplikasi, pengulangan eksekusi yang gagal secara adaptif, serta penyediaan mekanisme diagnosa. Tidak memerlukan library eksternal, dan langsung siap jika ingin di-*upgrade* ke sistem Redis sungguhan.

| Fitur | Implementasi | Konfigurasi |
|---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---|---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---|---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
```
---|
| **Idempotency Store (TTL)** | Mencegah sebuah *task* dieksekusi ganda jika sistem *recovery* mengirimnya dua kali. Menggunakan struktur in-memory dengan *Time-to-Live (TTL) eviction* otomatis untuk mencegah *memory leak*. Siap untuk migrasi ke Redis. | Parameter `ttl_seconds` untuk menentukan usia kedaluwarsa penyimpanan (default: 3600 detik). |
| **Retry Manager (Jitter)** | Mengulang *task* gagal dengan algoritma **Exponential Backoff**. Ditambahkan *Randomized Jitter* (0.1 - 1.0 detik) untuk mencegah efek *Thundering Herd* (server *down* karena diserang *worker* yang hidup bersamaan). | Bergantung pada `RetryPolicy`: `initial_backoff_seconds`, `backoff_multiplier`, dan `max_backoff_seconds`. |
| **Dead-Letter Queue (DLQ)** | Menampung *task* yang sudah melewati batas maksimal *retry*. Menyediakan fungsi `analyze_failures()` untuk laporan *post-mortem diagnostics* (pengelompokan alasan kematian *task* secara otomatis). | Terintegrasi langsung dan dipanggil otomatis oleh `RetryManager`. |
| **Chaos Simulator** | *Engine* penyuntik kekacauan untuk menguji *resilience* (ketahanan) kode anggota lain. Mendukung simulasi *Worker Crash* total, *High Latency*, *Packet Loss*, dan *Network Partition (Split-Brain)*. | Probabilitas kegagalan diatur melalui parameter `failure_rate` dan `issue_probability`. |
| **Task Recovery** | Mendeteksi *task* berstatus *running* yang "terlantar" karena node/workernya terputus. Mampu membaca data *checkpoint* untuk memulihkan *progress* secara parsial tanpa harus mengulang komputasi dari nol. | Mengambil input dari `active_tasks` dan `failed_node_ids` yang dilaporkan oleh sistem *monitoring*. |

Contoh penggunaan skenario *Fault Tolerance* secara mandiri:

```python
import time
from pycoremark.fault_tolerance.idempotency import InMemoryIdempotencyStore
from pycoremark.fault_tolerance.dlq import InMemoryDeadLetterQueue
from pycoremark.fault_tolerance.retry import RetryManager
from pycoremark.models.event import RetryPolicy
from pycoremark.models.task import Task

# 1. Inisialisasi Komponen Pengaman
store = InMemoryIdempotencyStore(ttl_seconds=3600)
dlq = InMemoryDeadLetterQueue()

# Kebijakan: Max 3x percobaan, tunggu 1s -> 2s -> 4s (maks 5s), ditambah Jitter
policy = RetryPolicy(initial_backoff_seconds=1, backoff_multiplier=2, max_backoff_seconds=5, max_retries=3)
retry_mgr = RetryManager(default_policy=policy)
retry_mgr.dlq = dlq  # Integrasi DLQ

# 2. Pipeline Eksekusi Task Aman
def process_task_safely(task: Task, executor_func):
    # A. Cek duplikasi (Idempotency)
    if store.has_seen(task.task_id):
        return store.get_result(task.task_id)

    # B. Eksekusi dengan sistem Retry & Jitter
    while retry_mgr.can_retry(task):
        try:
            result = executor_func(task)
            store.record(task.task_id, value=result) # Catat keberhasilan
            return result
        except Exception as e:
            delay = retry_mgr.prepare_retry(task)
            time.sleep(delay) # Sleep acak (backoff + jitter)

    # C. Jika tetap gagal, task otomatis terbuang ke DLQ oleh kebijakan sistem
    # Lakukan diagnosa: print(dlq.analyze_failures())
    return None
**
Implementasi Anggota 7 — Dashboard, Visualization, dan Reporting
Implementasi Anggota 7 berfokus pada layanan pengambilan status cluster, perbandingan hasil eksperimen, validasi hasil, dan pembuatan laporan benchmark. Bagian ini mendokumentasikan layanan yang tersedia pada kode; layanan tersebut belum otomatis berarti dashboard web interaktif sudah terintegrasi.
Fitur	Implementasi	Konfigurasi / batasan
Cluster topology	src/pycoremark/dashboard/topology.py mengambil snapshot dari NodeRegistry, meliputi ID node, status, total dan available CPU cores, total memori, serta waktu heartbeat terakhir	Data yang ditampilkan bergantung pada node yang terdaftar di NodeRegistry. UI web, hubungan visual antar-node, task aktif, dan network latency belum terverifikasi terintegrasi
Dashboard service	src/pycoremark/dashboard/service.py menyediakan get_cluster_status() dan compare_experiments() sebagai facade layanan	Ini merupakan lapisan layanan, bukan bukti bahwa UI dashboard web sudah tersedia
Experiment comparison	src/pycoremark/dashboard/comparison.py membandingkan eksperimen berdasarkan ID dan menyediakan informasi jumlah eksperimen yang diminta, ditemukan, serta ID yang tidak ditemukan	Perbandingan menggunakan data eksperimen yang tersedia. Grafik interaktif dan semua skenario analisis benchmark belum terverifikasi
Result validation	src/pycoremark/reporting/validator.py memeriksa jumlah task, nilai metrik, durasi, speedup, dan timestamp; hasil validasi dapat berisi error dan warning	Validasi hanya dapat memastikan hal yang didukung oleh data input. Kesetaraan checksum distributed dengan baseline sequential harus diintegrasikan dan diuji secara eksplisit
Report exporters	src/pycoremark/reporting/exporters.py menyediakan exporter JSON, CSV, Markdown, dan HTML	Keberhasilan ekspor tidak menjamin setiap field benchmark sudah tersedia di data sumber. Ekspor PDF belum diklaim
Report generator	src/pycoremark/reporting/generator.py menyediakan pembuatan laporan, termasuk laporan terperinci JSON dan Markdown yang menyertakan hasil validasi	Isi laporan bergantung pada field yang tersedia pada hasil eksperimen


Validasi hasil eksperimen
Salah satu pemeriksaan konsistensi yang diperlukan adalah memastikan seluruh task dapat dipertanggungjawabkan:
total_tasks_created = completed + permanently_failed + unfinished
Jika data jumlah task yang diperlukan tidak tersedia, persamaan tersebut belum dapat dibuktikan hanya dari hasil validator. Demikian pula, validasi kesetaraan hasil distributed dengan baseline sequential membutuhkan input yang setara dan perbandingan checksum yang benar-benar dijalankan.
Format laporan
Exporter yang tersedia menyediakan format berikut:
- JSON — hasil eksperimen dalam bentuk data terstruktur.
- CSV — data tabular untuk analisis lebih lanjut.
- Markdown — laporan teks terformat; laporan terperinci juga memuat tabel hasil validasi.
- HTML — laporan yang dapat dibuka menggunakan browser.
Kelengkapan konfigurasi eksperimen, jumlah worker, resource node, jenis workload, jumlah task, ukuran partition, strategi scheduling, execution time, throughput, speedup, efficiency, communication overhead, failure/retry, metrik node, dan kesimpulan harus diperiksa terhadap data aktual yang diberikan ke generator laporan. Field yang tidak tersedia tidak boleh dianggap telah diukur.
Batas integrasi
Perintah dashboard yang sebelumnya dijalankan menampilkan pesan Dashboard initialized successfully in skeleton mode. Pesan ini membuktikan inisialisasi skeleton, bukan dashboard web interaktif yang telah menampilkan data live dan grafik benchmark.
Integrasi dashboard dengan coordinator dan worker, visualisasi task aktif, network latency, grafik perbandingan, serta validasi checksum end-to-end perlu diverifikasi sebelum dinyatakan selesai. Exporter HTML dan Markdown juga tidak berarti ekspor PDF telah tersedia.
Verifikasi
Jalankan pengujian reporting:
python -m pytest tests/test_reporting_member7.py -v
Jalankan seluruh pengujian proyek:
python -m pytest tests/ -v
Periksa perubahan Git:
git diff --check
Pada verifikasi sebelumnya, 106 test lulus dan git diff --check tidak melaporkan kesalahan. Perubahan saat itu juga telah di-commit dan di-push ke branch fitur-dashboard-reporting. Hasil pengujian tersebut merupakan hasil pada saat verifikasi dilakukan dan tidak membuktikan bahwa seluruh integrasi dashboard sudah berjalan end-to-end.
📁 Struktur Folder Proyek**
```text
pycoremark/
├── README.md                  # Dokumentasi panduan arsitektur dan eksekusi
├── pyproject.toml             # Konfigurasi build sistem dan dependencies
├── .env.example               # Template environment variable konfigurasi cluster
├── .gitignore                 # Filter file sementara dan environment
├── compose.yaml               # Docker Compose multi-container virtual cluster
├── src/
│   └── pycoremark/
│       ├── __init__.py        # Versi dan metadata package
│       ├── common/            # Logging, config loader, dan domain exceptions
│       │   ├── __init__.py
│       │   ├── config.py
│       │   ├── exceptions.py
│       │   └── logging.py
│       ├── models/            # Domain model dataclasses & Enums (Kontrak Bersama)
│       │   ├── __init__.py
│       │   ├── node.py
│       │   ├── task.py
│       │   ├── workload.py
│       │   ├── experiment.py
│       │   ├── metrics.py
│       │   └── event.py
│       ├── cluster/           # Manajemen node cluster, heartbeat, & kegagalan
│       │   ├── __init__.py
│       │   ├── registry.py
│       │   ├── registration.py
│       │   ├── heartbeat.py
│       │   ├── detector.py
│       │   └── manager.py
│       ├── communication/     # Abstraksi broker, RPC, event bus, dan Lamport clock
│       │   ├── __init__.py
│       │   ├── broker.py
│       │   ├── rpc.py
│       │   ├── events.py
│       │   └── clock.py
│       ├── workloads/         # Generator beban kerja (CPU, data partition, mixed)
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── cpu.py
│       │   ├── data.py
│       │   ├── mixed.py
│       │   ├── registry.py
│       │   └── partitioner.py
│       ├── scheduling/        # Algoritma penjadwalan dan load balancing
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── static.py
│       │   ├── dynamic.py
│       │   └── resource_aware.py
│       ├── fault_tolerance/   # Retry policy, recovery, simulator chaos, DLQ
│       │   ├── __init__.py
│       │   ├── simulator.py
│       │   ├── retry.py
│       │   ├── recovery.py
│       │   ├── idempotency.py
│       │   └── dlq.py
│       ├── monitoring/        # Koleksi telemetri metrik dan distributed tracing
│       │   ├── __init__.py
│       │   ├── collector.py
│       │   ├── cluster_collector.py
│       │   ├── tracer.py
│       │   └── publisher.py
│       ├── analysis/          # Kalkulator throughput, speedup, efisiensi, overhead
│       │   ├── __init__.py
│       │   ├── analyzer.py
│       │   ├── metrics_calc.py
│       │   └── overhead.py
│       ├── storage/           # Antarmuka repositori & in-memory store
│       │   ├── __init__.py
│       │   ├── base.py
│       │   └── memory.py
│       ├── coordinator/       # Orkestrator pusat cluster, dispatcher & aggregator
│       │   ├── __init__.py
│       │   ├── service.py
│       │   ├── controller.py
│       │   ├── dispatcher.py
│       │   └── aggregator.py
│       ├── worker/            # Runtime node virtual worker & background agents
│       │   ├── __init__.py
│       │   ├── service.py
│       │   ├── executor.py
│       │   └── agents.py
│       ├── dashboard/         # Layanan visualisasi topologi & perbandingan hasil
│       │   ├── __init__.py
│       │   ├── service.py
│       │   ├── topology.py
│       │   └── comparison.py
│       ├── reporting/         # Validator hasil dan eksportir data (JSON/CSV)
│       │   ├── __init__.py
│       │   ├── validator.py
│       │   ├── exporters.py
│       │   └── generator.py
│       └── cli/               # CLI Entry point layanan aplikasi
│           ├── __init__.py
│           ├── coordinator.py
│           ├── worker.py
│           ├── dashboard.py
│           └── experiment.py
└── tests/                     # Unit test & import validation suite
    ├── __init__.py
    ├── test_imports.py
    ├── test_models.py
    ├── test_scheduler.py
    ├── test_lamport_clock.py
    └── test_fault_tolerance.py
```
---
### Implementasi Anggota 4 — Julius Einstein Chandra (241110272)
Modul `communication` mengimplementasikan abstraksi perpesanan terdistribusi (*message broker*), *remote procedure call* (RPC) dengan penanganan kegagalan & latensi, pengiriman event asinkron (*pub/sub event streaming*), serta jam logika (*Lamport Clock* & *Vector Clock*) untuk menjamin pengurutan kausalitas event di seluruh cluster.

| Komponen | File | Deskripsi Implementasi |
|---|---|---|
| **Message Broker** | `src/pycoremark/communication/broker.py` | Mendukung *wildcard topic matching* (`*` untuk single token, `#` untuk multi token), antrean thread-safe, *fanout pub/sub*, pembatasan masa hidup pesan (*message TTL*), dan perutean otomatis pesan basi ke *Dead-Letter Queue* (DLQ). |
| **RPC Client & Server** | `src/pycoremark/communication/rpc.py` | Antarmuka RPC sinkron (`call`), simulasi artificial network latency (`simulated_latency_ms`), jitter (`simulated_jitter_ms`), simulasi kegagalan transmisi (`simulated_failure_rate`), batas waktu eksekusi (*timeout*), serta registrasi handler dinamis. |
| **Event Streaming** | `src/pycoremark/communication/events.py` | `EventPublisher` dan `EventConsumer` untuk transmisi data `ClusterEvent` strongly-typed, sinkronisasi otomatis stempel jam logika, filter berbasis jenis event (`filter_types`) dan filter sumber node (`filter_sources`), serta operasi polling batch. |
| **Logical Clocks** | `src/pycoremark/communication/clock.py` | Thread-safe `LamportClock` untuk pengurutan linear waktu logika dan `VectorClock` untuk mendeteksi hubungan *happened-before* ($V_1 < V_2$) maupun konkurensi (*concurrent events*) antar-node. |

Contoh penggunaan RPC dan Broker:
```python
from pycoremark.communication import InMemoryMessageBroker, InMemoryRPCClient, InMemoryRPCServer, LamportClock, VectorClock

# 1. Message Broker dengan Wildcard
broker = InMemoryMessageBroker()
worker_sub = broker.subscribe("worker.*.tasks")
broker.publish("worker.node1.tasks", {"action": "START_BENCHMARK"})
msg = worker_sub.get_nowait().payload
assert msg["action"] == "START_BENCHMARK"

# 2. RPC dengan Timeout & Latensi
server = InMemoryRPCServer(node_id="worker-1")
server.register_handler("ping", lambda: "pong")
server.start()

client = InMemoryRPCClient(registry={"worker-1": server}, simulated_latency_ms=10.0)
res = client.call("worker-1", "ping", timeout=1.0)
assert res == "pong"

# 3. Vector Clock Concurrency
vc1 = VectorClock(local_node_id="nodeA")
vc2 = VectorClock(local_node_id="nodeB")
v1 = vc1.tick()
v2 = vc2.tick()
assert VectorClock.is_concurrent(v1, v2) is True
```

Verifikasi pengujian Member 4:
```bash
python -m pytest tests/test_communication.py -v
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
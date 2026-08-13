# SomniumDB

> A high-performance, asynchronous in-memory key-value store in modern C++17, powered by Linux `io_uring` and custom memory management.

---

## Overview

**SomniumDB** is a high-performance in-memory key-value store built in C++, inspired by the Redis architecture. It is engineered from the ground up to maximize throughput (**400,000+ OPS**) and minimize latency by leveraging:

* **Kernel-level asynchronous I/O** via Linux `io_uring`.
* **Zero-fragmentation memory management** with a custom C++ compliant pool allocator.
* **Cold storage tiering** using probabilistic data structures (Bloom Filters & CountMinSketch).
* **Fine-grained concurrency control** to eliminate lock contention under heavy multi-threaded workloads.

---

## Key Architectural Features

### Asynchronous I/O (`io_uring`)

Utilizes Linux's state-of-the-art I/O subsystem (`io_uring`) to process network sockets and disk operations asynchronously. By utilizing Submission and Completion queues shared directly with the kernel, it completely eliminates syscall overhead and context switching inherent in traditional `epoll`/`select` event loops.

### Custom C++ Compliant Pool Allocator

A high-speed memory allocator fully conforming to `std::allocator_traits`.

* **STL Compatibility:** Works seamlessly with standard STL containers (`std::vector`, `std::unordered_map`, etc.).
* **Performance:** Eliminates heap fragmentation and avoids expensive global locks from system `malloc`/`free`.

### Fine-Grained Locking Hierarchy

A multi-tiered concurrency model designed for parallel command execution:

* **Global Level:** `std::shared_mutex` allows multiple concurrent read operations across the database.
* **Partition Level:** Dedicated `std::mutex` per `Room` (partition). Commands targeting distinct partitions run completely in parallel with zero lock contention.

### Intelligent Cold Storage & Tiering

When RAM limits are reached, SomniumDB seamlessly shifts cold keys to disk:

* **CountMinSketch Algorithm:** Tracks access frequency to enforce a precise Least Frequently Used (LFU) eviction policy.
* **Bloom Filter:** Guardrail structure that prevents costly disk reads on non-existent keys (*cache miss optimization*).
* **Transparent Tiering:** Evicted keys are read back from cold storage automatically when requested by a client.

### Built-in Observability & Metrics

An embedded HTTP metrics server runs on port `9090` (`/metrics`). It exports real-time application metrics formatted for direct scraping by Prometheus and visualization via Grafana.

---

## Project Structure

```text
SomniumDB/
├── src/
│   ├── core/         # Engine orchestrator, RESP protocol parser, Room partitioning
│   ├── storage/      # Async I/O managers (aof_manager, snapshot_manager, eviction_manager)
│   └── utils/        # Probabilistic structures, pool allocator, Prometheus HTTP exporter
├── docker-compose.yml # Prometheus & Grafana stack configuration
└── CMakeLists.txt    # Build system configuration (-O3 optimized)

```

---

## Monitoring Stack (Prometheus & Grafana)

SomniumDB includes a ready-to-use Docker environment for real-time observability.

```bash
# Spin up Prometheus & Grafana in the background
docker-compose up -d

```

| Service | Endpoint / URL | Description |
| --- | --- | --- |
| **Metrics Endpoint** | `http://localhost:9090/metrics` | Raw Prometheus exposition format |
| **Grafana Dashboard** | `http://localhost:3000` | Real-time OPS, memory pools, and `io_uring` queues |

---

## Getting Started

### Prerequisites

Ensure your system meets the following requirements:

* **OS:** Linux Kernel 5.1+ (required for native `io_uring` support)
* **Compiler:** C++17 compliant compiler (GCC 8+ or Clang 7+)
* **Build Tools:** CMake 3.10+
* **Libraries:** `liburing` installed (`sudo apt install liburing-dev` on Debian/Ubuntu)

### Build Instructions

The build configuration automatically applies aggressive compiler optimizations (`-O3`).

```bash
# 1. Clone the repository
git clone https://github.com/username/SomniumDB.git
cd SomniumDB

# 2. Create and enter build directory
mkdir -p build && cd build

# 3. Configure and compile
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# 4. Launch the server
./Somnium

```

---

## Benchmarks & Performance Metrics

| Metric | Result | Notes |
| --- | --- | --- |
| **Throughput** | **400,000+ OPS** | Tested under pipelined `SET` / `GET` benchmarks |
| **I/O Latency** | Sub-microsecond | Powered by kernel submission/completion ring buffers |
| **Memory Overhead** | Low / Constant | Allocation pool reuses memory blocks without fragmenting heap |
| **Cold Key Fallback** | Transparent | Bloom Filter filters 100% of invalid disk lookups |

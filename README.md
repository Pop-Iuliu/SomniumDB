# SomniumDB

> A high-performance, asynchronous in-memory key-value store in modern C++20, powered by Linux `io_uring` and custom memory management.

---

## Overview

**SomniumDB** is a high-performance in-memory key-value store built in C++, inspired by the Redis architecture. It is engineered from the ground up to maximize throughput (**140,000+ unpipelined GET ops/s**, see Benchmarks) and minimize latency by leveraging:

* **Kernel-level asynchronous I/O** via Linux `io_uring`.
* **Zero-fragmentation memory management** with a custom C++ compliant pool allocator.
* **Cold storage tiering** with an exact `(room, key)` index, LFU eviction via CountMinSketch, and online compaction.
* **Fine-grained concurrency control** to eliminate lock contention under heavy multi-threaded workloads.

---

## Key Architectural Features

### Asynchronous I/O (`io_uring`)

Utilizes Linux's state-of-the-art I/O subsystem (`io_uring`) to process network sockets and disk operations asynchronously. By utilizing Submission and Completion queues shared directly with the kernel, it completely eliminates syscall overhead and context switching inherent in traditional `epoll`/`select` event loops.

### Custom C++ Compliant Pool Allocator

A high-speed memory allocator fully conforming to `std::allocator_traits`.

* **STL Compatibility:** Works seamlessly with standard STL containers (`std::vector`, `std::unordered_map`, etc.).
* **Performance:** Eliminates heap fragmentation and avoids expensive global locks from system `malloc`/`free`.

### Rooms: Partitions With a RAM Budget

Keys live in rooms (partitions). At most 3 rooms are active in RAM at once; idle rooms hibernate to a validated snapshot after 10 seconds and wake on their next command.

* **Explicit lifecycle:** `sleeping -> loading -> active -> hibernating -> sleeping`. The budget is enforced on both creation and wake; a full budget returns a clear `-ERR ROOMS FULL` instead of silently exceeding it. The `default` room is pinned.
* **Locking contract:** the registry `std::shared_mutex` is always taken before a room's `std::mutex`, never the reverse. Room state and last access are atomics, so the maintenance thread and `ROOM.INFO` never wait behind snapshot I/O.

### Intelligent Cold Storage & Tiering

When RAM limits are reached, SomniumDB seamlessly shifts cold keys to disk:

* **CountMinSketch Algorithm:** Tracks access frequency to drive an approximate Least Frequently Used (LFU) eviction policy.
* **Exact index:** An in-RAM `(room, key)` index points at the latest record in `cold.bin`, so a miss never touches the disk and a lookup reads exactly one record. A key lives in RAM or in cold storage, never both, so a deleted key can never resurface from disk.
* **Full fidelity:** Value, absolute expiry and CRDT version travel with the record through eviction and reload.
* **Compaction:** `COMPACT` rewrites only live records; the new file and index are published together, and a failed compaction keeps the old ones. Cold storage lasts for the process lifetime: on restart the AOF replays the full state into RAM.

### Built-in Observability & Metrics

An embedded HTTP metrics server runs on port `9090` (`/metrics`). It exports real-time application metrics formatted for direct scraping by Prometheus and visualization via Grafana.

### Commands

| Command | Description |
| --- | --- |
| `GET key`, `DEL key` | Read or delete; both see keys in cold storage |
| `SET key value [NX\|XX] [EX seconds\|PX milliseconds]` | Conditional and expiring writes, strictly validated; a failed condition replies nil and writes nothing |
| `MGET key [key ...]`, `MSET key value [key value ...]` | Multi-key read and atomic multi-key write (one AOF record) |
| `EXISTS key [key ...]` | Number of existing keys; repeated keys count each time |
| `EXPIRE key seconds [NX\|XX\|GT\|LT]`, `PEXPIRE key ms [...]`, `PERSIST key` | Set or remove a deadline (Redis 7 options; no deadline counts as infinite); a non-positive TTL deletes the key |
| `TTL key`, `PTTL key` | Remaining lifetime; `-1` persistent, `-2` missing |
| `CRDTMERGE key value timestamp node` | Last-writer-wins merge on `(timestamp, node)` |
| `ROOM name` | Select (and wake) a room |
| `ROOMS` | List known rooms |
| `ROOM.INFO name` | State, last access, resident keys |
| `ROOM.HIBERNATE name`, `ROOM.WAKE name` | Explicit room control |
| `COMPACT` | Compact cold storage; replies with reclaimed bytes |
| `REWRITEAOF` | Rewrite the AOF as current state (also automatic, see below) |
| `SUBSCRIBE channel [channel ...]`, `UNSUBSCRIBE [channel ...]`, `PUBLISH channel message` | Pub/Sub; `PING` and `UNSUBSCRIBE` also work while subscribed |
| `PING [message]`, `ECHO message`, `QUIT` | Connection commands |
| `HELLO [2\|3 [AUTH user pass] [SETNAME name]]` | Protocol negotiation; `HELLO 3` switches the connection to RESP3 (nulls, maps, Pub/Sub pushes, normal commands while subscribed) |
| `INFO`, `SAVE` | Server status, persistence health |

Commands can be sent as RESP arrays or inline (one line split on spaces, with `"..."` and `'...'` quoting as in `redis-cli`, e.g. from `telnet`), so `redis-cli` and `redis-benchmark` work unmodified, in RESP2 or RESP3.

Environment: `REDIS_PORT`, `SOMNIUM_AOF_SYNC` (`always`/`everysec`/`no`), `SOMNIUM_MAX_KEYS` (keys per room before eviction, default 1,000,000), `SOMNIUM_AOF_REWRITE_MIN_BYTES` (the AOF is rewritten automatically once it doubles past this size, default 64 MB), `SOMNIUM_NO_METRICS`, `SOMNIUM_CLOCK_OFFSET_MS` (shifts the clock, used by expiry tests).

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
* **Compiler:** C++20 compliant compiler (GCC 10+ or Clang 10+)
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

## Benchmarks

Measured head-to-head against **Redis 6.0.16** on the same machine, using `redis-benchmark` (100,000 operations per test). The reference Redis ran as a dedicated instance (empty dataset, no persistence), and SomniumDB ran in Release mode with a clean data directory and the default `SOMNIUM_AOF_SYNC=everysec` durability policy.

### Results

Measured after the S5 (persistence contracts) milestone. Both servers measured back to back in the same session:

| Workload | Redis 6.0.16 | SomniumDB | Delta |
| --- | --- | --- | --- |
| GET, 50 clients | 79,365 ops/s | **141,044 ops/s** | **+78%** |
| SET, 50 clients | 77,700 ops/s | **110,375 ops/s** | **+42%** |
| GET, 1 client | 22,957 ops/s | 25,733 ops/s | +12% |
| SET, 1 client | 27,609 ops/s | 24,015 ops/s | -13% |
| GET pipelined (-P 16) | 980,392 ops/s | **1,137,273 ops/s** | **+16%** |
| SET pipelined (-P 16) | 676,216 ops/s | 689,655 ops/s | +2% (parity) |
| 100k x 100B insert (deep pipeline) | 1,089,913 ops/s | 564,972 ops/s | -48% |
| RSS for 63k keys (100B values) | +13 MB | +17 MB | +29% |
| CRDTMERGE, 1 client pipelined | n/a | 263,158 ops/s | SomniumDB only |

For history: before the queued-output milestone, pipelined throughput collapsed to ~19k ops/s (a blocked sender stalled the whole event loop); the same workload now runs over 50x faster. The S5 milestone also removed the SET handicap: the per-command AOF append became a single POSIX `write()` and the `everysec` fsync moved off the command thread onto the watchdog.

### Correctness under load

Both servers passed an independent verification probe: 10,000 unique keys written and read back byte-for-byte (10,000/10,000 on both). SomniumDB's internal command counter matched the exact number of benchmark commands, and the Prometheus metrics stayed consistent with the real dataset throughout the run.

### Interpretation

* **Unpipelined GET wins by ~1.8x.** The `io_uring` poll-read-write loop has less per-request overhead than Redis's epoll path on this kernel (6.8) and hardware.
* **SET is no longer taxed.** With the AOF write reduced to one syscall per record and durability fsyncs running on the watchdog thread (policy `everysec`), SET outperforms Redis unpipelined and reaches pipelined parity. Choose `SOMNIUM_AOF_SYNC=always` when durability must cover power loss on every acknowledged write (throughput drops accordingly); `no` for maximum speed when only crash-safety against process death is needed.
* **Deep pipelines with large values still trail ~2x.** The remaining gap comes from per-command allocations and RESP parsing in the dispatcher, not from persistence.
* **Memory is +29% per key**, coming from per-record CRDT metadata and pool slack.

### How to reproduce

```bash
# SomniumDB in Release mode on a free port (REDIS_PORT overrides the default 6379)
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release
mkdir -p /tmp/somnium-bench && cd /tmp/somnium-bench
REDIS_PORT=6380 SOMNIUM_NO_METRICS=1 /path/to/build-release/Redis &

# Dedicated reference Redis (does not touch any system instance)
redis-server --port 6399 --save "" --appendonly no --dir /tmp/somnium-bench2 &

# Run the same matrix
redis-benchmark -h 127.0.0.1 -p 6399 -t set,get -c 50 -n 100000 --csv
redis-benchmark -h 127.0.0.1 -p 6380 -t set,get -c 50 -n 100000 --csv
redis-benchmark -h 127.0.0.1 -p 6399 -t set,get -c 1  -n 50000 --csv
redis-benchmark -h 127.0.0.1 -p 6380 -t set,get -c 1  -n 50000 --csv
redis-benchmark -h 127.0.0.1 -p 6399 -t set,get -c 50 -n 100000 -P 16 --csv
redis-benchmark -h 127.0.0.1 -p 6380 -t set,get -c 50 -n 100000 -P 16 --csv

# Deep pipeline with 100B values over a random 100k keyspace (also used for the RSS delta:
# measure VmRSS of each server before/after; -r makes the keys distinct)
redis-benchmark -h 127.0.0.1 -p 6399 -t set -c 50 -n 100000 -d 100 -r 100000 --csv
redis-benchmark -h 127.0.0.1 -p 6380 -t set -c 50 -n 100000 -d 100 -r 100000 --csv
```

Numbers vary across machines, kernels, and virtualization. The meaningful comparison is relative behavior between the two servers on identical hardware, run back to back.

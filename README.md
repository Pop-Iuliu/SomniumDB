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

An embedded HTTP metrics server runs on `127.0.0.1:9090` (`/metrics`; the address is set by `SOMNIUM_METRICS_BIND`). It exports real-time application metrics formatted for direct scraping by Prometheus and visualization via Grafana.

### Commands

| Command | Description |
| --- | --- |
| `GET key`, `DEL key` | Read or delete; both see keys in cold storage |
| `SET key value [NX\|XX] [EX seconds\|PX milliseconds]` | Conditional and expiring writes, strictly validated; a failed condition replies nil and writes nothing |
| `MGET key [key ...]`, `MSET key value [key value ...]` | Multi-key read and atomic multi-key write (one AOF record); an `MGET` reply over 32 MB is refused with `-ERR reply too large` |
| `EXISTS key [key ...]` | Number of existing keys; repeated keys count each time |
| `EXPIRE key seconds [NX\|XX\|GT\|LT]`, `PEXPIRE key ms [...]`, `PERSIST key` | Set or remove a deadline (Redis 7 options; no deadline counts as infinite); a non-positive TTL deletes the key |
| `TTL key`, `PTTL key` | Remaining lifetime; `-1` persistent, `-2` missing |
| `CRDTMERGE key value timestamp node [expire_at]` | Last-writer-wins merge on `(timestamp, node)`; timestamps are Hybrid Logical Clock values (48-bit ms, 16-bit counter), plain milliseconds are accepted and converted; the optional absolute deadline (ms) travels with the state |
| `CRDTDEL key timestamp node` | Versioned delete (replication); loses against a newer write |
| `REPLFRONTIER node frontier` | Replication heartbeat: every write of `node` up to `frontier` has been delivered |
| `ROOM name` | Select (and wake) a room; names are 1 to 200 bytes without `/` or NUL, since they become file names |
| `ROOMS` | List known rooms |
| `ROOM.INFO name` | State, last access, resident keys and estimated bytes |
| `ROOM.HIBERNATE name`, `ROOM.WAKE name` | Explicit room control |
| `COMPACT` | Compact cold storage; replies with reclaimed bytes |
| `REWRITEAOF` | Rewrite the AOF as current state in the background: a forked child writes a copy-on-write snapshot while clients keep being served; `INFO` shows progress (also automatic, see below) |
| `SUBSCRIBE channel [channel ...]`, `UNSUBSCRIBE [channel ...]`, `PUBLISH channel message` | Pub/Sub; `PING` and `UNSUBSCRIBE` also work while subscribed |
| `PING [message]`, `ECHO message`, `QUIT` | Connection commands |
| `AUTH [default] password` | Authenticate the connection when `SOMNIUM_PASSWORD` is set; until then only `AUTH`, `HELLO` and `QUIT` are accepted, and a connection that has not authenticated within 10 seconds is closed |
| `HELLO [2\|3 [AUTH user pass] [SETNAME name]]` | Protocol negotiation (and authentication in the same step); `HELLO 3` switches the connection to RESP3 (nulls, maps, Pub/Sub pushes, normal commands while subscribed) |
| `INFO`, `SAVE` | Server status, persistence health |

Commands can be sent as RESP arrays or inline (one line split on spaces, with `"..."` and `'...'` quoting as in `redis-cli`, e.g. from `telnet`), so `redis-cli` and `redis-benchmark` work unmodified, in RESP2 or RESP3.

Environment: `REDIS_PORT`, `SOMNIUM_BIND` (IPv4 address to listen on, default `127.0.0.1`; refuses to start on a non-loopback address without a password), `SOMNIUM_PASSWORD` (require `AUTH` from every client; replication peers send it too, so all nodes of a cluster share one password. Like Redis `requirepass`, an authenticated client is fully trusted, including with replication and admin commands), `SOMNIUM_MAXCLIENTS` (maximum simultaneous connections, default 10,000; extra connections get `-ERR max number of clients reached` and are closed), `SOMNIUM_AOF_SYNC` (`always`/`everysec`/`no`), `SOMNIUM_MAX_ROOMS` (maximum number of known rooms, default 10,000; a new name beyond it gets `-ERR too many rooms`, existing rooms are unaffected), `SOMNIUM_MAX_KEYS` (keys per room before eviction, default 1,000,000), `SOMNIUM_MAXMEMORY` (global budget in bytes for resident keys and values, estimated; default unlimited), `SOMNIUM_AOF_REWRITE_MIN_BYTES` (the AOF is rewritten automatically once it doubles past this size, default 64 MB), `SOMNIUM_NODE_ID` (CRDT node id of local writes, default 1), `SOMNIUM_PEERS` (`host:port,...`: replicate local writes and deletes to these nodes as batched CRDT merges, one thread per peer, assuming a symmetric peer list; delete markers are garbage-collected once every peer's frontier has passed them; per-peer phi and lag appear in `INFO` and `/metrics`), `SOMNIUM_MAX_CLOCK_OFFSET_MS` (reject merges whose timestamp is further in the future, default 10 minutes), `SOMNIUM_METRICS_BIND` (IPv4 address of the metrics exporter, default `127.0.0.1`), `SOMNIUM_NO_METRICS`, `SOMNIUM_CLOCK_OFFSET_MS` (shifts the clock, used by expiry tests).

Data files (`appendonly.aof`, `room_*.bin`, `cold.bin`, replication offsets) are created readable by the server's user only. Files created by older versions keep their mode: run `chmod 600` on them once after upgrading.

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
# Prometheus in Docker scrapes host.docker.internal:9090, which a localhost
# bind does not reach: expose the exporter, and firewall port 9090 from the network
SOMNIUM_METRICS_BIND=0.0.0.0 ./Redis &

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

## Security

To report a vulnerability, see [`SECURITY.md`](SECURITY.md). The threat model and the hardening work are tracked in [`securityprogress.md`](securityprogress.md).

Deployment checklist:

* Run the server as a dedicated non-root user.
* Start it in a working directory owned by that user with mode `0700`: the data files (`appendonly.aof`, `room_*.bin`, `cold.bin`) are written there.
* Set `SOMNIUM_PASSWORD` to a long random value (for example `openssl rand -hex 32`). Every client and replication peer that knows it is fully trusted.
* Keep `SOMNIUM_BIND` and `SOMNIUM_METRICS_BIND` on localhost unless something outside the host needs them.
* Firewall the server port (`6379` by default) and the metrics port (`9090`).
* Traffic is not encrypted: put a tunnel in front of anything that crosses a network (see below).

### Encrypted transport

SomniumDB speaks plain TCP, so the password and the data are readable on the wire. Encrypt everything that leaves the host with a tunnel; the server itself stays as it is.

**Replication: WireGuard.** Put the nodes on a private WireGuard network and replicate over its addresses. On node 1 (`10.0.0.1`); node 2 mirrors it:

```ini
# /etc/wireguard/wg0.conf  (keys: wg genkey | tee private.key | wg pubkey > public.key)
[Interface]
Address = 10.0.0.1/24
ListenPort = 51820
PrivateKey = <node 1 private key>

[Peer]
PublicKey = <node 2 public key>
AllowedIPs = 10.0.0.2/32
Endpoint = node2.example.com:51820
```

```bash
wg-quick up wg0
SOMNIUM_BIND=10.0.0.1 SOMNIUM_PASSWORD=<shared secret> SOMNIUM_PEERS=10.0.0.2:6379 ./Redis
```

Firewall port 6379 everywhere except on `wg0`.

**Remote clients: stunnel.** The server stays on localhost; stunnel terminates TLS next to it, and a second stunnel on the client machine verifies the server's certificate:

```bash
openssl req -x509 -newkey rsa:3072 -nodes -days 365 -subj /CN=somnium \
    -keyout somnium.key -out somnium.crt   # copy somnium.crt (not the key) to the clients
```

```ini
; server host: TLS on 6380 -> SomniumDB on localhost
[somnium]
accept = 0.0.0.0:6380
connect = 127.0.0.1:6379
cert = /etc/stunnel/somnium.crt
key = /etc/stunnel/somnium.key
```

```ini
; client host: plain on localhost:6379 -> TLS to the server, certificate pinned
client = yes
[somnium]
accept = 127.0.0.1:6379
connect = db.example.com:6380
verifyPeer = yes
CAfile = /etc/stunnel/somnium.crt
```

Clients then connect to their own `127.0.0.1:6379` (`redis-cli -a <password>`). The tunnel makes the server reachable from the network, so `SOMNIUM_PASSWORD` must be set even though `SOMNIUM_BIND` is still localhost. A replication peer can use the same client-side stunnel when WireGuard is not an option: point `SOMNIUM_PEERS` at the local stunnel port.

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

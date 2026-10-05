# Security Progress

A step-by-step plan for securing SomniumDB. Each step is small enough for one pull request, closes one concrete hole, and comes with a check that proves it.

**YAGNI rules for this plan**

- Fix real, present holes first. The cheapest fix that closes the hole wins.
- Secure defaults over options. Add a setting only when a real deployment needs to change the default.
- Nothing speculative. Anything without a concrete threat or a user asking for it goes to the deferred list at the end, together with the trigger that would bring it back.
- Every step ships with its test, in `tests/sec_test.py` (one file that grows step by step, registered in `tests/run_all.sh`) unless stated otherwise.

**How to use this file:** do the steps in order, one PR each. When a step is merged, set its status to `DONE`, add the PR number, and note anything that changed from the plan.

## Status

| Step | Title | Phase | Status |
| --- | --- | --- | --- |
| SEC-1 | Listen on localhost by default | 1. Secure defaults | DONE |
| SEC-2 | Password authentication (`AUTH`) | 1. Secure defaults | TODO |
| SEC-3 | Private data files | 1. Secure defaults | TODO |
| SEC-4 | Remove automatic real-time priority | 1. Secure defaults | TODO |
| SEC-5 | Safe room names | 2. Limits | TODO |
| SEC-6 | Maximum number of clients | 2. Limits | TODO |
| SEC-7 | Maximum number of rooms | 2. Limits | TODO |
| SEC-8 | Harden the metrics endpoint | 2. Limits | TODO |
| SEC-9 | UndefinedBehaviorSanitizer in CI | 3. Find bugs first | TODO |
| SEC-10 | Fuzz the RESP parser | 3. Find bugs first | TODO |
| SEC-11 | Explicit build hardening | 3. Find bugs first | TODO |
| SEC-12 | Least-privilege CI token | 3. Find bugs first | TODO |
| SEC-13 | Remove the unused vendored `json.hpp` | 3. Find bugs first | TODO |
| SEC-14 | `SECURITY.md` and a deployment checklist | 4. Process and transport | TODO |
| SEC-15 | Encrypted transport through a tunnel (docs) | 4. Process and transport | TODO |
| SEC-16 | Native TLS | 4. Process and transport | DEFERRED (trigger below) |

## Threat model

Kept short on purpose. Update it when a step changes an assumption.

**What we protect**

- The data: keys and values in RAM, `appendonly.aof`, room snapshots (`room_*.bin`), `cold.bin`.
- Availability of the server and of the host it runs on.

**Trusted**

- The host operating system and its administrators.
- Whoever sets the environment variables (the operator).
- Disk contents. They may be corrupted (S5 handles that), but are not assumed to be malicious.
- After SEC-2: anyone who knows the password, including the replication peers. Like Redis `requirepass`, an authenticated client is fully trusted.

**Untrusted**

- Every network client before it authenticates.
- The network between clients and server, and between replication peers, until SEC-15 or SEC-16.
- Other unprivileged users on the same machine.

**Out of scope for now** (see the deferred list): malicious authenticated clients, malicious peers, side channels, encryption at rest.

## Current gaps

An audit snapshot of `main` on 2026-10-04 (after PR #9). Each gap maps to a step.

| # | Gap | Where | Step |
| --- | --- | --- | --- |
| 1 | The server listens on all interfaces (`INADDR_ANY`) with no authentication: anyone who can reach the port can read, write and delete everything | `main.cpp`, listener setup in `main()` | SEC-1, SEC-2 |
| 2 | Admin and replication commands are open to any client: `REWRITEAOF`, `COMPACT`, `ROOM.HIBERNATE`, `CRDTMERGE`/`CRDTDEL` with any node id, and `REPLFRONTIER`, which can push the tombstone watermark forward so deletes are forgotten early and incoming writes are ignored | `src/core/database.cpp`, `execute()` | SEC-2 |
| 3 | Data files are created world-readable (`0644`), so any local user can read the whole dataset | `aof_manager.cpp`, `snapshot_manager.cpp`, `eviction_manager.cpp`, the replication offset files | SEC-3 |
| 4 | A burst of more than 500 events per loop turn switches the main thread to `SCHED_FIFO` at maximum priority; on a privileged process a client flood can starve the whole host | `main.cpp`, `adjust_thread_priority()` | SEC-4 |
| 5 | Room names go into file names unchecked (`"room_" + name + ".bin"`): `/` and NUL bytes are accepted, and names over 255 bytes make hibernation fail forever, pinning an active-room slot | `src/storage/snapshot_manager.cpp` | SEC-5 |
| 6 | No limit on connections: file descriptors and per-client buffers (up to 128 MB input and 32 MB output each) can be exhausted | `main.cpp`, accept handling | SEC-6 |
| 7 | Every new room name stays in the registry forever, even when `ROOM` is refused for lack of a slot | `Database::wake_room()` / `find_room()` | SEC-7 |
| 8 | The metrics exporter listens on all interfaces, handles one connection at a time with a blocking `read` (one idle connection stops it), leaves `sockaddr_in` uninitialized, and passes `SO_REUSEADDR \| SO_REUSEPORT` as one option name | `metrics.cpp`, `prometheus_thread()` | SEC-8 |
| 9 | No UndefinedBehaviorSanitizer and no fuzzing of the network parser | `.github/workflows/ci.yml` | SEC-9, SEC-10 |
| 10 | Hardening depends on distribution compiler defaults, the CI token has default permissions, and an unused 1 MB vendored header (`json.hpp`) is listed in the build | `CMakeLists.txt`, `ci.yml`, `json.hpp` | SEC-11 to SEC-13 |
| 11 | No way to report a vulnerability and no deployment guidance | repository root | SEC-14 |
| 12 | All traffic, including replication, is plain text | network | SEC-15, SEC-16 |

## Phase 1: Secure defaults

### SEC-1: Listen on localhost by default

**Why:** gap 1. Today any host that can route to port 6379 has full access.

**Do:**
1. In `main()`, replace `INADDR_ANY` with the address from `SOMNIUM_BIND`, default `127.0.0.1`, parsed with `inet_pton(AF_INET, ...)`. An invalid value prints an error and exits with code 1.
2. Print the address and port actually bound at startup.
3. If `SOMNIUM_BIND` is not a loopback address (`127.0.0.0/8`), print a warning that the server is reachable from the network without authentication. SEC-2 turns this warning into a refusal.
4. Document `SOMNIUM_BIND` in the README environment list.

**Done when:**
- A default start accepts connections on `127.0.0.1` and the log shows `127.0.0.1:<port>`.
- `SOMNIUM_BIND=not-an-ip` exits with code 1.
- The whole existing suite still passes (it already connects to `127.0.0.1`).

**Files:** `main.cpp`, `README.md`, `tests/sec_test.py`.

### SEC-2: Password authentication (`AUTH`)

**Why:** gaps 1 and 2. A localhost bind is not enough once nodes replicate across machines.

**Do:**
1. Read the password from `SOMNIUM_PASSWORD`. Unset means no authentication, which keeps today's behavior on localhost.
2. Add `bool authenticated` to `Client` in `main.cpp`. It starts as `true` when there is no password and `false` otherwise. Authentication is connection state, like `resp3`, so handle it in `process_buffered()` next to `QUIT` and `HELLO`.
3. While not authenticated, allow only `AUTH`, `HELLO` and `QUIT`. Answer everything else with `-NOAUTH Authentication required.`
4. `AUTH <password>` and `AUTH default <password>`: compare in constant time (loop over the full expected length and accumulate differences, never return early). Success replies `+OK`; failure replies `-WRONGPASS invalid username-password pair` and logs the client fd. With no password configured, `AUTH` replies with an error, as Redis does.
5. `HELLO ... AUTH <user> <pass>`: use the same check instead of today's accept-anything. `HELLO` without `AUTH` from an unauthenticated client replies `-NOAUTH`.
6. Limit pre-authentication input: if an unauthenticated client has more than 16 KB buffered, close it. Today a stranger can make the server hold up to 128 MB per connection.
7. Replication: when `SOMNIUM_PASSWORD` is set, the replicator sends `AUTH <password>` as the first command after each connect, counting its reply like any other. All nodes of a cluster share one password.
8. Startup: if `SOMNIUM_BIND` is not loopback and `SOMNIUM_PASSWORD` is empty, refuse to start with an error that names both variables.
9. Document both variables and the "every authenticated client is fully trusted" rule in the README.

**Done when:**
- With a password set: `GET` before auth gets `-NOAUTH`, a wrong password gets `-WRONGPASS`, the right one gets `+OK` and then everything works; `HELLO 3 AUTH default <pw>` works in one step.
- `redis-cli -a <pw>` and `redis-cli -3 -a <pw>` run commands.
- An unauthenticated client sending 20 KB is disconnected.
- Two nodes with the same password replicate (reuse the S10 scenario); a node with a wrong password does not.
- `SOMNIUM_BIND=0.0.0.0` without a password refuses to start.

**Files:** `main.cpp`, `src/storage/replicator.*`, `README.md`, `tests/sec_test.py`.

### SEC-3: Private data files

**Why:** gap 3. The dataset is readable by every local user.

**Do:**
1. Call `umask(077)` at the top of `main()`. Every file and directory the server creates afterwards (AOF, temp files, snapshots, `cold.bin`, replication offsets) becomes owner-only. One line instead of changing every `open(..., 0644)` call.
2. Change the explicit `0644` modes in `open()` calls to `0600`, so the intent is visible in the code.
3. README: existing files keep their old mode, so run `chmod 600` on them once after upgrading.

**Done when:** after a `SET`, `ROOM.HIBERNATE` and `COMPACT`, `appendonly.aof`, the room snapshot and `cold.bin` all have mode `0600` (check with `os.stat` in the test).

**Files:** `main.cpp`, the four storage `.cpp` files, `README.md`, `tests/sec_test.py`.

### SEC-4: Remove automatic real-time priority

**Why:** gap 4. A load spike, which a client can cause, should never raise the process above every other process on the host.

**Do:** delete `adjust_thread_priority()` and its call at the end of the event loop. If real-time scheduling is ever wanted, the operator can set it from outside (`chrt`, systemd `CPUSchedulingPolicy`).

**Done when:** the function is gone, `sched.h` is no longer needed, and the suite and benchmark job pass.

**Files:** `main.cpp`.

## Phase 2: Limits

### SEC-5: Safe room names

**Why:** gap 5. Room names become file names.

**Do:**
1. In `Database::wake_room()`, the single entry point for `ROOM` and `ROOM.WAKE` (replicated `ROOM`s go through it too), reject names that are empty, longer than 200 bytes, or contain `/` or a NUL byte. Reply `-ERR invalid room name`.
2. Leave AOF replay alone: it only contains rooms that were accepted live.

**Done when:** `ROOM ""`, `ROOM a/b`, a name containing `\0` and a 201-byte name are refused; `ROOM LIST`, `ROOM SET` and a 200-byte name still work and can hibernate and wake.

**Files:** `src/core/database.cpp`, `tests/sec_test.py`.

### SEC-6: Maximum number of clients

**Why:** gap 6. Unlimited connections exhaust file descriptors and memory.

**Do:**
1. Add `SOMNIUM_MAXCLIENTS`, default 10000 (the Redis default).
2. In the accept branch of the event loop, if `clients.size()` has reached the limit, write `-ERR max number of clients reached\r\n` to the new socket, close it, and re-arm accept. Do not create a `Client`.

**Done when:** with `SOMNIUM_MAXCLIENTS=2`, a third connection receives the error and is closed, and after one client disconnects a new one is accepted.

**Files:** `main.cpp`, `README.md`, `tests/sec_test.py`.

### SEC-7: Maximum number of rooms

**Why:** gap 7. A client can grow the registry forever by trying new room names.

**Do:**
1. Add `SOMNIUM_MAX_ROOMS`, default 10000.
2. In `Database::wake_room()`, if the name is new and the registry already holds that many rooms, reply `-ERR too many rooms`. Existing rooms are unaffected, and AOF replay is not limited.

**Done when:** with `SOMNIUM_MAX_ROOMS=3`, a fourth new name is refused while the first three keep working.

**Files:** `src/core/database.*`, `README.md`, `tests/sec_test.py`.

### SEC-8: Harden the metrics endpoint

**Why:** gap 8. The metrics port leaks internal state to the network, and one idle connection silences monitoring.

**Do:**
1. Bind to `SOMNIUM_METRICS_BIND`, default `127.0.0.1`.
2. Zero-initialize `sockaddr_in` (`sockaddr_in address{};`).
3. Set `SO_REUSEADDR` alone. The combined `SO_REUSEADDR | SO_REUSEPORT` value is not a valid option name.
4. Set a 1 second `SO_RCVTIMEO` and `SO_SNDTIMEO` on every accepted socket, so an idle or slow scraper is dropped instead of blocking the exporter.
5. README and `docker-compose.yaml`: Prometheus in Docker scrapes `host.docker.internal:9090`, which a localhost bind does not reach. Document `SOMNIUM_METRICS_BIND=0.0.0.0` for that setup, together with a firewall rule.

**Done when:** a connection that sends nothing does not stop a second scrape from succeeding within 2 seconds, and the S1 metrics test still passes.

**Files:** `metrics.cpp`, `README.md`, `tests/sec_test.py`.

## Phase 3: Find bugs before attackers do

### SEC-9: UndefinedBehaviorSanitizer in CI

**Why:** memory-safety bugs are the most likely way into a C++ server. ASan and TSan already run; UBSan catches a different class (overflow, misaligned access, invalid shifts, bad casts).

**Do:** in the CI "Debug + ASan" configure step, change the flags to `-fsanitize=address,undefined -fno-sanitize-recover=undefined -g`. Fix whatever it reports.

**Done when:** the full suite passes under ASan + UBSan.

**Files:** `.github/workflows/ci.yml`, plus any fixes.

### SEC-10: Fuzz the RESP parser

**Why:** `resp::parse` and `resp::parse_request` read raw bytes from strangers. They are header-only and have no dependencies, so they are cheap to fuzz.

**Do:**
1. Add `tests/fuzz_resp.cpp` with an `LLVMFuzzerTestOneInput` that runs both parsers over the input in a loop, the way `process_buffered()` does, and asserts: a `Complete` result always moves `end` forward and never past the buffer, and parsing the same bytes twice gives the same status, end offset and arguments. The sanitizers catch the rest (out-of-bounds reads, overflow).
2. Add a CI step that installs `clang`, builds with `clang++ -std=c++20 -g -O1 -fsanitize=fuzzer,address,undefined tests/fuzz_resp.cpp -o fuzz_resp`, and runs `./fuzz_resp -max_total_time=60`. No CMake changes are needed.
3. Any crash found becomes a regular case in `tests/resp_test.cpp` before it is fixed.

**Done when:** the fuzz step runs for 60 seconds per CI run without findings.

**Next targets** (only after this works, and when the related format changes): the snapshot loader and AOF recovery.

**Files:** `tests/fuzz_resp.cpp`, `.github/workflows/ci.yml`.

### SEC-11: Explicit build hardening

**Why:** Ubuntu's GCC turns most hardening on by default, but other distributions and compilers may not. The binary should be hardened wherever it is built.

**Do:** in `CMakeLists.txt`:
- `add_compile_options(-fstack-protector-strong)`
- for Release builds, `-U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=2` (the `-U` avoids redefinition warnings where the compiler already sets it)
- `set(CMAKE_POSITION_INDEPENDENT_CODE ON)` and `add_link_options(-pie -Wl,-z,relro,-z,now)`

**Done when:** for the Release binary, `readelf -h` shows `Type: DYN` and `readelf -d` shows `BIND_NOW`. Add this check as one CI step.

**Files:** `CMakeLists.txt`, `.github/workflows/ci.yml`.

### SEC-12: Least-privilege CI token

**Why:** the workflow only needs to read the repository. A compromised build step should not get a token that can write.

**Do:** add `permissions: contents: read` at the top level of `.github/workflows/ci.yml`. The artifact upload still works with it.

**Done when:** CI is green with the restricted token.

**Files:** `.github/workflows/ci.yml`.

### SEC-13: Remove the unused vendored `json.hpp`

**Why:** about 1 MB of third-party code that nothing includes is still listed in the build. Code that isn't there can't have vulnerabilities.

**Do:** delete `json.hpp` and remove it from `add_executable` in `CMakeLists.txt`.

**Done when:** the build and the suite pass.

**Files:** `json.hpp`, `CMakeLists.txt`.

## Phase 4: Process and transport

### SEC-14: `SECURITY.md` and a deployment checklist

**Why:** people who find a vulnerability need a private way to report it, and operators need to know how to run the server safely.

**Do:**
1. `SECURITY.md`:
   - Supported versions: `main` only.
   - How to report: GitHub private vulnerability reporting. The repository owner turns it on under Settings > Code security.
   - What to expect: an acknowledgement within 7 days. No fixed SLA beyond that.
2. A "Security" section in the README with a short deployment checklist:
   - Run as a dedicated non-root user.
   - Data directory owned by that user, mode `0700`.
   - Set `SOMNIUM_PASSWORD` to a long random value.
   - Keep `SOMNIUM_BIND` and `SOMNIUM_METRICS_BIND` on localhost unless needed.
   - Firewall the ports.
   - Use a tunnel for anything that crosses a network (SEC-15).

**Done when:** both documents exist and the README links to `SECURITY.md`.

**Files:** `SECURITY.md`, `README.md`.

### SEC-15: Encrypted transport through a tunnel (docs)

**Why:** gap 12. Passwords and data cross the network in plain text. A tunnel encrypts everything today without adding TLS code to the server.

**Do:** a short README guide:
- **Replication:** run the nodes over WireGuard and point `SOMNIUM_PEERS` at the WireGuard addresses.
- **Remote clients:** put `stunnel` (or `spiped`) in front of the server, with a minimal config example. The server itself stays bound to localhost.

**Done when:** the guide exists and a manual run of both setups works once.

**Files:** `README.md`.

### SEC-16: Native TLS (deferred)

**Trigger:** the first real deployment that cannot use a tunnel, for example a managed client library that requires `rediss://`.

**Sketch for when it comes:**
- **Server side:** OpenSSL with memory BIOs. The io_uring loop keeps moving raw bytes, and TLS runs inside `Client` before parsing.
- **Replicator side:** a plain blocking `SSL_connect` on its existing per-peer sockets.
- **Configuration:** certificate and key paths through `SOMNIUM_TLS_CERT` and `SOMNIUM_TLS_KEY`.

## Deferred (YAGNI)

Each item comes back only when its trigger happens.

| Item | Trigger |
| --- | --- |
| Native TLS (SEC-16) | A deployment that cannot use a tunnel |
| ACLs, multiple users, per-command permissions | A need to give an untrusted application restricted access |
| Separate peer credentials, authenticated `REPLFRONTIER` | Clients and replication peers living in different trust zones |
| Rate limiting or lockout of failed `AUTH` attempts | The password port being exposed beyond localhost or a tunnel (prefer long random passwords first) |
| Encryption at rest | A compliance requirement; until then use disk encryption (LUKS) |
| Counting client buffers in `SOMNIUM_MAXMEMORY` | Memory exhaustion from client buffers in practice |
| Fuzzing the snapshot loader and AOF recovery | After SEC-10 runs cleanly, or when either file format changes |
| Audit logging of commands | A compliance requirement |
| Static analysis (CodeQL) | After SEC-9 and SEC-10 run clean, as a cheap extra pass |
| Pinning GitHub Actions to commit SHAs, Dependabot | Adding any third-party action beyond `actions/*` |
| IPv6 bind addresses | A deployment that needs them |

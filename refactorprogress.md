# Refactor Progress

A step-by-step plan for restructuring SomniumDB after the security plan (`securityprogress.md`) was completed. Each step is one pull request, has a single purpose and states how it is verified.

## Objectives

1. **Make the structure match the design.** The architecture (rooms, cold storage, CRDT replication, io_uring) is sound, but two files carry most of it: `src/core/database.cpp` (1,007 lines) and `main.cpp` (686 lines).
2. **Make implicit rules explicit.** Several properties are decided by where code happens to sit, not by anything written down. One of them already caused a bug that went unnoticed: in SEC-21, `INFO` woke a hibernated room.
3. **Keep every guarantee already earned.** The test suite, the sanitizers, both fuzzers and `tests/sec_test.py` define the behaviour; a refactor that needs a test change is not a refactor.

## Ground rules

- **One step, one pull request.** Branch from the latest `origin/main`; when a step is merged, set its status to `DONE`, add the PR number, and note anything that changed from the plan.
- **Behaviour-preserving by default.** A step changes behaviour only where it says so under **Behaviour change**, and then it ships with a test. Every other step must pass CI with no test changes.
- **Moves and edits never share a commit.** Moved code is moved verbatim, so its review is `git diff --color-moved`; any edit a step needs sits in its own commit.
- **CI is the gate.** All four jobs green: the ASan + UBSan suite, TSan, both fuzzers and the benchmark. The benchmark's GET and SET numbers must stay within run-to-run noise of `main`.
- **Security invariants are not refactored away.** They must still hold, in one obvious place, after every step:
  - the `AUTH` gate before any command reaches the database (SEC-2);
  - buffer accounting after every buffer change (SEC-22), and the per-client caps (SEC-17, SEC-19);
  - room-name validation in `wake_room()` (SEC-5);
  - owner-only data files (SEC-3);
  - TLS between the socket and the buffers (SEC-16).

## Status

| Step | Title | Phase | Status |
| --- | --- | --- | --- |
| REF-1 | Construct the server after startup checks | 1. Foundations | DONE (#35) |
| REF-2 | One source of configuration | 1. Foundations | DONE (#36) |
| REF-3 | Split `database.cpp` by concern | 2. Structure | DONE (#37) |
| REF-4 | A command table | 2. Structure | DONE (#38) |
| REF-5 | Split `main.cpp`: connection layer and authentication | 2. Structure | IMPLEMENTED (awaiting CI) |
| REF-6 | Source layout | 3. Layout and tests | TODO |
| REF-7 | Shared test helpers | 3. Layout and tests | TODO |

## Current state

Findings from reading `main` at `39aac90` (2026-10-16). Each maps to a step. Rows 1 and 2 are latent bugs found while planning; they were read from the code, not reproduced.

| # | Finding | Where | Step |
| --- | --- | --- | --- |
| 1 | The `Database` is a global, so it is built during static initialization, before `main()` runs. Its constructor replays and may migrate the AOF, and its `EvictionManager` opens `cold.bin` with `O_TRUNC`. A second instance started by mistake in the same directory truncates the running instance's `cold.bin` before failing to bind the port. The running instance then reads its evicted keys as missing until it restarts (the AOF still holds them), so `GET` returns nil and `SET NX` can overwrite them. This ordering is also why SEC-3 needed explicit `0600` modes: `umask` runs too late | `main.cpp:42`, `eviction_manager.cpp:46` | REF-1 |
| 2 | 25 `getenv` calls are spread over 7 files, and three variables are parsed twice with different rules. `SOMNIUM_PEERS`: `Database` counts every non-empty entry, while `Replicator` skips malformed ones. Tombstone collection waits to hear from `peer_count_` peers, so one malformed entry silently stops it for good, and tombstones then grow without bound. `SOMNIUM_NODE_ID` and `SOMNIUM_PASSWORD` are also read in two places. Invalid values exit in `main.cpp` but fall back with a warning elsewhere | `main.cpp`, `database.cpp`, `replicator.cpp`, the storage managers | REF-2 |
| 3 | `database.cpp` holds five concerns: dispatch, room lifecycle, key commands, CRDT versions and tombstones, and persistence (replay, background rewrite) | `src/core/database.cpp` | REF-3 |
| 4 | Commands are dispatched by three `if` chains (`execute()`, `handle_room_admin()`, `execute_in_room()`). Whether a command needs a room, works in RESP2 subscribe mode, or counts toward `INFO`'s "Comenzi procesate" follows only from its position. For example, `PING`, `PUBLISH`, `SUBSCRIBE` and `UNSUBSCRIBE` are not counted, only because they return before the counter | `src/core/database.cpp` | REF-4 |
| 5 | `main.cpp` mixes configuration, the client connection (buffers, accounting, TLS), authentication and the io_uring loop. Most of the SEC-2, 17, 18, 20, 22 and 16 logic lives in it, next to ring bookkeeping | `main.cpp` | REF-5 |
| 6 | `metrics`, `pubsub`, `watchdog`, `record.h` and `pool_allocator.h` sit at the repository root, outside `src/` | repository root | REF-6 |
| 7 | `wait_until` is defined six times, in `s10`, `s11`, `s12`, `s13`, `s18` and `sec_test.py` | `tests/` | REF-7 |

## Phase 1: Foundations

### REF-1: Construct the server after startup checks

**Why:** finding 1. A failed or mistaken start must not touch the data directory.

**Do:**
1. Replace `static Database db;` and `static Watchdog watchdog(db);` with objects constructed inside `main()`. A failed configuration check or `bind()` then exits before any data file is opened.
2. The free functions that use the database (`process_buffered()`, `schedule_close()`, `deliver_pubsub()`) keep their access through one pointer or reference set in `main()`; nothing else changes.
3. Keep the explicit `0600` modes from SEC-3: they are still correct, and they no longer carry the whole guarantee.

**Behaviour change:** a start that fails its checks no longer replays the AOF or truncates `cold.bin`.

**Done when:**
- with one instance running and a cold key evicted (`SOMNIUM_MAX_KEYS=0`), a second instance in the same directory on the same port exits with code 1, and the first instance still returns the cold key;
- the whole suite is otherwise unchanged.

**Files:** `main.cpp`, a new `tests/ref_test.py` (registered in `tests/run_all.sh`).

**Not in this step:** two instances on different ports in the same directory still share files. Preventing that takes a lock file, which is a feature, so it is on the deferred list.

**Changes from the plan:**
- The database is `static optional<Database> db`, built with `db.emplace()`. The functions that use it switch from `db.` to `db->`; `Watchdog` becomes a local static next to its `start()`.
- Only `schedule_close()` and `process_buffered()` use the database outside `main()`; `deliver_pubsub()`, listed in the plan, does not.
- It is built after every check that can fail (configuration, `bind()`, and `io_uring` setup, which moved before it) and before `listen()`. So the port still refuses connections until the AOF has been replayed, as when it was not bound at all during replay, and readiness checks keep their meaning.

**Result:**
- The SEC-3 `umask` now also covers the data files; the explicit `0600` modes stay.
- Startup messages from the database (AOF recovery, migration) now come after `setvbuf`, so they reach a redirected log line by line instead of when the buffer fills.
- **Test:** `tests/ref_test.py`, registered in `tests/run_all.sh`. A server with `SOMNIUM_MAX_KEYS=0` puts a key in `cold.bin`; then a second start in the same directory, once on the same port and once with an invalid `SOMNIUM_BIND`, must exit with code 1 and leave `cold.bin` unchanged, and the first server must still return the key.
- **Verification so far:** `main.cpp` compiles cleanly with GCC 13 (stub headers). The test was run against a Python stand-in for the server: it passes when the data is opened after the checks, and fails all three data checks when it is opened before them, as the old binary does. The server itself needs Linux, so CI on this PR is the first real run.

### REF-2: One source of configuration

**Why:** finding 2. Each variable should be parsed once, by one rule, with one error path.

**Do:**
1. Add a `Config` struct with one field per variable (`REDIS_PORT` and every `SOMNIUM_*`), filled by a single `load_config()` called at the top of `main()`.
2. Pass each component the fields it needs: `Database`, `AOFManager`, `EvictionManager`, `SnapshotManager`, `Replicator` and the metrics exporter. No component calls `getenv` any more.
3. `SOMNIUM_PEERS` is parsed once into a list of `host:port` entries. `Database` uses the size of that list, so it can no longer disagree with the replicator.
4. Keep each variable's current reaction to an invalid value (exit, or warn and fall back). Unifying them is decision D2 below.

**Behaviour change:** a malformed `SOMNIUM_PEERS` entry no longer stops tombstone collection.

**Done when:**
- `grep -rn getenv` finds only `load_config()`;
- a node configured with one valid peer and one malformed entry collects tombstones like a node with only the valid peer (extend the `s13` scenario);
- the whole suite is otherwise unchanged.

**Files:** `main.cpp`, a new `src/config.{h,cpp}`, the constructors of the components listed above, `tests/s13_test.py`.

**Depends on:** REF-1, which puts construction in `main()`, where the configuration is loaded.

**Changes from the plan:**
- Components take `const Config&` and copy the fields they need, so no component keeps a reference to it. The defaults now live only in `Config`; the class members that held them no longer have initializers.
- `main.cpp` keeps one `static Config cfg` in place of the globals `g_password`, `g_maxclients`, `g_max_client_buffers` and `g_trace`.
- The AOF sync policy enum moved to `config.h` (`AofSync`); `AOFManager::SyncPolicy` is now an alias, so the AOF code is unchanged.
- `get_current_time_ms()` became a private `Database` member that adds the configured clock offset; its 17 call sites are unchanged.
- The snapshot test delay is now a parameter of `SnapshotManager::hibernate_room()`, instead of a `static` read from the environment on first use.
- The TLS paths are `std::optional<std::string>`, because a variable set to an empty string must still count as set: `SOMNIUM_TLS_CERT=""` refuses to start rather than silently serving plain text.
- The SEC-23 fuzzer now builds a `Config` instead of calling `setenv`.

**Result:**
- `grep getenv` finds only `src/config.cpp`. 25 calls in 7 files became one function. The components lost 108 lines of parsing; the new module adds 165, most of them the documented `Config` fields.
- **Unchanged:** every variable's rule and message, including which invalid values stop the server (port, client limits, bind address, a network bind without a password) and which are reported and fall back to their default.
- **Behaviour change:** `Database` counts the peers that the replicator actually uses, so a malformed `SOMNIUM_PEERS` entry no longer stops tombstone collection. Test: a third `s13` section, `sec_malformed_peer`.
- **Verification so far:** every changed file compiles cleanly with GCC 13 (stub headers). `load_config()` was compiled on Windows against the real Winsock `inet_pton` and passed 28 checks: defaults, each fatal case, each fallback, the clamps, the peer list and the TLS and metrics switches, with messages identical to the old ones. `Database` with all its storage components was linked and run through the fuzz harness on sample inputs. The server and the suites need Linux, so CI on this PR is the first real run.

## Phase 2: Structure

### REF-3: Split `database.cpp` by concern

**Why:** finding 3. Related code should be read together, and each file should state the locking rules it relies on.

**Do:** move functions verbatim into one file per concern. `database.h` stays a single class.

| File | Contents |
| --- | --- |
| `database.cpp` | constructor and destructor, `execute()`, client-room bookkeeping, `lookup()`, `insert()`, `set_value()`, `erase()`, `cleanup_client()` |
| `rooms.cpp` | `find_room()`, `activate()`, `hibernate()`, `wake_room()`, `handle_room_admin()`, `hibernate_inactive_rooms()` |
| `commands.cpp` | `execute_in_room()` and the key commands: `GET`, `SET`, `MGET`, `MSET`, `EXISTS`, `DEL`, the expiry commands, `INFO`, `clean_expired_keys()` |
| `crdt.cpp` | `incoming_version()`, `stale()`, the tombstone functions, `collect_tombstones()`, `CRDTMERGE`, `CRDTDEL` |
| `persistence.cpp` | `replay_record()`, `dump_state()`, `rewrite_aof()`, `discard_active_snapshots()`, the background rewrite |

Add the new files to `CMakeLists.txt` and to the source list of the `fuzz` job in `.github/workflows/ci.yml`, which names `database.cpp` explicitly.

**Done when:** `git diff --color-moved` shows only moved blocks and the new file headers, no file is above roughly 350 lines, and CI is green with no test changes.

**Files:** `src/core/*.cpp`, `CMakeLists.txt`, `.github/workflows/ci.yml`.

**Result:**
- **The move:** a script cut each `Database::` function, with the comment above it, into its file in the original order, and asserted that every original line landed in exactly one file. Afterwards the five files hold the same 864 non-blank lines as the old `database.cpp`, and the same 44 functions. `bulk_string()` stays in `database.cpp`, next to `execute()`, because every file uses it.
- **The only new lines:** a header per file stating what it holds and the lock rules it relies on (taken from `room.h` and `database.h`), and the includes each file needs. `database.cpp` drops `<cstdlib>`, `<sys/wait.h>` and `<unistd.h>`, which only the persistence code used.
- **Sizes:** `commands.cpp` 305 lines, `database.cpp` 220, `persistence.cpp` 200, `rooms.cpp` 157, `crdt.cpp` 139 (1,007 in one file before).
- **Commits:** one. The added lines are only headers and includes, so `git diff --color-moved` still separates them from the moved code, which is what the ground rule is for.
- **Verification so far:** each file compiles on its own with GCC 13 (stub headers), and all five link together with the storage components and run the fuzz harness's sample inputs. The server and the suites need Linux, so CI on this PR is the first real run.

### REF-4: A command table

**Why:** finding 4. A command's properties should be written next to its name, not implied by where its `if` sits.

**Do:**
1. One table, one row per command: name, scope (connection, administration, room), whether it is allowed in RESP2 subscribe mode, whether it counts toward "Comenzi procesate", and its handler. `execute()` looks the name up once and applies the row.
2. Preserve today's behaviour exactly, including the counter (`PING`, `PUBLISH`, `SUBSCRIBE` and `UNSUBSCRIBE` not counted) and every error message. Changing either is decision D4.
3. Argument-count checks stay in the handlers for now: moving them into the table would change error texts (decision D4).
4. `replay_record()` keeps its own dispatch. It applies stored state rather than running commands, and folding the two together would couple recovery to the live command surface.

**Done when:** adding a command means adding one row and one handler; CI is green with no test changes, including both fuzzers (the SEC-23 harness exercises the table directly).

**Files:** `src/core/database.{h,cpp}`, `src/core/rooms.cpp`, `src/core/commands.cpp`.

**Depends on:** REF-3, so the change lands in the already-split files.

**Changes from the plan:**
- "Allowed in RESP2 subscribe mode" and "not counted" hold for exactly the same four commands (`PING`, `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`), so they are one column, `pubsub`, documented as both. Separating them belongs to decision D4, if the counter ever changes.
- `ROOM.<anything>` stays a family: the dispatcher looks every `ROOM.` name up as the single row `"ROOM."`, and `handle_room_admin()` picks the subcommand, as before. So an unknown `ROOM.X` still answers with its argument, room or unknown-command error.
- An unknown command is a fallback row with room scope. It still wakes the client's room (and can get `ROOMS FULL`) before answering `-ERR unknown command`, exactly as before.
- Rows are one-line lambdas calling the existing handlers. Four inline bodies longer than three lines became handlers in the file of their concern: `handle_subscribe()` and `handle_ping()` (`database.cpp`), `handle_room()` (`rooms.cpp`) and `handle_replfrontier()` (`crdt.cpp`, next to the tombstone collection it drives).
- `execute_in_room()` now receives the row and runs its handler. Its empty-arguments check went: `execute()` already returns on empty input, and it is the only caller.

**Result:**
- One table in `database.cpp`, 27 rows in three groups (4 Pub/Sub, 9 connection and server, 14 key commands). `execute()` uppercases the name once, finds the row, applies the subscribe-mode check and the counter unless the row is `pubsub`, and runs the handler, inside the client's room if the row says so.
- **Verification so far:** every file compiles cleanly with GCC 13 (stub headers). A differential check built `Database` from `main` and from this branch with the same driver and ran a 75-command script through both: every row, both fallbacks, the `ROOM.*` error variants, subscribe mode on RESP2 and RESP3, lowercase names, wrong argument counts, and `INFO`'s command counter. The replies were byte-for-byte identical. The server and the suites need Linux, so CI on this PR is the first real run.

### REF-5: Split `main.cpp`: connection layer and authentication

**Why:** finding 5. The code that protects the server from clients should be one reviewable unit, separate from ring bookkeeping.

**Do:**
1. `src/net/connection.{h,cpp}`: `Client`, `drain_read()` and `process_buffered()` (with the `AUTH` gate), buffer accounting (`track()`, the budget, the caps), the TLS helpers, `enqueue_output()` and `flush_output()`.
2. `src/net/auth.{h,cpp}`: `credentials_ok()`, `AUTH`, `HELLO` and the failed-attempt counter.
3. `main.cpp` keeps startup and the io_uring loop: accept, the client registry, arming, CQE handling, Pub/Sub delivery and the periodic sweep.
4. **The seam:** code in `src/net/` never touches the ring or the client registry. Where it acts on them today (closing a client, arming a write, scheduling more work), it returns what the loop must do instead, and the loop does it. These return values are the only edits that are not pure moves; they get their own commit.

**Done when:** `main.cpp` is under roughly 300 lines; the connection-level invariants (the `AUTH` gate, buffer accounting and caps, TLS) live in `src/net/`; and CI is green with no test changes.

**Files:** `main.cpp`, `src/net/*`, `CMakeLists.txt`.

**Depends on:** REF-2, which removes configuration parsing from `main.cpp` first.

**Changes from the plan:**
- **The seam, concretely:** `enqueue_output()` returns `false` instead of closing the client, and `process_buffered()` returns a `Step` (`Drop`, `Done`, `More`). A small `serve()` in `main.cpp` acts on it: it closes on `Drop`, arms a write while `pending_out() > 0`, re-arms the read, and reports `More`. Output pending after a flush is exactly the old `FlushResult::Pending`, so the polls are armed in the same order and under the same conditions.
- **Configuration:** the connection layer gets its settings once, through `net::init(cfg, tls)`, instead of reading `main.cpp`'s globals. `Client`'s constructor uses them for `authenticated` and the TLS session. The buffer total (SEC-22) moved with it.
- **Auth:** `auth()` and `hello()` take the password as a parameter, so `auth.cpp` does not depend on the connection's settings. The SEC-20 counter lives there; the sweep reads and resets it with `net::take_auth_failures()`.
- The buffer limits became `constexpr` constants inside `connection.cpp`; the output cap uses `Database::MAX_REPLY` directly.
- `SOMNIUM_TRACE_FD` (decision D3) is untouched: the one trace line in `flush_output()` moved with it.

**Result:**
- `src/net/connection.{h,cpp}` (257 lines) and `src/net/auth.{h,cpp}` (106 lines). The `AUTH` gate, buffer accounting and caps, and TLS now live there, as one reviewable unit.
- `main.cpp` went from 652 to 394 lines, not the planned "roughly 300". What remains is startup (configuration, TLS context, socket, ring, components) and the io_uring loop itself. Shrinking it further would mean splitting the loop, which is a separate decision, not part of this step.
- **Verification so far:** every file compiles cleanly with GCC 13 (stub headers). A differential check `#include`d the old and the new `main.cpp` into the same driver, with a `send()` stub that records the bytes and can simulate a full socket. It ran 20 scenarios through both: pipelined and partial commands, `QUIT`, malformed input, the 8,192-command budget, a full socket, RESP3, and, with a password, `NOAUTH`, wrong and right passwords, `HELLO 3 AUTH`, an unauthenticated client hitting the 16 KB cap, and one facing a full socket. Bytes sent, closing, armed polls, the "more work" signal, leftover input, pending output and the failed-attempt count were identical. The server and the suites need Linux, so CI on this PR is the first real run.

## Phase 3: Layout and tests

### REF-6: Source layout

**Why:** finding 6. Every source file should live under `src/`, in a directory that names its role.

**Do:**
1. Move `metrics.*` and `watchdog.*` to `src/server/`, `pubsub.*` to `src/core/`, and `record.h` and `pool_allocator.h` to `src/core/`.
2. Update includes, `CMakeLists.txt`, the `fuzz` job's source list, and the "Project Structure" section of `README.md`.

**Done when:** the repository root holds only build, documentation and configuration files, and CI is green with no test changes.

**Files:** the moved files and everything that includes them.

**Timing:** last, because it touches includes in most files; merge it when few other pull requests are open.

### REF-7: Shared test helpers

**Why:** finding 7. Six copies of the same helper drift apart.

**Do:** move `wait_until` to `tests/common.py` with the most general signature in use (a condition and a timeout), and import it everywhere. `server_log` and `run_once` stay in `sec_test.py`, their only user.

**Done when:** `grep -rn "def wait_until" tests` finds one definition, and CI is green.

**Files:** `tests/common.py`, the six test files.

## Housekeeping

`SPRINT.md` still marks S14 as "implemented, awaiting CI verification", though it merged with green CI in PR #9. Correct it in whichever pull request touches documentation first. (`build/` was untracked in PR #33.)

## Decisions for the team

These are not refactors: each changes behaviour or conventions, so each needs an explicit decision before it becomes a step.

| # | Question | Recommendation |
| --- | --- | --- |
| D1 | Comment language: code comments are in Romanian, documentation in English | Pick one for new code and for code a step touches anyway. Do not spend a pull request on translation alone |
| D2 | Invalid environment values: some exit with code 1, others warn and fall back | After REF-2, make every invalid value fatal, consistent with SEC-1. A wrong setting that is silently replaced is harder to notice than a refused start |
| D3 | `SOMNIUM_TRACE_FD`: 12 call sites of undocumented debug tracing in `main.cpp`, much of it around the io_uring start-up flake | Keep it and document it if the flake still occurs. Otherwise delete it during REF-5 |
| D4 | Error texts: database commands answer `-ERR Wrong number of arguments for X`, while `AUTH` uses Redis's `-ERR wrong number of arguments for 'auth' command` | After REF-4, use Redis's text everywhere and move the argument counts into the table. Clients and tools that match on Redis's messages then behave the same |

## Out of scope

- **Persistence formats** (AOF, snapshots, cold storage): unchanged. Data compatibility is not touched by a refactor.
- **The I/O model:** io_uring stays.
- **Abstractions for their own sake:** no interfaces, plugin points or dependency-injection frameworks. The seams above exist because a concrete step needs them.
- **History rewriting:** `main`'s history stays as it is.

## Deferred

| Item | Trigger |
| --- | --- |
| A lock file in the data directory, so two instances cannot share it | A deployment that runs several instances on one host |
| Fuzzing the connection layer (`src/net/`) directly | After REF-5, if a bug is found there that the suite missed |
| Fuzzing snapshot loading and AOF recovery (also deferred in `securityprogress.md`) | A change to either file format |

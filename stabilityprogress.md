# Stability Progress

A step-by-step plan for the stability and continuity problems found by stress-testing SomniumDB after the refactor plan (`refactorprogress.md`) was completed. The six problems that could be fixed in place are already fixed in #43 (see [Done](#done)). What remains here changes behaviour or the persistence model, so each item is one pull request with its own test, and one item needs a decision first.

## Objectives

1. **A restart must succeed wherever the server ran.** A server that stayed within its memory budget for weeks must not need more memory, or crash, when it starts again.
2. **Background maintenance must not stall clients.** Work that runs on its own (expiry, compaction) must take bounded time per step, however large the dataset grows.
3. **Disk usage must be bounded by live data.** Files must not grow without limit under a steady workload.
4. **A write the server acknowledges must survive a restart.** When it cannot, the client must be told.

## Ground rules

- **One step, one pull request.** Branch from the latest `origin/main`; when a step is merged, set its status to `DONE`, add the PR number, and note anything that changed from the plan.
- **Every step ships with a test that fails on `main`.** New tests go in `tests/stab_test.py` (registered in `tests/run_all.sh`) unless a suite already owns the area.
- **Performance steps report before and after**, measured out of process on a Release build, with the commands in the pull request. A latency claim needs a probe in a separate process: Python client threads in one process share the GIL and hide the server's own stalls.
- **Persistence steps prove continuity.** Any step that touches the AOF, snapshots or cold storage must show, across a restart, that every acknowledged write is present and that every deleted key stays deleted (including keys that were in cold storage).
- **CI is the gate.** All four jobs green: the ASan + UBSan suite, TSan, both fuzzers and the benchmark.

## Status

| Step | Title | Status |
| --- | --- | --- |
| STAB-1 | Honour the memory budget during AOF replay | TODO |
| STAB-2 | Compact cold storage automatically | TODO |
| STAB-3 | Incremental expiry | TODO |
| D1 | Writes acknowledged while the AOF cannot be written | Decision needed |

## Current state

Findings from stress-testing `main` at `b8d6f7b` (2026-10-16) on the Release and ASan builds. Each was reproduced and measured.

| # | Finding | Evidence | Where | Step |
| --- | --- | --- | --- | --- |
| 1 | AOF replay inserts every key into RAM and never evicts, so a restart ignores `SOMNIUM_MAXMEMORY` and `SOMNIUM_MAX_KEYS`. Eviction only runs after a live command touches the room. A dataset kept mostly in cold storage must fit in RAM at every restart, or the process is killed for lack of memory and the restart loops | With `SOMNIUM_MAX_KEYS=1000 SOMNIUM_MAXMEMORY=2000000` and 30,000 keys written: 1,000 keys and 320 KB resident while running, 29,986 keys and 9.6 MB resident after a restart (30x the budget) until the first command | `Database::replay_record`, `src/core/persistence.cpp` | STAB-1 |
| 2 | `cold.bin` only shrinks on a manual `COMPACT`. Every command that reads a cold key reloads it into RAM, and the eviction that follows appends a new record and leaves the old one obsolete, so under a read/evict workload the file grows until the disk is full | On a 4 MB tmpfs, eviction churn alone refilled 2.4 MB of freed space within 20 commands | `EvictionManager`, `src/storage/eviction_manager.cpp` | STAB-2 |
| 3 | Every 5 seconds the watchdog scans every key of every room while holding that room's lock. The command thread blocks on the same lock, so every client waits for the scan. The pause grows linearly with the keys in a room | Release build, 995,115 keys in `default`, `GET` probe every 2 ms from another process: p50 0.14 ms, max 237 ms, a stall of more than 20 ms every 5 seconds | `Database::clean_expired_keys`, `src/core/commands.cpp` | STAB-3 |
| 4 | When an AOF write fails (disk full, I/O error) the handlers ignore `AOFManager::append()`'s result: the mutation stays in RAM and the client gets `+OK`. The write is not in the AOF, so it is not replicated (the replicator reads the AOF) and is lost on restart. The comment on `append()` says the server never acknowledges falsely | On a tmpfs with room for 7 of them, 12 of 12 large `SET`s returned `+OK`; the 5 that did not fit were gone after a restart | every write handler in `src/core/commands.cpp` and `src/core/crdt.cpp` | D1 |

## STAB-1: Honour the memory budget during AOF replay

**Why:** finding 1. A restart is when the server can least afford to fail.

**Do:**
1. Make every replay path aware of cold storage first. Today replay only looks at `room->keys`, which is correct only because nothing is ever evicted during replay. `put` must drop a cold copy before inserting; the `DEL`, `CRDTDEL`, `EXPIRE`, `PEXPIRE` and `PERSIST` branches must find and change a key that is in cold storage. This keeps the invariant in `src/storage/eviction_manager.h`: a key lives in RAM or in cold storage, never both. Without this, a key evicted during replay comes back when a later `DEL` record misses it.
2. After each applied record, evict the room down to its limits with the same `evict_despised_keys` that live commands use, so peak resident memory stays within the budget during replay as well as after it.

**Behaviour change:** after a restart, keys over the budget start in cold storage instead of RAM. The README sentence "on restart the AOF replays the full state into RAM" changes accordingly.

**Done when:**
- a test writes ten times the `SOMNIUM_MAXMEMORY` budget, restarts, and reads `INFO` before any other command: keys and resident bytes are within the budget;
- every key reads back its exact value, with its TTL;
- keys deleted before the restart stay deleted, including keys deleted while they were in cold storage and keys whose `SET` and `DEL` records both fall after an eviction during replay;
- startup time for a large AOF is measured before and after, and stated in the pull request.

**Files:** `src/core/persistence.cpp`, possibly `src/core/database.cpp`, `tests/stab_test.py`, `README.md`.

**Not in this step:**
- making `cold.bin` survive a restart, so that replay would not need to rebuild it, is a persistence format change;
- replay writes evicted records to `cold.bin`, and a key written many times in the AOF becomes many obsolete records there. STAB-2 reclaims them; if startup alone fills the disk on a real dataset, do STAB-2 first.

## STAB-2: Compact cold storage automatically

**Why:** finding 2. Without it, the disk-full handling fixed in this plan's first pull request is only a matter of time.

**Do:**
1. Run `compact()` automatically when obsolete bytes exceed half of `cold.bin` and a fixed minimum (the 64 MB default of `SOMNIUM_AOF_REWRITE_MIN_BYTES` is a reasonable start). Check after eviction, which is where the file grows. Prefer a constant to a new setting until a deployment needs one.
2. After a failed compaction, wait before retrying (for example until the file has grown by the minimum again), as `AOFManager::rewrite_due` resets its base after every attempt. A full disk must not cause an attempt on every command.

**Behaviour change:** `cold.bin` shrinks without `COMPACT`. Compaction runs on the command thread, so clients pause while it runs; with a 50% trigger its cost is amortised over the writes that made the garbage, but a single compaction of a large file is still one long pause. Measure it, and state it in the pull request.

**Done when:**
- a churn test with `SOMNIUM_MAX_KEYS=0` (every command evicts everything) runs thousands of reload and evict cycles, and `cold.bin` stays within twice the live cold data plus the minimum;
- every key still reads back its own value;
- with compaction made to fail (`cold.bin.tmp` as a directory, as in `tests/s1_test.py`), the server does not retry on every command.

**Files:** `src/storage/eviction_manager.h`, `src/storage/eviction_manager.cpp`, `tests/stab_test.py`.

**Not in this step:** incremental or background compaction. Do it only if the measured pause is too long for a real dataset.

## STAB-3: Incremental expiry

**Why:** finding 3.

**Do:**
1. Bound the work done under one room lock. Two ways that fit the code:
   - **Cursor (recommended):** each watchdog tick resumes a per-room cursor over the hash table's buckets and examines at most a fixed number of entries (for example 1,000) per lock hold, releasing the lock between batches. A rehash may make a pass skip or revisit a few keys; that is harmless, because lazy expiry in `Database::lookup` still hides expired keys and the next pass catches them.
   - **Sampling, as Redis does:** sample keys with a deadline and repeat while many of them have expired. It needs an index of keys with a TTL, since random keys in a mostly persistent room are rarely expired, and that index costs memory per key.
2. Keep deleting through `Database::erase`, so key counts and resident bytes stay exact, and keep the lock order documented in `src/core/room.h` (`rooms_mutex` before `room_mutex`).
3. Skip sleeping rooms without taking their lock; they hold no keys in RAM.

**Behaviour change:** none that a client can observe, except latency. Expired keys stay in RAM a little longer before their memory is reclaimed; reads already treat them as missing.

**Done when:**
- the measurement in finding 3, repeated for 30 seconds, shows no stall above 10 ms where `main` shows about 240 ms;
- a test sets many keys with a short `PX` and reads none of them, and `INFO` reaches 0 keys within a bounded time;
- the TSan job (`tests/s4_test.py`, commands and watchdog under load) passes.

**Files:** `src/core/commands.cpp` (`clean_expired_keys`), `src/core/room.h`, possibly `src/server/watchdog.cpp`, `tests/stab_test.py`.

**Not in this step:** expiring keys in cold storage. Their records are dropped by compaction (`compact()` already skips expired records), so STAB-2 covers them.

## Decisions for the team

| # | Question | Recommendation |
| --- | --- | --- |
| D1 | When the AOF cannot be written, should a write still be applied and acknowledged (finding 4)? | **Write ahead: append to the AOF before changing RAM, and on failure reply `-MISCONF` and change nothing.** The handlers already compute the full record (value, absolute deadline, version) before the mutation, so the order can be swapped one handler at a time. The alternatives are worse. An error reply alone, with the mutation kept, leaves RAM, disk and the peers disagreeing: readers see a value that will vanish on restart. Refusing every write while the AOF is unhealthy, as Redis does, needs a way to learn that space is back, and without a write to try there is no reliable signal. Write-ahead needs no gate: each write tries the disk, and disk-full handling (torn records are now cut off) makes a failed try leave nothing behind. One case needs a choice of its own: with `SOMNIUM_AOF_SYNC=always`, a write that reached the file but whose `fdatasync` failed may or may not survive. Redis exits in that case |

Once D1 is decided, it becomes STAB-4, with a test like the disk-full tests in `tests/stab_test.py`: no `+OK` while the file-size limit is in place, and nothing changed in RAM by a refused write.

**Since the batched AOF (`tests/pipe_test.py`):** `AOFManager::append()` only buffers the record. The `write()` happens once per event-loop iteration in `flush()`, before any reply of that iteration leaves the server, so a failed write is known while the replies can still be changed. Behaviour on failure is unchanged: the writes stay in RAM and are acknowledged. Write-ahead as recommended above would have to call `flush()` before every mutation, which brings back one `write()` per write command (measured: about half the deep-pipeline throughput). The choice now includes doing write-ahead only while the AOF is unhealthy, and keeping the batched path while it is healthy.

## Done

Fixed in #43, the pull request that added this plan. Each fix has a test that fails on `main` and passes after it.

| Problem | Impact on `main` | Fix | Test |
| --- | --- | --- | --- |
| A snapshot length near `SIZE_MAX` (for example `0xFF` bytes) wrapped the loader's bounds check, and allocating the string threw `std::length_error` | The process aborted on every start that replayed a record for that room | Compare against the bytes left; check the v2 header length | `tests/s5_test.py`, section 8 |
| A failed AOF `write()` (disk full, `EFBIG`) left a partial record, and later appends landed after it | On restart, recovery took everything after the partial record for an incomplete tail and truncated it: 0 of 10 writes made after the disk recovered survived, while `SAVE` reported the AOF healthy | Truncate exactly the bytes the failed write left; stop appending until a rewrite if that fails too | `tests/stab_test.py` |
| A failed cold-storage write left a partial record, and the index offsets no longer matched the file | `GET` returned another key's value (11 of 14 keys in the test). All rooms share the file, so the value can belong to another room | Write records with `pwrite` at the logical end, so the next record overwrites a partial one | `tests/stab_test.py` |
| `SIGXFSZ` was not ignored | With a file-size limit (`ulimit -f`, `LimitFSIZE=`), the kernel killed the server when the AOF reached it, on every restart | Ignore it; the write fails with `EFBIG` and takes the normal error path | `tests/stab_test.py` |
| `SOMNIUM_MAXCLIENTS` (10,000) could exceed the fd limit, and a failed `accept()` was re-armed at once | A core at 100% and about a million log lines per second (11 GB in minutes) | Raise the soft fd limit at startup or lower `maxclients` with a warning; pause accepting until the next sweep and log once per second | `tests/stab_test.py` |
| The count-min sketch (one for the whole server) never decays, so after about 40,000 accesses no key looked cold and every eviction scanned the whole room | 300,000 keys: 41 `SET`s per second past the key limit, 486 ms each, with every client stalled | Stop the search for a cold key after 64 extra samples | `tests/stab_test.py` (correctness); 41 to 18,182 `SET`s per second measured |
| The replication offset is keyed by inode, which ext4 reuses after two rewrites | A saved offset inside a record stopped replication to that peer for good, with healthy heartbeats | Resend from offset 0 when the first record at the offset does not parse | `tests/stab_test.py` |
| `DEL a b` deleted only `a` | Clients silently kept every key after the first | Delete every key, one AOF record per key | `tests/s7_test.py`, `tests/s13_test.py` |

## Deferred

| Item | Trigger |
| --- | --- |
| Identify the AOF by more than its inode (for example inode and birth time from `statx`), so a reused inode can never match a saved offset that happens to fall on a record boundary of the new file. The fix in [Done](#done) catches the common case, an offset inside a record | Any report of a key present on one node and missing on its peer |
| Keep parser state between reads. The parser starts again from the beginning of the buffer on every read, so a command with the maximum of 1,048,576 arguments sent slowly costs repeated parsing: up to 50 ms pauses in the stress test, bounded by the argument limit | A latency report traced to large pipelined commands, or any change to `src/core/resp.h` |
| Age the count-min sketch (halve its counters periodically, as TinyLFU does). Without it, eviction is closer to random than LFU once the sketch saturates. It is a quality improvement, not a stability fix, since the cost of eviction is now bounded | A workload where hot keys are seen being evicted |

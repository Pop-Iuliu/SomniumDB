# Sprint: Odd Words, Solid Foundations

**Goal:** Make SomniumDB more dependable under storage churn, room switching, and slow clients, while adding useful command features.

**Status:** DONE. All five tickets implemented and verified. These are five implementation tickets, each inspired by one supplied word.

**Planning target:** One two-week sprint. Size the tickets against available capacity at kickoff; completion means meeting the acceptance checks below.

| ID | Inspiration | Deliverable | Priority | Depends on |
| --- | --- | --- | --- | --- |
| S1 | excrescencies | Cold-storage compaction and stale-record cleanup | P1 | S5 |
| S2 | sundaes | Composable SET options and expiry inspection | P1 | S1, S5 |
| S3 | naigie | Fair, nonblocking client output | P1 | None |
| S4 | rezoned | Safe room lifecycle and explicit room controls | P0 | S5 storage-result contract |
| S5 | calcaneoscaphoid | Reliable connections between writes, snapshots, and recovery | P0 | None |

P0 means correctness foundations to land first. P1 means the next deliverables built on those foundations.

## S1 - Excrescencies: Prune the Growth

**Status: DONE.** Verified by CI on PR #1 (AddressSanitizer suite and the ThreadSanitizer S4 stress job, both green). Cold storage moved to `cold.bin` with an in-RAM `(room, key)` index that points at the latest record; reads are a single `pread`, misses never touch the disk, and the index replaces the Bloom filter (deleted). Records carry value, absolute expiry and CRDT version. A key lives in RAM or in the index, never both: every access goes through one `lookup()` that moves a cold key back into RAM, so DEL and expiry cannot resurrect a disk value. Because the AOF replays the full state into RAM at startup, the cold file lasts for the process lifetime (truncated on start), which removes the need for on-disk deletion markers, index rebuilds and legacy migration. `COMPACT` rewrites live, unexpired records and publishes file and index together only after every write succeeded. Metrics: `db_cold_file_bytes`, `db_cold_obsolete_bytes`, `db_cold_reclaimed_bytes`. `SOMNIUM_MAX_KEYS` sets the per-room limit (0 makes eviction deterministic for tests). Tests: `tests/s1_test.py`.

**Inspiration:** Unwanted outgrowths become obsolete records accumulating in cold storage.

**Why now:** `despised_keys.bin` retains every eviction, reads scan the whole file, and `DEL` only removes RAM entries. An old disk value can reappear after deletion.

**Deliver:**
- Maintain an index keyed by `(room, key)` pointing to the latest cold record, including deletion markers.
- Carry value, absolute expiry, and CRDT version through eviction and reload. Represent a missing record separately from an empty value.
- Add a maintenance compaction operation that retains the latest live records and any deletion markers still needed to prevent resurrection. Publish the replacement file and index together only after successful validation.
- Expose cold-file bytes, obsolete bytes, and reclaimed bytes through the existing metrics exporter.

**Acceptance checks:**
- Evict, reload, overwrite, delete, restart, and compact a key. Its last acknowledged logical state remains correct at each step.
- Two rooms containing the same key remain independent; empty keys and empty values round-trip.
- A fixture with 1,000 obsolete versions compacts to its live state, with matching values and metadata before and after.
- A failed compaction preserves the previous usable file and index. A failed eviction write retains the RAM record.

**Primary files:** `src/storage/eviction_manager.*`, `src/core/database.cpp`, `record.h`, `metrics.*`.

## S2 - Sundaes: Build Your Own SET

**Status: DONE.** Verified by CI on PR #1 (AddressSanitizer suite and the ThreadSanitizer S4 stress job, both green). `SET key value [NX|XX] [EX|PX]` validates every option before any effect (conflicts, duplicates, missing values, nonpositive or overflowing durations), evaluates NX/XX through the same `lookup()` as reads (cold storage included, expired keys absent), and writes nothing to the AOF when a condition fails. `TTL`/`PTTL` follow Redis (`-1`/`-2`). Deadlines are absolute and already travel through AOF meta, snapshots and cold records. Replay now applies CRDTMERGE verbatim like SET (live, an expired key counts as absent, so re-comparing at replay could diverge). `SOMNIUM_CLOCK_OFFSET_MS` is the controllable clock used to simulate time passing while the server is down. Tests: `tests/s2_test.py`.

**Inspiration:** A base scoop with optional toppings becomes a write with composable conditions and expiration.

**Why now:** `handle_set()` accepts additional arguments but ignores them, always setting `expire_at` to zero.

**Deliver:**
- Support `SET key value [NX|XX] [EX seconds|PX milliseconds]` with strict, complete argument validation.
- Add `TTL key` and `PTTL key`, including Redis-style `-1` for a persistent key and `-2` for a missing key.
- Evaluate conditions against the logical store, including cold records and expired keys, while holding the room's mutation lock.
- Persist absolute deadlines and the resulting record version. Replay restores the original mutation instead of reevaluating its condition or restarting its TTL.

**Example:** `SET dessert sundae NX EX 60` creates the key only if absent and keeps it for 60 seconds.

**Acceptance checks:**
- Conflicting flags, missing option values, nonpositive durations, and numeric overflow return errors without changing data.
- A failed NX/XX condition returns a nil reply and adds no mutation to the AOF.
- Expired keys count as absent. A plain SET replaces the value and removes an existing TTL.
- Expiry survives eviction, hibernation, and restart without gaining extra lifetime. Use a controllable clock for boundary tests.

**Primary files:** `src/core/database.*`, `record.h`, `src/storage/aof_manager.*`, `src/storage/eviction_manager.*`.

## S3 - Naigie: The Little Workhorse

**Status: DONE.** Implemented and verified. The event loop now works with per-client output queues flushed nonblocking (POLLOUT via io_uring), shared_ptr-poll tickets for disconnect-safe lifetimes, a 32MB output cap that disconnects slow clients, a 8192-command budget per turn with eventfd-driven resumption, and accept-all-per-event. Pub/Sub delivers through the same queues. Verified with a slow-client fairness test, byte-exact large replies, a dead-slow subscriber (publisher stays sub-2ms), the eventfd pump, and an AddressSanitizer run with zero findings. Pipelined throughput went from 19.4k to 568k ops/s (29x).

**Inspiration:** A steady workhorse that keeps every client moving, even when one falls behind.

**Why now:** `send_all()` waits indefinitely in `poll(POLLOUT)`, blocking the command loop. Pub/Sub uses a separate unchecked `send()` path.

**Deliver:**
- Give each client an ordered output queue and write offset. Resume partial writes through io_uring readiness events rather than waiting inside a command.
- Route command replies and Pub/Sub messages through the same output mechanism.
- Bound queued output per client and disconnect clients that exceed the configured limit.
- Apply per-turn command and byte budgets, rescheduling buffered work so a large pipeline cannot monopolize the loop. Handle disconnects and outstanding completion lifetimes explicitly.

**Acceptance checks:**
- Stop one client from reading a large response. Another client still completes GET/SET requests before the slow client resumes.
- With tiny socket buffers, large replies arrive byte-for-byte and in order.
- A slow subscriber cannot stall the publisher or other subscribers; an over-limit disconnect removes its subscriptions.
- Disconnecting during pending I/O produces no stale-pointer access under AddressSanitizer. Buffered commands continue processing even without another socket-read event.

**Primary files:** `main.cpp`, `pubsub.*`, with an extracted client-I/O component if needed.

## S4 - Rezoned: Rooms With Rules

**Status: DONE.** Verified by CI on PR #1 (AddressSanitizer suite and the ThreadSanitizer S4 stress job, both green). Rooms have an explicit atomic state (`sleeping`, `loading`, `active`, `hibernating`) with the contract documented in `src/core/room.h`: registry lock before room lock, never the reverse. The active budget is an atomic slot counter reserved with a CAS on both creation and wake, so it holds without scanning the registry under a lock; a full budget returns `-ERR ROOMS FULL` (no implicit LRU eviction: that would put snapshot I/O on the command thread). INFO reads counters only, removing the room-lock then registry-lock inversion. Rooms are never erased from the registry, so a write racing with hibernation of an empty room stays reachable. New commands: `ROOMS`, `ROOM.INFO`, `ROOM.HIBERNATE`, `ROOM.WAKE`; `ROOM name` still selects. Recovery bypasses the budget. Tests: `tests/s4_test.py`, plus a ThreadSanitizer CI job running the S4 stress section.

**Inspiration:** Changing how space is allocated becomes explicit control of which rooms occupy RAM.

**Why now:** Room creation checks the active-room limit, but waking an existing room bypasses it. Lifecycle fields have inconsistent lock protection, and some paths acquire room/map locks in opposite orders.

**Deliver:**
- Model loading, active, hibernating, and sleeping states explicitly, with a documented locking and ownership contract.
- Enforce the active-room budget on both creation and wake. If full, hibernate an eligible least-recently-used room or return a clear capacity error.
- Add `ROOMS`, `ROOM.INFO name`, `ROOM.HIBERNATE name`, and `ROOM.WAKE name`. Keep `ROOM name` as selection, so names such as `LIST` remain valid. Keep the default room pinned.
- Expose state and last access in room inspection, plus resident-key count for active rooms. Recover all persisted rooms even when their total exceeds the active-room budget.

**Acceptance checks:**
- Cycle through six rooms with a three-active-room budget. Data remains isolated and no activation exceeds the budget.
- Concurrent commands, INFO, expiry cleanup, and hibernation finish without deadlock or races in a bounded stress test, including ThreadSanitizer coverage.
- A write racing with empty-room hibernation remains reachable through the room registry.
- A failed snapshot leaves the room active and its records readable; recovery does not silently skip a room because RAM slots are full.

**Primary files:** `src/core/database.*`, `src/core/room.h`, `watchdog.cpp`, `src/storage/snapshot_manager.*`.

## S5 - Calcaneoscaphoid: Strengthen the Joint

**Status: DONE.** Implemented and verified. The AOF is now format v3: a RESP header (`SOMNIUM-AOF` / `3`) followed by records that carry the room, the command and the resulting mutation metadata (`SOMNIUM-META` + expire + CRDT timestamp + node). Recovery is strict: the last fully valid byte offset is tracked; an incomplete final record (crash mid-write) is truncated exactly there, while mid-file corruption preserves the whole file renamed `.corrupt.<ts>` for diagnosis and reserializes the replayed state. Legacy v1 (command-only) and v2 (room-prefixed) files are detected once per file at the first record and migrated to v3 by serializing the replayed RAM state through a temp file + fsync + atomic rename; a room literally named `SET` round-trips because format detection no longer guesses per command. Snapshots load into temporary state and replace live records only after full validation (explicit version check, bounds-checked lengths); corrupt snapshots are preserved, never silently deleted. Hibernation returns explicit success/failure and releases records only after write+fsync+close+checked-rename all succeed. Durability policy via `SOMNIUM_AOF_SYNC` (`always`/`everysec` default/`no`); everysec fsyncs from the watchdog thread so the command thread never blocks on fdatasync. AOF replay applies stored metadata verbatim - replayed SETs keep their original CRDT timestamps, so local SET vs remote CRDTMERGE resolves identically after restart. Recovery also bypasses the active-room budget so history with more than 3 rooms is never dropped. Verified by a 76-point per-byte truncation sweep, v1/v2 migration round-trips, mid-file corruption preservation, injected write/rename failures (room stays active and readable), corrupted-magic/version/truncated snapshot preservation, CRDT ordering across restart, and recovery past the 3-room budget; suite green under AddressSanitizer.

**Inspiration:** A load-bearing connection becomes a reliable contract between in-memory mutations and their persistent representation.

**Why now:** AOF recovery drops the command token from legacy entries and can truncate inside an incomplete record. Snapshots free records after an unchecked rename, and reads can delete a corrupt snapshot. New SET timestamps during replay can also change CRDT ordering.

**Deliver:**
- Introduce unambiguous, versioned AOF records and an explicit migration path for existing formats. Room names that resemble commands must not be guessed as format markers.
- Validate complete RESP records and remember the last fully valid byte offset. Handle an incomplete final record without consuming or overwriting valid history; preserve files containing other corruption for diagnosis.
- Serialize the resulting mutation, including room, CRDT timestamp/node, and expiry. Define how replay relates to snapshots so stale snapshots cannot overwrite newer recovered state.
- Make persistence return explicit success/failure. Validate snapshot version and lengths, load into temporary state, check write/close/rename results, and release live records only after a successful replacement.
- Implement and document a durable-acknowledgement policy using `fdatasync`/`fsync` as appropriate. Stream `flush()` alone does not establish power-loss durability.

**Acceptance checks:**
- Legacy SET/DEL fixtures migrate correctly, and a room literally named `SET` survives round-trip recovery in the versioned format.
- Truncate a final AOF command at every byte boundary. Recovery retains exactly the complete preceding commands, and a subsequent append survives another restart.
- Inject write, close, sync, rename, and malformed-snapshot failures. They neither discard the last usable state nor falsely acknowledge successful persistence.
- Local SET followed by remote CRDTMERGE resolves identically before and after restart, including snapshot/cold-tier transitions.

**Primary files:** `src/storage/aof_manager.*`, `src/storage/snapshot_manager.*`, `src/core/database.cpp`, `record.h`.

## Execution and completion

- ~~Start with S5's storage contracts and regression fixtures~~ (S5 DONE), ~~then S4 and S1. Build S2 on the resulting record/expiry semantics.~~ (S4, S1, S2 DONE) S3 is independent of the storage work.
- Each ticket includes its acceptance tests and documentation in its implementation change.
- Integration tests use a fresh temporary data directory, an explicitly selected free port, and verification that the launched process owns the endpoint. Development data and an existing Redis service are not test fixtures.
- Add the regression suite to CTest and run it from a clean build. Record benchmark conditions for the slow-client and compaction demonstrations.
- Sprint demo: create an expiring key in one room, switch rooms, exercise cold storage and hibernation, stall a subscriber, restart, and show correct values, deadlines, room states, and reclaimed disk space.

---

# Sprint 2: Odd Words, Fewer Ceilings

**Goal:** Remove the remaining hard ceilings: inputs that can kill the server, clients that cannot connect, a log that grows forever, memory that no budget actually bounds, and a CRDT that never leaves its node.

**Status:** In progress. S6 done; S7 implemented, awaiting CI verification; S8 to S10 planned. Five tickets, each inspired by one supplied word.

**Planning target:** One two-week sprint for S6 to S9; S10 may spill into the next one. Completion means meeting the acceptance checks below.

| ID | Inspiration | Deliverable | Priority | Depends on |
| --- | --- | --- | --- | --- |
| S6 | hyaenanche | Linear, hardened RESP parser and deadlock-free Pub/Sub | P0 | None |
| S7 | catholicized | Redis client compatibility | P1 | S6 |
| S8 | rebag | AOF rewrite as current state | P1 | S1, S4 |
| S9 | scolog | Byte-based memory budget | P1 | S1, S4 |
| S10 | koreish | Batched CRDT replication between nodes | P2 | S6, S8 |

P0 fixes failures that can take the server down. P1 removes ceilings on adoption, disk and memory. P2 builds the differentiating feature on top.

## S6 - Hyaenanche: Poison for Scavengers

**Status: DONE.** Verified by CI on PR #2 (AddressSanitizer suite, ThreadSanitizer and benchmark jobs green); the deep-pipeline numbers are in that run's `benchmark` artifact and are not yet copied into the README. The AOF already had a correct offset-based RESP parser, so it moved to `src/core/resp.h` and now serves both the network and AOF recovery: integers via `std::from_chars` (no temporary strings), header scan bounded to 32 bytes, CRLF checked after every bulk, limits of 1M arguments and 512 MB per bulk. The event loop parses from an offset and erases consumed input once per batch, so a deep pipeline is linear instead of quadratic. Malformed input gets `-ERR Protocol error` and closes only that connection; replies to the commands before it are still sent. `publish()` copies the subscriber list under the lock and delivers after releasing it, so disconnecting a subscriber during delivery can no longer self-deadlock. Tests: `tests/resp_test.cpp` (every prefix of a command waits for more bytes, malformed cases, pipelined offsets; run by `run_all.sh`), `tests/s6_test.py` (protocol errors, byte-by-byte delivery, a subscriber pushed past the 32 MB cap). The CI benchmark job now also records a 100-byte SET at `-P 64` for both servers; the before/after gap is not measured yet.

**Inspiration:** A plant whose fruit was used to poison hyenas becomes a defense against the inputs and clients that can bring the server down.

**Why now:** `parse_resp()` erases consumed bytes from the front of the input buffer after every command, so a deep pipeline (the buffer may reach 128 MB) costs quadratic copying; this is the likely cause of the 2x gap on the deep-pipeline insert benchmark. Lengths go through `stoi(substr(...))`: `$-1` wraps to a huge `size_t` and silently corrupts the stream, and a length near `INT_MAX` overflows. Separately, `PubSubManager::publish()` holds `ps_mutex` while delivering; disconnecting a slow or dead subscriber during delivery calls `remove_client()`, which locks the same mutex on the same thread and hangs the whole server.

**Deliver:**
- Parse from a read offset and erase consumed input once per batch. Parse integers with `std::from_chars`, without temporary strings.
- Reject malformed multibulk input (negative or oversized counts and lengths, a wrong type prefix, a bulk without its trailing CRLF) with `-ERR Protocol error` and close the connection, instead of silently clearing the buffer.
- In `publish()`, collect the subscriber targets under the lock and deliver after releasing it.

**Acceptance checks:**
- The deep-pipeline insert benchmark (100k x 100B, README conditions) is recorded before and after, and the gap to Redis shrinks.
- `$-1`, `$99999999999`, `*-1` and a bulk missing its CRLF each produce a protocol error and a closed connection, while other clients keep being served.
- A command split at every byte boundary across reads parses exactly once.
- A subscriber pushed past the output cap, and one whose socket resets mid-PUBLISH, are disconnected without hanging the publisher. Bounded test, AddressSanitizer clean.

**Primary files:** `main.cpp`, `pubsub.*`.

## S7 - Catholicized: Every Client Welcome

**Status: IMPLEMENTED, awaiting CI verification.** `resp::parse_request` accepts inline commands (LF or CRLF, split on spaces and tabs, 64 KB line cap) and otherwise defers to the strict `resp::parse`, which AOF recovery keeps using. New commands: `PING`, `ECHO`, `EXISTS`, `MGET`, `MSET`, `EXPIRE`, `PEXPIRE`, `PERSIST`, `UNSUBSCRIBE`, and `QUIT` (handled in the event loop, since it closes the connection); `SUBSCRIBE` takes several channels, and `PING`/`UNSUBSCRIBE` work in subscribe mode. Every key access goes through `lookup()`, so the new reads see cold storage and treat expired keys as absent. `MSET` is one AOF record (atomic under a crash), and expiry commands log the absolute deadline in AOF meta so replay never restarts a TTL; a non-positive TTL reuses `DEL`, as in Redis. SET, CRDTMERGE and MSET now share one `insert()` for new records. Tests: `tests/s7_test.py`, including real `redis-cli` and `redis-benchmark -t ping,set,get,mset` runs (CI installs `redis-tools`), and inline cases in `tests/resp_test.cpp`.

**Inspiration:** Made universal: any Redis client, tool or library should work unmodified.

**Why now:** `COMMAND` and `HELLO` return empty arrays, input that does not start with `*` is dropped, and `PING`, `EXISTS`, `MGET`, `MSET`, `EXPIRE` and `UNSUBSCRIBE` are missing. `redis-benchmark` can only run its SET/GET tests, and a subscribed client can never leave subscribe mode.

**Deliver:**
- Inline commands (one line split on spaces) next to RESP multibulk.
- `PING [message]`, `ECHO`, `EXISTS key [key ...]`, `MGET`, `MSET`, `EXPIRE`/`PEXPIRE`/`PERSIST` on top of S2's absolute deadlines, `UNSUBSCRIBE [channel ...]` and `QUIT`. PING and UNSUBSCRIBE also work in subscribe mode.
- New mutations persist their resulting state through the existing AOF meta (EXPIRE logs the absolute deadline), so replay stays verbatim.
- Keep the if-chain dispatcher until it measurably hurts; no RESP3.

**Acceptance checks:**
- An interactive `redis-cli` session works, and `redis-benchmark -t ping,set,get,mset` runs without errors.
- `EXPIRE` survives restart without extra lifetime; `PERSIST` removes the deadline; both are visible through `TTL`.
- `MGET` and `EXISTS` see cold-storage keys and treat expired keys as absent.
- A subscriber can UNSUBSCRIBE from every channel and then run normal commands.

**Primary files:** `main.cpp`, `src/core/database.*`, `pubsub.*`.

## S8 - Rebag: Pack the Log Again

**Inspiration:** Repacking a bag becomes rewriting the append-only log as current state instead of full history.

**Why now:** `appendonly.aof` only grows. Startup replays all of history, so recovery time and disk usage grow without bound, and every restart reloads every key into RAM.

**Deliver:**
- Rewrite the AOF as current state through the existing `start_rewrite` / `append_rewrite` / `commit_rewrite` path: resident keys, cold-storage keys read through the index (S1 relies on the AOF holding every key), and sleeping rooms.
- Decide and document how sleeping rooms are handled: either serialize them (loading each snapshot under its room lock) or keep the snapshot as their base and discover snapshot-only rooms at startup.
- `REWRITEAOF` runs on request and automatically when the AOF reaches twice its size after the last rewrite. Synchronous on the command thread first, marked as a known ceiling; a background rewrite only once that pause is measured.
- Expose the current AOF size and the size after the last rewrite as metrics.

**Acceptance checks:**
- 10,000 overwrites of 100 keys rewrite to about 100 records; restart restores identical values, deadlines and CRDT versions.
- Keys in cold storage and in sleeping rooms survive a rewrite followed by a restart.
- Expired keys are not written.
- A failure injected at create, write, sync or rename leaves the old AOF in use, and appends continue.

**Primary files:** `src/storage/aof_manager.*`, `src/core/database.cpp`, `src/storage/eviction_manager.*`, `src/storage/snapshot_manager.*`.

## S9 - Scolog: Rooms Pay Rent

**Inspiration:** A tenant farmer paying rent for church land becomes rooms paying for RAM in bytes.

**Why now:** Both budgets are counts (3 active rooms, `SOMNIUM_MAX_KEYS` keys per room) and neither bounds memory: a million 1 MB values is a terabyte.

**Deliver:**
- Track resident bytes per room (key, value and a fixed per-record overhead) at the centralized points S1 and S4 created: `lookup()`, SET/CRDTMERGE, eviction, expiry, `activate()` and `hibernate()`.
- `SOMNIUM_MAXMEMORY` sets one global byte budget. Eviction runs while over budget; waking a room that cannot fit returns a clear error.
- Report bytes in `ROOM.INFO`, `INFO` and the metrics exporter. An estimate is enough; no allocator-level accounting.

**Acceptance checks:**
- Writing past the budget with mixed value sizes keeps resident bytes within the budget plus one record, and every key stays readable.
- The byte counters return to zero after deleting, expiring, evicting or hibernating everything (no drift).
- Waking a room larger than the free budget returns an error and changes nothing.

**Primary files:** `src/core/database.*`, `src/core/room.h`, `src/storage/eviction_manager.*`, `metrics.*`.

## S10 - Koreish: Caravans Between Nodes

**Inspiration:** The merchant caravans of the Koreish become batches of CRDT merges carried between nodes.

**Why now:** CRDTMERGE is commutative and idempotent, and every AOF v3 record already carries `(timestamp, node)`, but `LOCAL_NODE_ID` is hard-coded to 1 and nothing ever leaves the node. Convergent multi-node replication is the differentiating feature, and most of its parts already exist.

**Deliver:**
- `SOMNIUM_NODE_ID` and `SOMNIUM_PEERS` (a `host:port` list).
- On each watchdog tick, a sender reads AOF records from its saved offset and ships them to each peer as one pipelined batch of CRDTMERGE. Only records that originated on this node are shipped, so nothing echoes back. Delivery is at-least-once, and the offset persists so a restart resumes.
- Versioned delete markers so a replicated DEL cannot be undone by a late, older merge. If they are deferred, document "writes replicate, deletes stay local" as the explicit first limit.
- Sender offsets survive an AOF rewrite (S8).

**Acceptance checks:**
- Two nodes writing the same keys concurrently converge to identical values and versions after a quiet period.
- Killing a peer mid-batch and restarting it still converges; duplicate deliveries change nothing.
- Records received from a peer are never re-sent.
- If delete markers are in scope: a DEL on one node followed by an older remote merge stays deleted on both.

**Primary files:** a new replication component, `src/storage/aof_manager.*`, `src/core/database.cpp`, `watchdog.cpp`.

## Sprint 2 execution and completion

- Land S6 first: it removes the two server-killing failures and its parser work is where S7's inline commands live.
- S8 and S9 are independent of each other; S10 waits for S8 because rewrites move AOF offsets.
- Each ticket includes its acceptance tests, a line in the README command or configuration reference, and its status paragraph here.
- Sprint demo: pipeline 100k inserts and compare with Redis, connect with `redis-cli`, rewrite a long AOF and restart instantly, fill past `SOMNIUM_MAXMEMORY`, and show two nodes converging.

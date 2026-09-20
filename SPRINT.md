# Sprint: Odd Words, Solid Foundations

**Goal:** Make SomniumDB more dependable under storage churn, room switching, and slow clients, while adding useful command features.

**Status:** Planned. These are five implementation tickets, each inspired by one supplied word.

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

- Start with S5's storage contracts and regression fixtures, then S4 and S1. Build S2 on the resulting record/expiry semantics. S3 is independent of the storage work.
- Each ticket includes its acceptance tests and documentation in its implementation change.
- Integration tests use a fresh temporary data directory, an explicitly selected free port, and verification that the launched process owns the endpoint. Development data and an existing Redis service are not test fixtures.
- Add the regression suite to CTest and run it from a clean build. Record benchmark conditions for the slow-client and compaction demonstrations.
- Sprint demo: create an expiring key in one room, switch rooms, exercise cold storage and hibernation, stall a subscriber, restart, and show correct values, deadlines, room states, and reclaimed disk space.

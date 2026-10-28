# Storage primitives and durable SQLite snapshots

The existing `Storage`/`MemoryStorage` API remains an independent volatile record store. Phase 2b adds `durable::SqliteStore` for the dedicated broker persistence worker. `Storage` provides per-record get/put/remove for opaque encoded records in separate session, retained and inflight namespaces. `MemoryStorage` enforces entry, key, value and aggregate logical byte limits. Rejected writes do not change existing state. Operations require exclusive ownership for writes; callers choose synchronization. Record data is borrowed on reads, so there is no additional value allocation on lookup.

This is volatile storage. It does not implement durable sessions, MQTT expiry, QoS recovery, offline message delivery, crash recovery or a complete session serialization schema. Those cannot be claimed until broker/session integration and restart tests exist. Byte accounting excludes allocator/tree overhead, which is bounded indirectly through entry and per-record limits. Caller-owned input allocations exist before put and must be bounded at ingress.

## Persistence evaluation and decision

| Backend | Consistency and durability | Performance tradeoff | Maintenance cost |
| --- | --- | --- | --- |
| BTreeMap (implemented) | Single-process, per-record atomic mutation, no durability | No disk IO; O(log n) keys | Low, std only |
| SQLite WAL (implemented in Phase 2b) | Transactions can atomically update session plus inflight state; durable acknowledgement requires an explicit synchronous policy and successful commit | Blocking operations need a bounded worker; batching can amortize commit IO | Established format, migrations/backup and checkpoint policy still required |
| Append-only custom log | Requires checksums, torn-write recovery, commit markers and compaction design | Sequential writes potentially useful; recovery and compaction add costs | High; not selected for initial persistence |
| Atomic full-file snapshot | Rename atomicity alone does not establish power-loss durability; needs file and directory sync and recovery policy | Rewrites all data per commit; poor fit for frequent inflight mutations | Moderate, difficult ambiguous commit errors |

No performance measurements were made, so the comparison is qualitative. The Phase 2b snapshot and transaction contract is frozen in `docs/contracts/phase2b.md`. SQLite is now a dependency and provides the opaque snapshot commit layer; session encoding and MQTT transaction ordering remain the runtime caller's responsibility. A backend must durably commit before the relevant MQTT acknowledgement; transitions spanning records require a transaction rather than separate `put` calls. The current trait will therefore need a coordinated extension before durable QoS2 support.

Persistent IO must run in a bounded `spawn_blocking` worker or dedicated thread and never on a Tokio runtime worker. Acceptance requires restart/torn-write tests, transaction rollback, disk-full failures, schema migration, retained replacement, session expiry and QoS state recovery. No benchmark numbers are claimed.


## Atomic memory batches

`MemoryStorage::apply_batch(Vec<Mutation>)` applies ordered `Put` and `Remove`
operations atomically under exclusive mutable ownership. It validates all supplied
keys (including removals) and values before cloning records, and rejects more than
`max_entries` operations or more than `max_bytes` summed incoming value bytes.
Every supplied value counts, including repeated puts to the same key that would
subsequently be overwritten. Checked addition rejects size overflow. Input is
caller-owned and already allocated; callers must bound allocation before invoking
this synchronous API.

The implementation stages a second bounded copy of the current records, applies
operations using existing single-record validation and accounting, then replaces
the current state only if every operation succeeds. This bounds staging to one
additional store plus the bounded supplied batch, rather than cloning per mutation.
All **intermediate** states must satisfy entry/byte limits; when full, remove an
old entry before adding a replacement. A put followed by a removal can fail even
if its final result would fit. Batch operation count is itself limited by
`max_entries`, so a one-entry store cannot use a two-operation batch. Empty batches
succeed without cloning. Removing a missing valid key is idempotent. Failed
batches leave values, namespaces, entry count and byte accounting unchanged.

This API offers volatile in-memory atomicity only. It does not add durability to MemoryStorage. The separate SQLite API below stores
the versioned session/outbox snapshot defined by the Phase 2b contract.


## Durable SQLite snapshot API

`durable::SqliteStore::open(path, max_snapshot_bytes)` opens one local database.
`load()` returns its optional opaque snapshot. `commit(bytes)` atomically replaces
the entire snapshot. The version-1 SQL schema is a single `broker_snapshot` table
with `id=1`, `schema_version=1`, and `data BLOB`; `PRAGMA user_version=1` and the
RMQT application ID identify it. The session layer separately versions and
validates its JSON envelope. Unknown schema versions, unexpected objects/table
definitions, corrupt databases and oversize stored snapshots fail closed.

The implementation uses rusqlite 0.41 with bundled SQLite (tested version 3.54.1),
verifies WAL mode and synchronous=FULL, and performs each replacement in an
IMMEDIATE transaction. Length is queried before BLOB allocation. Oversize writes
fail before SQL. SQL errors roll back; callers must not install proposed session
state or send success ACKs on any commit error. Reopening the database is the
authority after an ambiguous IO/commit failure. Durability relies on the local
filesystem and hardware honoring synchronization requests; no physical power-loss
experiment or network-filesystem guarantee is claimed.

A lifetime nonblocking OS advisory lock uses the canonical database path plus
`.broker-lock`. The permanent sidecar's existence does not indicate ownership and
it must not be deleted. Process termination releases the lock. A lock on the DB
inode itself conflicted with SQLite's native locks on this platform; the test
suite reproduced DatabaseBusy, then passed after the sidecar correction. Parent
directories must exist. Canonical/symlink path aliases use the same lock. Hard-link
aliases and external rename/replacement while running are unsupported. Every broker
writer must honor this API; external SQLite tools must not mutate a live DB.

Blocking operations belong only on the runtime's bounded blocking worker, never
on a Tokio async worker. This layer cannot by itself provide MQTT recovery or
application exactly-once behavior: the runtime must commit its proposed persistent
registry/outbox before installing memory state and writing protocol ACKs.

Tests cover reopen, limits and old-value preservation, exclusive owner locking,
foreign/version/corrupt schema refusal, a real SQLITE_FULL error followed by
rollback and continued operation, and a child killed with an uncommitted SQLite
transaction. After the kill, the prior snapshot survives and the OS lock can be
reacquired. These are process-crash/SQLite transaction checks, not physical disk
failure tests. Whole snapshots have write amplification; WAL growth, disk quotas,
backup/migration policy and performance benchmarks remain future work.

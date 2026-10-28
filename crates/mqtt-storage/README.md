# Storage groundwork

This crate is **not wired into the broker runtime**. `Storage` provides per-record get/put/remove for opaque encoded records in separate session, retained and inflight namespaces. `MemoryStorage` enforces entry, key, value and aggregate logical byte limits. Rejected writes do not change existing state. Operations require exclusive ownership for writes; callers choose synchronization. Record data is borrowed on reads, so there is no additional value allocation on lookup.

This is volatile storage. It does not implement durable sessions, MQTT expiry, QoS recovery, offline message delivery, crash recovery or a complete session serialization schema. Those cannot be claimed until broker/session integration and restart tests exist. Byte accounting excludes allocator/tree overhead, which is bounded indirectly through entry and per-record limits. Caller-owned input allocations exist before put and must be bounded at ingress.

## Persistence evaluation and decision

| Backend | Consistency and durability | Performance tradeoff | Maintenance cost |
| --- | --- | --- | --- |
| BTreeMap (implemented) | Single-process, per-record atomic mutation, no durability | No disk IO; O(log n) keys | Low, std only |
| SQLite WAL (recommended next) | Transactions can atomically update session plus inflight state; durable acknowledgement requires an explicit synchronous policy and successful commit | Blocking operations need a bounded worker; batching can amortize commit IO | Established format, migrations/backup and checkpoint policy still required |
| Append-only custom log | Requires checksums, torn-write recovery, commit markers and compaction design | Sequential writes potentially useful; recovery and compaction add costs | High; not selected for initial persistence |
| Atomic full-file snapshot | Rename atomicity alone does not establish power-loss durability; needs file and directory sync and recovery policy | Rewrites all data per commit; poor fit for frequent inflight mutations | Moderate, difficult ambiguous commit errors |

No performance measurements were made, so the comparison is qualitative. SQLite is an architectural recommendation, not an added dependency or implemented guarantee. Freeze a versioned record schema and transaction API with Session/QoS before implementing it. A backend must durably commit before the relevant MQTT acknowledgement; transitions spanning records require a transaction rather than separate `put` calls. The current trait will therefore need a coordinated extension before durable QoS2 support.

Future persistent IO must run in a bounded `spawn_blocking` worker or dedicated thread and never on a Tokio runtime worker. Acceptance requires restart/torn-write tests, transaction rollback, disk-full failures, schema migration, retained replacement, session expiry and QoS state recovery. No benchmark numbers are claimed.


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

This API offers volatile in-memory atomicity only. It does not add crash recovery,
durable MQTT acknowledgement ordering, persistent sessions, or a SQLite backend.
A future storage transaction trait and versioned outbox/session schema still need
coordinated design and fault/restart tests before durable QoS support.

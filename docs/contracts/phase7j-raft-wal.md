# Phase7j — independent Raft WAL

Frozen before implementation. User supersedes Phase7h SQLite Raft-log backend.
The unimplemented Phase7i batching idea is deferred: do not mix admission/model
changes into this storage rewrite. Metadata state machine stays incremental RAM;
SQLite stores only identity and business-state snapshots/checkpoints after migration.

## Frozen blocking WAL boundary

Use exact raft-log0.4.6, reviewed official source, no homemade Raft consensus.
`storage::wal::Wal` owns the raft-log instance. All methods are blocking and called
only on bounded spawn_blocking workers, serialized by one WAL mutex. Methods:
- open(path: &Path, cluster: &str, node: u64) -> io::Result<Wal>
- entries(from: u64, to: u64) -> io::Result<Vec<Entry<Types>>> (inclusive)
- vote() -> io::Result<Option<Vote<u64>>>; save_vote(Vote<u64>) -> io::Result<()>
- committed() -> io::Result<Option<LogId<u64>>>;
  save_committed(Option<LogId<u64>>) -> io::Result<()>
- state() -> io::Result<LogState<Types>>
- append(Vec<Entry<Types>>) -> io::Result<()>
- truncate(LogId<u64>) -> io::Result<()>; purge(LogId<u64>) -> io::Result<()>
All mutation returns mean flush(true) callback completed, including commit/vote/
truncate/purge. No relaxed committed durability in this phase. OpenRaft append
callback only after this point. WAL I/O failure poisons instance against subsequent
successful operations. Reads validate IDs, bounds and existing Entry SHA envelope.
Cache and flush queue finite; chunk rollover finite; fail closed on corrupted or
incomplete records, never silently discard an acknowledged tail. WAL lives under
controller directory/raft-wal; identity/version persisted and checked before use.
No SQLite write for Raft entries, vote, committed/purged positions after migration.

## Storage/state-machine integration

Store public OpenRaft traits, State/Metadata/Delta and checksum payload schema stay.
Separate SQLite checkpoint and WAL serialization; consistent lock order, never await
while holding std locks. Verify durable inputs from WAL before normal RAM apply.
Snapshot SQLite commit happens BEFORE WAL purge. Snapshot install persists valid
checkpoint first then advances WAL committed floor; crash between is recoverable
using checkpoint as authority floor. Recovery replays contiguous committed-only tail
from WAL after verified checkpoint; missing IDs, conflicting identities/checksums
fail closed. Existing cancellation-owned apply/install serialization remains.
Direct trait apply still checkpoints before ACK when no recoverable durable input.

Legacy SQLite logs/vote/committed/purged are imported only after full validation,
under exclusive controller directory lock and bounded streaming. Destination import
must flush completely, sync directory/atomic publish, then record SQLite migration
marker. Interrupted migration is safely retryable; old source never deleted before
completion. Once WAL migration marker exists, missing WAL fails closed, no fallback
to stale SQLite. Legacy SQL Raft records may be retained as inert backup or removed
after durable publication; never dual-write. Existing snapshots remain compatible.
Downgrade/mixed-version cluster unsupported. No deleting deployed state directories.

## Ownership and acceptance

WAL owner owns ONLY storage/wal.rs and storage/wal/**. Coordinator owns Cargo.toml/
lock, docs, benchmark scripts and CI integration. Storage integrator owns storage.rs,
storage/recovery.rs, storage/log.rs and existing storage/tests.rs; coordinate exact
boundary above first. Quality owns tests/cluster_raft_wal* and independent code
review; no shared worktree writes. Each assigned independent worktree/branch, test
before fix and return commits. lib.rs docs/mod/reexports only.

Tests: reopen vote/committed/append, overwrite/truncate/purge, corrupt/torn tail,
failed flush callback, committed gap/speculative exclusion, snapshot install/purge
ordering and crash restart; migration interrupted stages and stale-SQLite rejection;
OpenRaft full storage conformance plus existing control-tail/reliable forwarding/
replica recovery tests. Fmt, strict all-target Clippy, workspace tests, local locked
amd64-musl Zig release. No performance claim without measured comparison. Server
pressure only after storage correctness gate; other known admission bottlenecks
must remain reported rather than silently attributed to SQLite alone.

Adapter mapping clarification (coordinator approved before implementation): persist
committed watermark in raft-log UserData alongside identity/format, in the same WAL
checkpoint record with sync completion, rather than native commit action. Native
commit requires <=last and forbids covered truncation, incompatible with the existing
snapshot-floor/direct-trait semantics. Preserve monotonicity and same-index identity;
no fabricated log entries/last positions. Native append/vote/truncate/purge remain.
Wal::open receives the WAL directory itself, creates only a nonexistent directory;
existing empty/missing-identity directories fail closed. Strict official DumpApi
preflight validates ALL existing records before open can auto-clean a tail.

Flush configuration: explicitly zero flush_batch_wait, preserving native ready-queue
aggregation without the upstream default1ms deliberate wait. Keep finite flush item/
byte queues, read buffer, payload cache and chunk rollover; report actual configured
values. Sync completion remains mandatory, no timer-only durability.

Lifecycle clarification: underlying WAL Drop performs sync and joins its worker.
Store.Inner must move its owned WAL/SQLite/controller.lock together into at most one
blocking cleanup job when dropped on a Tokio worker; hold directory lock until WAL
and database shutdown finish. No per-request cleanup tasks. Blocking open may wait
at most2s for the exclusive directory lock, to tolerate immediately reopening a
locally closing store; a genuinely held directory still returns WouldBlock. No
release-lock-then-write race. Synchronous/nonruntime close stays on calling blocking
thread. The production Controller owns a single Inner instance.

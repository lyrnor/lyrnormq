# Phase7k — bbolt Controller checkpoints

Frozen before implementation. Replace Controller SQLite with bbolt git tagv0.1.0
(commit3aa4b1c1f44fe3a67e5a8b718978dc7af0315962), default-features=false.
Raft WAL/Entry/vote/committed/purged, MQTT/RAM replicas, epochs, ACK and applied
barrier semantics unchanged. This phase does not change admission capacities.

## Checkpoint adapter API / ownership

Checkpoint owner owns storage/checkpoint.rs and storage/checkpoint/** only.
`Checkpoint::open(path:&Path, cluster:&str,node:u64)->io::Result<Self>` receives
controller data directory; underlying file controller.bbolt. Blocking methods:
`read_snapshot(&self)->io::Result<Option<SavedSnapshot>>` and
`write_snapshot(&mut self,&SavedSnapshot)->io::Result<()>`.
Use existing parent SavedSnapshot/State and digest validation; no SQLite dependency.
Identity, version, checkpoint last-applied/membership and business state must be one
atomic transaction. New format version1. Store business Metadata maps as separate
buckets (nodes/clients/routes/durable); metadata scalars and State/Snapshot metadata
in a metadata bucket. Encode each record with SHA256/integrity and finite lengths;
bounds checked before deserializing. Reconstruct the exact public State/Metadata
JSON representation; derived caches rebuilt by existing model path.
Checkpoint writes compare/upsert changed records and remove deleted keys in one
transaction; unchanged records need not rewrite. Scanning an entire frozen State
at periodic checkpoint is permitted, normal Raft apply remains sparse RAM+WAL.
Do not store a duplicate full-state/snapshot JSON BLOB as the primary business DB.
Keep existing16MiB raw Snapshot bound and16711680 metadata budget (KV backend does
not remove wire limits). File max256MiB, finite transaction data, no long-lived
read tx across await. no_sync=false/no_grow_sync=false, application owns allocator.
On uncertain commit or committed-mapping failure poison instance, fail closed,
never treat error as successful rollback or blindly retry. All DB IO and Drop run
on existing bounded blocking workers. read_snapshot returns latest durable check-
point only; valid identity-only DB returns None, missing/corrupt fields fail closed.

## Store integration

Integration owner owns storage.rs, storage/recovery.rs, storage/log.rs and storage/
tests.rs, storage/wal_integration_tests.rs. No changes wal.rs/wal/** or model.
Replace Connection with Checkpoint; no rusqlite in mqtt-cluster dependencies or
normal Controller code. WAL→checkpoint lock order, owned apply/install cancellation
serialization and whole-resource deferred cleanup/directory lock retained.
Snapshot-before-purge and contiguous committed-only replay unchanged. Persist valid
business checkpoint BEFORE WAL floor advancement and RAM publication. Direct
trait apply still persists checkpoint before ACK; OpenRaft conformance required.
Entry checksum codec remains storage/log.rs; obsolete SQLite migration code removed.
No silently recreate missing/invalid DB when WAL exists or migration proves prior
state. Fresh store initialization distinguishes nonexistent/new data vs lost DB.

## Explicit offline migration exchange

Coordinator owns scripts/export_control_sqlite.py and migration documentation.
The normal Controller must not open SQLite. For legacy metadata.sqlite directories
without controller.bbolt, require an offline export, never silently initialize.
Exporter uses Python stdlib sqlite3, exclusive controller.lock, read-only DB query,
requires completed Phase7j raft_wal_migration=1 and existing raft-wal; old pre-WAL
SQLite logs require upgrading with Phase7j first. No source deletion or modification.
Atomic exchange file `controller-checkpoint.export.json` with schema:
`{"format":1,"identity":[cluster,node],"state":<optional legacy State>,
"snapshot":<optional existing SavedSnapshot>,"snapshot_record_version":<optional u64>}`.
Exporter cap128MiB; atomic temp write/fsync/rename/parent fsync. New Store under
exclusive lock validates identity, versions/checksums and chooses newest coherent
legacy State/snapshot; initializes staging bbolt, recovers/verifies WAL tail before
publishing, sync/drop/rename/dirsync. Existing valid bbolt ignores stale SQLite or
exchange backup. Interrupted staging retry safe, published bbolt never replaced
from old exchange. If existing WAL lacks required checkpoint and no exchange,
fail closed; no losing acknowledged business state. Legacy source preserved.

## Quality, integration and benchmark

Quality owner owns tests/cluster_bbolt* and coordinated updates to
cluster_control_optimization_blackbox.py, cluster_raft_wal_blackbox.py and the
Controller file-layout assertion in cluster_memory_blackbox.py only.
New assertions inspect real Controller file layout and no creation/modification of
SQLite. Keep historical SQLite-gate evidence, update current CI to bbolt gates.
Real previous Phase7j binary used for migration/restart validation. Test corruption,
missing state store, atomic checkpoint/deletes, import interruption, commit error
propagation, snapshots/purge/reopen, migration and QoS2 recovery, double versions
and all QoS with current RAM fixtures. Each writer uses explicitly assigned Git
worktree/branch, tested commits, lib.rs declarations/reexports only.

Coordinator owns Cargo.toml/lock, docs, CI, export script, benchmark helpers and
loadgen diagnostics. Full fmt/strictClippy/workspace tests and local locked Zig
amd64-musl build. Deploy to fresh dir on47.84.224.186, load47.84.241.200 via private
addresses. Same3dualrole topology, limits/session/admission unchanged and explicit.
First1000/s cumulative10k..100k run; stop matrix on first failed plateau, preserve
logs/resources and restore source port settings. Only if it passes continue2000/
4000 and10kTCP/10kpublish/s128B QoS0/1/2, correct mean/p99/ACK/loss reporting.
Backend replacement alone does not prove prior admission/reacquisition bottlenecks
fixed. Never compile Rust on servers or hide failed setup as measured message TPS.

Coordinator clarifications before integration: input snapshots are validated against
their original digest/identity/membership before canonical State serialization and
recomputed checkpoint digest; public logical State preserved, raw JSON whitespace
is not a durable contract. No duplicate raw snapshot body is required.
Publish bounded `controller.bbolt.required` identity/version marker atomically with
file+parent-directory sync AFTER Bbolt publication and BEFORE Store readiness/ACK.
If marker exists and Bbolt is missing, never reimport a stale exchange. Validate
marker content/identity; existing valid DB without marker is verified/recovered
before completing the marker. Marker is the O(1) missing-store fence, not business
storage or a substitute for the independent Raft committed watermark.

Long-key amendment before deployment: bbolt physical keys are capped32768bytes,
while valid MQTT Client IDs/Topic Filters and prefixed owner keys can reach65538.
Business buckets use32-byte SHA256(logical key) physical keys; values include a
bounded u32 logical-key length, full UTF8 logical key, contextual record digest and
data. Validate physical hash, logical length/checksum and duplicate/collision before
publishing a checkpoint. Metadata static keys stay unchanged. Never narrow MQTT
limits to accommodate the KV backend. This replaces the initial raw-key layout
before any deployed Phase7k business state exists.

Dependency correctness amendment: process snapshot gate exposed valid-row/global-
digest inconsistency with initial v0.1.0. Native-only repeated sorted-hash writes
reproduce missing keys at512 rows. Root cause: append hint did not enforce the
cached leaf's right boundary. The upstream fix limits the hint to a root leaf and
uses branch lookup otherwise (format/API unchanged), released as v0.1.1 at
3e33a39f4e3ac32123ddc8bfaa7638277a3e7a23 on an independent upstream branch.
Use THIS exact tag/locked revision, superseding the initial v0.1.0 requirement.
Keep deterministic native and repeated keyed-checkpoint RED regressions enabled.
No affected Phase7k store was deployed; prior failed fixture evidence retained.

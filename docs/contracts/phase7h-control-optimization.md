# Phase7h — incremental control state, durable log and applied-version barriers

Supersedes Phase7a/7e control hot-path implementation, not authority/ACK semantics.
RAM data replicas, W2/outbox/receipts, epochs, repair and event forwarding unchanged.
No new RocksDB, no synchronous=OFF/NORMAL, no automatic log/data loss recovery.

## Shared interfaces frozen before parallel work

Metadata::apply_tracked(Command) -> (Answer, Option<MetadataUpdate>), and
State::apply_tracked(Entry<Types>) -> same. Existing apply wrappers remain valid.
Successful revision-changing commands emit exact sparse Delta; errors/heartbeats
emit None. Existing serialized Metadata/State/Delta schemas/import paths retained.
MetadataUpdate::merge_in_place(self, &mut Metadata) -> Result<(), &'static str>;
validate base before mutation. Existing merge remains compatibility wrapper.

Store::watch_updates(base: Option<u64>) -> Vec<MetadataUpdate>: bounded batch of
contiguous deltas since base; Progress when caught up; Snapshot when no base or
history gap. History max256 updates/16MiB; batch max64 updates/16MiB (oversize single
bounded existing frame can be snapshot fallback). No unbounded journal/tasks.
Subscribe via existing Store.changed; it notifies on every applied batch including
heartbeats even if value revision unchanged, for tiny applied-version waiters.
Watch consumers compare scalar revision before cloning anything. Metadata snapshots
are allowed for bootstrap/gap/repair; not normal connect/barrier/delta application.
Coordinator interface baseline may use correct full-copy fallbacks; owners MUST
replace those in final implementation, not claim them as optimized.

## Incremental model owner

Own model.rs, repair_state.rs and new model/**. Preserve deterministic state-machine
results, exact8MiB metadata bound, node/client/count limits, rejected mutation atomicity,
route union dedupe/reference counts and session/repair generation fencing.
Normal Register/Claim/Release/Acquire/DurableFilters/Filters/Heartbeat touch only
changed entries/routes; no entire Metadata clone/rebuild/JSON encoding for each
command. Maintain exact incremental encoded-byte accounting and bounded route/node
indexes, skipped from serde. Rebuild derived caches once after deserialization;
unchanged wire/durable fields. Rare maintenance/repair may retain bounded global
validation with explicit tests, not the routine connection path. Delta emission
must use touched keys, not diff entire maps. merge_in_place applies deltas without
cloning old metadata. Existing apply remains thin wrapper through tracked apply.
Regression/property-style tests compare full serialization, byte limits, rollback,
overlapping node/filter references, replay and emitted/merged deltas.

## Raft storage owner

Own storage.rs and new storage/**. Preserve exclusive directory/identity and bounded
blocking IO. Logs/vote/committed index remain SQLite WAL/FULL durable. Flush callbacks
only after actual durable commit. OpenRaft committed watermark and flushed entries
must be durable before RAM apply can acknowledge; direct trait/conformance apply
also cannot create unrecoverable acknowledged state (ensure durable input/marker
if needed). No per-apply full State clone/serialize/blob write. CPU model work must
not hold SQLite IO mutex; serialize apply/install with bounded work and no locks
acrossawait. Publish RAM state + sparse history coherently, notify after install.

Periodic snapshots follow existing LogsSinceLast1000 policy. Persist snapshot bytes,
last included log ID and membership atomically before publishing snapshot as current
or purging covered entries. Snapshot install + recovery checkpoint + committed floor
must be crash-consistent. Purge cannot remove needed unsnapshotted committed entries.
Startup chooses valid durable snapshot/checkpoint, replays contiguous COMMITTED tail
only, never speculative tail. Missing/corrupt log/snapshot/identity fails closed.
Load historical state/checkpoint DB safely; explicit format validation, no destructive
migration. Downgrade/mixed-version operation unsupported. Snapshot/RPC bounds stay.
Maintain bounded sparse change history; bootstrap/gap returns snapshot. Tests first:
no per-apply state rewrite, crash with committed tail/uncommitted tail, snapshot
crash boundaries/install/purge, gaps/corruption, legacy DB and OpenRaft conformance.

## Runtime/barrier owner

Own controller.rs, controller/**, rpc.rs, rpc/**, broker.rs and broker/** (not model/storage). Add authenticated
bounded ReadRequest::Applied{revision} and Owner{key}; keep legacy Metadata/Status.
Applied waits via changed notifications for committed metadata revision and ALL
current live nodes applied>=target, not quorum-only. Revalidate leadership on success;
term/lease loss/gap must not return success. Whole server wait<=4s/client barrier<=5s,
bounded admitted waiters. Dedicated client timeout if needed; forwarding deadlines
unchanged. No20ms full-metadata reads. Owner returns only needed owner/epoch under
linearizable authority; inspect must not fetch full directory per connection.

Controller watch uses Store.watch_updates and scalar base; no per-update full clone
or map diff. Broker applies sparse deltas in place, copies only touched old entries
for fencing before publication; snapshots only bootstrap/gap. Preserve stale-client
fencing, SubAck route visibility, ownership/repair/readiness wakeups, observer gap
checks and event scheduler fairness. Heartbeat progress must not trigger routing
rebuild/whole-directory scan. No network/state lock acrossawait. Add focused tests
for full-read elimination, node lag/loss/term changes and watcher gaps/recovery.

## Coordinator/quality and acceptance

Coordinator additionally owns tests/cluster_control_optimization* and its CI gate,
benchmark helpers/preflight, bin/workspace/docs; other tests unchanged. Each writer
independent assigned worktree/branch, tested commits. Changes to shared interface
coordinated before implementation. lib.rs docs/modules/reexports only.

Benchmark config sets BOTH max_connections and max_sessions>=ceil(target/3), audits
FD/ports/state/record/subscription/admission budgets and records them. Do not quietly
raise protocol quotas/change consistency to pass. After tests/fault review, local
Zig amd64-musl release only, fresh three cohosted Controller+RAM Broker processes
on47.84.224.186; load47.84.241.200 over private addresses. Same requested rates1000/
2000/4000 and cumulative10k..100k plateaus; failed plateau stops that rate. Message
case10000TCP=5000pairs,10000publish/s,128B,QoS0/1/2, mean/p99/ACK/loss with explicit
setup/measurement/drain. Stop on first real failure for diagnosis, do not run rest
of known-failing matrix. No target-load p99 for failed setup. Existing RAM17/event10/
legacy controller crash/recovery tests retained; fullfmt/strictClippy/workspace tests.

Coordinator clarification: exact byte validation includes FINAL revision digits and
Heartbeat updates. Old code checked before revision increment and bypassed heartbeat
bytes; fix both with RED regression, deterministic rejection and complete rollback.
Do not add uncoordinated reserve policy or relax the8MiB bound.

Applied-progress amendment (coordinator approved before implementation): add
ReadRequest::AppliedAck{node,incarnation,revision}. Controller leader keeps at most
1024 volatile monotonic per-node ACKs, bound to current incarnation and Raft term;
reject unknown/stale/excluded node or revision beyond committed/applied metadata.
ACK cache loses authority on term change; no Raft/SQLite write per ACK. Barrier can
use same-incarnation volatile installation ACK OR durable Heartbeat.applied; initial
and final ReadIndex and all-current-live-node condition remain mandatory. A Broker
single coalescing task pushes latest installed revision only after fence/merge/
repair reconciliation; heartbeat success/leader change repushes current revision
even with no new delta. Finite retries and shutdown, no task per revision. This
removes250ms heartbeat reporting as the routine barrier latency floor without
multiplying consensus writes. Lost cache can delay/retry a barrier, never fake ACK.

Revision-exhaustion clarification: reject nonheartbeat mutation at u64::MAX rather
than saturating revision while changing state without a Delta. Rejection atomic.

Private checkpoint integrity amendment: versioned SavedSnapshot may add a SHA256
digest over meta+raw state bytes. Legacy unversioned snapshots load compatibly;
new-version records require a valid digest, matching last-included ID/membership.
Missing/mismatched digest for a new record fails closed. No public State, RPC
Snapshot or SQLite table schema change; use existing sha2 dependency.

Bounded replication batching amendment: permit max_payload_entries24 instead of8.
With512KiB command admission and small Entry/Append envelopes,24 full-sized entries
must encode within existing16MiB RPC bound; add actual encoder regression before
changing the setting. Single-command/MQTT/snapshot bounds and WAL/FULL unchanged.
This is finite replication batching, not an unbounded queue or weaker flush ACK.

Installed-watermark clarification: durable Heartbeat.applied also reads the fully
installed watermark (after fencing, merge and repair reconciliation), not an
intermediate canonical Metadata.revision. Heartbeat success may repush that same
watermark but must not promote it. A higher heartbeat answer revision must not
force healthy watch streams into repeated cold snapshots; keep true stream gap,
progress mismatch, registration recovery and stalled-stream timeout handling.

Committed-tail integrity amendment after independent review: newly appended log
records use a private versioned Entry+SHA256 envelope (including log identity and
payload), decoded back into unchanged public Raft Entries. Missing/mismatched hash
or unknown record version fails closed, including valid-JSON payload alterations.
Atomically validate/wrap legacy log rows and publish the new control-log format on
open; no destructive migration, no checksum claim for corruption predating legacy
migration. Snapshot/State/wire/SQLite table schemas unchanged. Malformed or gapped
committed tails still fail; uncommitted entries are never replayed. Add direct
valid-JSON corruption RED before fix and retain complete OpenRaft conformance.

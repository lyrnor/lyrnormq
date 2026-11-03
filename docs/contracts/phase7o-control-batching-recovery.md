# Phase7o — control batching and idempotent acquisition recovery

User authorizes fixing the reproduced control-path failure. Preserve WAL flush
callbacks, committed-only replay, W2, ReadIndex, all-live barriers, cancellation-
owned submitted work and lease fencing. Do not raise deadlines or reintroduce
reject-on-capacity quotas. No experimental OpenRaft fork is approved. lib.rs thin.

## Control/model/storage owner

Own controller.rs/controller/**, model.rs/model/**, storage.rs/storage/**, rpc.rs/
rpc/** and necessary existing mqtt-cluster/src/tests.rs and broker/control_tests.rs
fixture adaptations only. No broker/durable changes. Independent worktree.

Add internal Command::Batch { commands: Vec<Command> } and Answer.results: Vec<Answer>
(default empty, omit empty on serialization). Only the Controller's internal worker
may construct a batch; reject incoming Mutate Batch and nested/empty/malformed
batches before mutation. At most64 commands and512KiB encoded command batch; keep
existing512KiB single-command/1MiB WAL/16MiB RPC limits. Size counting must include
wrappers. Oversized individual valid commands run alone without artificial padding.

One owned Controller worker receives bounded FIFO queued requests with cancellable
async waiting before enqueue, no try-acquire rejection. Once enqueued work belongs
to worker even after caller cancellation. Drain only already-ready commands (no
batch-fill timer), bounded per batch by count/bytes; await one Raft client_write
completion before submitting next batch. This amortizes sync and removes concurrent
write flooding of Core without modifying OpenRaft. No unbounded waiting-task spawn.
Health/maintenance preserve ordering and server-assigned lease timestamps; don't
let a canceled reply abort a submitted write. Recheck leader before actual submit;
post-submit errors remain unknown. No response before durable quorum and apply.

Apply children in FIFO order using existing per-command validation/undo and distinct
metadata revisions/Acquire epochs. Each has its own Answer; one rejected child
neither rolls back successful siblings nor skips later children. Do not assign all
children the Raft index or final batch revision. Coalesce sparse MetadataUpdate
changes without full-state clones; exact base/final revision and final touched
values must match sequential apply. Keep intermediate command answers. last_applied
is per Raft entry, replay applies all children once. Existing singleton WAL/checkpoint
records remain readable and byte checksums unchanged. New batch records require
all Controllers upgraded together; no mixed-version/downgrade compatibility claim.

Add Command::AcquireOnce with SAME fields as Acquire. Legacy Acquire retains its
semantics. DurableOwner.acquired_from: Option<u64> (defaultNone, omittedNone) records
the predecessor epoch only for AcquireOnce. If current owner matches node,
incarnation,generation and acquired_from==expected_epoch, duplicate AcquireOnce
returns the original owner.epoch without revision/epoch change; still require valid
current node, readiness/repair constraints. Otherwise perform CAS against expected
and normal Acquire, recording acquired_from. Conflicting/newer ownership must never
be overwritten by stale retries. No retained per-request history/unbounded dedupe.
RPC maps stale AcquireOnce epoch to existing AcquisitionConflict typed marker.
State machine replay/checkpoint/watch must retain acquired_from exactly.

Regressions first: many independent commands share fewer durable append calls,
per-command answers/epochs and failures, bytes/count/nesting guards, cancellation,
unknown/duplicate AcquireOnce before/after restart, same Client takeover, committed
batch replay and snapshot/purge invariants. Existing tests that assume1log perRPC
must inspect actual applied business state rather than weaken cancellation checks.

## Runtime owner

Own broker/durable.rs and broker/durable/** only in independent worktree. Use
AcquireOnce and retain actor-local pending (generation, expected_epoch) through
unknown mutate/barrier/Fence/Write outcomes. Background recovery retries that SAME
identity until resolved; never increments expected_epoch merely because metadata
watch reports the prior request committed. Expose a private Recover mailbox job,
so choosing pending state/rebuilding remains FIFO inside the actor. A genuine new
foreground generation may CAS a new request; stale delayed attempts cannot replace
newer ownership. If no pending acquisition exists (e.g. uncertain data commit),
start one normal fenced recovery and retain its identity across failures.

After resolved AcquireOnce, perform existing all-live barrier/Fence/W2 write and
recheck authority before installing cache. If a repeated acquisition finds a record
already at its target epoch, preserve physical_revision rather than reset to0;
new epochs may reset. Preserve outbox/receipts/logical generations. Recovered
abandoned/failed admissions activate offline work without reviving a canceled
connection; successful foreground admission is not spuriously activated. Keep
unknown mutation fail-closed, retry backoff and cancellation ownership.

Tests include lost Acquire reply then same-epoch retry, repeated recovery failures
without epoch churn, late old request versus newer takeover, same-epoch partial
W2 recovery, cancellation and eventual forwarding. No unguarded read-only clearing
of poisoned cache; unknown quorum writes still require fencing/repair semantics.

## Quality and integration

Quality owns tests/cluster_control_batching* only in independent worktree, using
real processes for concurrent connections, cross-node QoS/order and Controller
restart with active metadata. Failure before load remains failure; finite deadlines,
cleanup every process. Reuse existing memory/event/fault suites. Coordinator freezes
interfaces, integrates commits, fmt/strictClippy/workspace + fault/process tests,
and repeats the same finite900case; report real results, not desired targets.
Only after local correctness and reproduction pass, build locally cargo zigbuild
amd64musl and repeat the previously authorized first server1000/s→10000cohort with
private network. Stop on first failure; no larger cohorts until it passes.

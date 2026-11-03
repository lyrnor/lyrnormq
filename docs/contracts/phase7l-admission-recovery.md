# Phase7l — admission, recovery and no-change commits

User authorizes fixing admission/recovery, PING commits and control health isolation,
then increasing finite quotas and retesting. This supersedes phase7e fixed32 limits;
all quorum, all-live visibility, epoch, receipt/outbox, WAL and bbolt guarantees stay.
No logical Raft batching in this phase. No durable/wire schema changes.

## Runtime owner
Own broker/durable.rs and broker/durable/** only. Separate queued admission from
execution: at most256 executing and512 globally queued jobs per data node, per-key
channel8 and16MiB payload budget retained. Hold execution permit only while actor
executes; retain a finite global job token while queued/active and cancellation-owned
completion once submitted. Do not create unbounded tasks waiting for admission.
Background Work remains best effort; scheduler single-flight/backoff/fairness retained.
Use typed local recovery-required errors: only missing/poisoned authoritative state
may trigger reacquire; transient admission/timeouts must not blindly start new epoch.
Poison before potentially submitted ownership mutation; successful recovery clears
only after authority/quorum validation. Unknown outcomes fail closed. Align background
worker deadline with actor completion budget (no outer6s/inner8s inversion).
Regression tests before implementation. Public errors remain io::Result.

## Transport owner
Own mqtt-transport only. No-change PING must not commit replicated session state,
increase replicated revision or wake durable forwarding. Keep local Keep Alive,
readiness/ownership checks, housekeeping, Will/expiry and genuine durable mutations.
Explicit effects or safe structural change detection; never blanket skip changes
produced by housekeeping. Include deterministic regression and QoS/expiry coverage.

## Control owner
Own controller.rs, controller/**, rpc.rs, rpc/**, broker.rs only. Raise business write
slots32->256 and applied-barrier slots32->1024 (three data nodes256 each plus margin).
Reserve16 separate bounded slots for Heartbeat/Register; no independent unbounded
wait queues. Retain4s Applied server/5s client, initial/final ReadIndex and all-live
condition. Use bounded health retry; transient heartbeat failure must not fetch full
Metadata. Use existing bounded Owner/Status or add an internal small node-presence
read (coordinator approval first) to prove registration loss before full bootstrap.
Preserve fencing and lease deadlines. Preserve RPC error categories/status details;
no blind retry of potentially committed writes. Document end-to-end deadlines and
safe-vs-unknown outcomes. Add regressions for saturation/health/reply loss.

## Integration and acceptance
Each specialist independent worktree/branch; no overlap. Coordinator docs/build,
full fmt/clippy/tests, fault regressions and local Zig amd64-musl release. Preserve
existing RAM/event/control recovery tests. Deploy fresh isolated three dual-role
processes on47.84.224.186; load47.84.241.200 private network. First gate1000/s to10k,
then hold/PING. Record actual budgets/raw evidence/resources; stop first failure.
No further cohorts or QoS claims if setup fails. No server compilation.

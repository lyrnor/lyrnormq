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

Approved private RPC extension: authenticated ReadRequest::Node{node} returns only
Option<DataNode> under linearizable authority. Heartbeat-error recovery uses this
small read to prove absence; no full-directory read on a transient heartbeat error.
Connection probe optional plateau count1..10 (default10) permits an explicit first
10k-only gate with unchanged arrivals/hold/PING and first-failure stop behavior.

Coordinated quota-chain amendment: Broker command/legacy-forward/delivery admission
also rises32->256, because durable session generation/filters use that command gate.
Both data/control RPC servers use per-connection concurrency1024 instead of32;
otherwise HTTP/2 transport would hide the larger bounded application admission.
Body/frame limits remain. Health reservation protects application admission, not
priority inside Raft or a guarantee against CPU/disk/network stalls.

Submitted Controller writes transfer admission to a bounded owned task before
client_write, retaining the permit through actual completion after response timeout
or cancellation. Never classify post-submission ForwardToLeader as proven safe;
only an explicit check before submission can emit the retryable redirect marker.

Cross-layer acquisition retry clarification: mqtt-cluster-core exports the additive
AcquisitionConflict error marker. Only Command::Acquire rejected with the existing
state-machine 'stale acquisition epoch' maps to it. It proves no ownership mutation;
transport may re-inspect/retry only this marker within existing3-attempt/5s total
claim limit. Generic WouldBlock, overload, unknown replies never trigger another
foreground Acquire. Durable/wire formats and ClusterBridge signatures unchanged.

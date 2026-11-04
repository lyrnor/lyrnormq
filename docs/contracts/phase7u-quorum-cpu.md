# Phase7u — quorum RPC lifecycle and shared wire buffers

Scope: reduce measured CPU/task/copy overhead from Phase7t; preserve RF3/W2,
first-two-successful response selection, same-version conflict checks, generation/
epoch/physical revision fencing, all-live barriers, ReadIndex, FIFO and durable ACK.
No WAL/checkpoint/data schema, protobuf wire, admission, four-batch window, SDK or
existing connection/RPC/caller timeout changes. lib.rs remains thin.

Isolated ownership: runtime owns broker/quorum.rs and broker/quorum/** only;
RPC owns rpc.rs/rpc/**, build.rs and ONLY empty Envelope literal adaptation in
broker.rs/controller.rs; quality owns tests/cluster_quorum_cpu_blackbox.py.
Coordinator owns profile.rs, workspace/lock if needed, docs/harness/integration.
Agents use explicit separate worktrees/branches. Rust tests/builds use the project
volume and coordinator-granted sequential cache access, never system/tmp targets.

Quorum execution:
- Replace three independently spawned peer tasks with one owned coordinator polling
  three peer futures. Keep first TWO successful replies as the same quorum; select
  highest version and compare canonical bodies for identical versions as before.
- Reply immediately once that quorum is determined. Pending third response must not
  change a replied result, including higher versions/conflicts/errors.
- Normally complete already-issued tail RPCs instead of canceling them merely because
  quorum succeeded. Retain existing per-peer connect/request deadlines; tail drain
  has an absolute three-second budget after decision, never reset by progress.
  Overall owned work is capped by the existing eight-second reply budget. Deadline
  cancellation may still cause resets; do not claim reset-free arbitrary networks.
- An accepted coordinator owns execution if its caller cancels; reply loss does not
  roll back submitted writes. All exit paths (success/conflict/insufficient/canceled
  receiver) have finite tail handling. No global quota/unbounded channel/task per peer.
- Reuse original request for local execution. Encode one remote ReplicaMessage body
  per operation and share it across the two remote envelopes. No deep Request clone
  or per-remote JSON encoding. Keep size validation and authenticated RPC boundaries.
- Late old Write/Fence still pass current store generation/frozen/min_epoch/version
  checks; cannot overwrite new state or make an unready replacement count as a vote.

RPC buffers and metadata:
- Generate Envelope.data as reference-counted Bytes. Protobuf bytes field/tag and
  JSON payload bytes remain identical; encode/decode keep existing MAX_WIRE guards.
  Buffer clones share immutable bodies; output protobuf framing may still copy.
- Prevalidate/precompute constant cluster/Bearer metadata once in ClientRpc::new;
  requests clone metadata values; check preserves constant-time token-byte comparison
  and cluster binding. Do not log credentials. No cache TTL or access-policy change.
- Defer unrelated channel-cache redesign. Tiny sampled metadata functions are not
  presented as the principal CPU bottleneck.

Regression first: pending third future survives W2 reply, completes after release,
absolute tail timeout, caller cancellation, errors/insufficient votes, selected-pair
version/conflict/None semantics, late tail independence. Use controlled real loopback
RPC and independent process recovery tests. Assert wire compatibility, shared clones,
invalid/oversize decode and metadata auth behavior. Preserve storage fencing tests.

Measurement: one same 2000/s10k four-probe cohort after integrated tests/fmt/strict
workspace Clippy and local optimized amd64musl zigbuild. Same binary-pinned probe,
limits, servers and profiling. Pin perf clock mono; inspect existing unwinder before
changing build settings. No server compilation. Failed gate stops escalation.
Compare CPU average cores and full-ramp-bracketing raw CPU ticks/CPU seconds per
successful connection, recording each bracket's start/end padding. Do not multiply
partial-window average cores by full-ramp time. Compare h2 reset/GOAWAY, completion
counts, latency/RSS, RPC/queue/stages; shared tail execution may increase replica work.
Capture only fixed numeric diagnostic fields, never bodies/credentials/client IDs.
Archive exact raw evidence, stop processes; no higher or business-message benchmark.

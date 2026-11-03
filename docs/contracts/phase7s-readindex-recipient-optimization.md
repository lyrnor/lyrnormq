# Phase7s — fresh ReadIndex groups and indexed durable recipients

Scope: optimize the measured Phase7r connection/control hot paths, then compare the
same 4x500/s, 4x2500 connection workload on the same two servers. Do not change
admission limits, timeouts, batch size, durable schemas or protocol semantics.
No OpenRaft fork. Keep Controller WAL/checkpoints and data replica RAM guarantees.

Ownership in separate worktrees: control owns controller.rs/controller/**;
runtime owns broker.rs/broker/**; quality owns tests/cluster_readindex* and
tests/cluster_recipient*. Coordinator owns profile.rs, docs/harness and integration.
No root Cargo changes or other modules without coordination. lib.rs remains thin.

ReadIndex contract:
- Group only requests already queued before a NEW ensure_linearizable invocation.
  Requests arriving during that invocation belong to a later group, never reuse
  the current round or a cached prior result. No TTL/leader-lease shortcut.
- Bound each ready-only group and handoff storage; wait asynchronously rather than
  add capacity rejection. Preserve the existing one-second end-to-end authority
  deadline, errors, cancellation safety and shutdown. Do not wait to fill a group.
- Retain OpenRaft applied-index waiting and all existing per-caller term/leader,
  revision, fencing and all-live-node barrier checks. Do not remove barrier calls.
- A canceled caller cannot abort a group used by other callers. After a failed or
  timed-out round the next group starts a fresh check. Worker teardown closes waiters.
- Existing ReadIndex metric remains logical-call elapsed time. New ReadIndexRound
  times actual ensure_linearizable rounds, ReadIndexGroupSize records participants.
- Regression tests precede implementation: sharing queued calls, late arrivals,
  cancellation, failure/timeout, shutdown and term/change correctness.

Recipient contract:
- Replace all-owner scanning with an incremental topic-filter index over client
  durable owners having filters. Reuse mqtt-router matching where suitable, retain
  root-$ exclusion, wildcard semantics, duplicate-filter deduplication and one
  recipient/client at its current logical_generation. Do not introduce shared-group
  scheduling or normalize filters differently from the old scan.
- Index is volatile derived state, never serialized. Snapshot installation rebuilds
  it; sparse deltas touch only changed durable owners (including deletes and logical
  generation changes). Do not rebuild a full index on every connection mutation.
- Publish metadata and its index as one coherent view under existing metadata lock
  discipline. No lock across await. Lookups cannot see a new generation with old
  filters or an index ahead/behind the metadata. Non-client/system owners excluded.
- No subscriptions means no full-directory iteration. Preserve deterministic client
  ordering and bounded metadata/resource semantics; remove stale index entries.
- Regression tests compare an independent old-scan oracle across filters, $SYS,
  snapshots, deltas, deletes, reconnect/generation changes and empty subscriptions.

Acceptance: targeted tests plus strict workspace Clippy/fmt, profile-enabled real
process dual-version QoS and leader failure/session recovery. A fresh optimized
local amd64musl release with the same symbols/profile settings as Phase7r is deployed
without server compilation. Exactly one same-load comparison cohort after local
correctness passes. Preserve raw failed gates; do not increase load or limits after
failure. Compare logical reads versus rounds/RPCs, batch latency/occupancy, queues,
CONNACK latency/counts, CPU and simultaneous RSS. CPU samples use all sampled IPs.
Stop all test processes; archive raw data/hash, report improvements and remaining
bottlenecks without claiming a maximum capacity or untested message throughput.

Post-run measurement correction: the one comparison cohort retained Phase7r's
default perf clock, whose relation to Python CLOCK_MONOTONIC was not recorded.
Per-phase perf attribution is therefore withdrawn for both cohorts; raw captures
remain. Future harness uses --clockid mono. This does not change internal Raft
timers or /proc resource accounting, and does not authorize another pressure run.

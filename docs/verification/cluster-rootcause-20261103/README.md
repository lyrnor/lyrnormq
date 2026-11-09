# Cluster first-failure diagnosis — 2026-11-03

## Conclusion and scope

The quota-free connection failure is reproducible locally with the same production
logic. In original-scheduling staged900, the first measured failure is a
**Controller Acquire reply exceeding its
2s deadline**, followed by Missing/Poisoned cache recovery that issues another
Acquire and fences the previous connection generation. This is not evidence of a
Controller SQLite bottleneck: that runtime has already been removed.

Two concrete control-path problems are demonstrated: individual synchronous WAL
appends limit processing rate, and OpenRaft's long API-processing rounds postpone
completion notifications. Merely draining notifications or shortening rounds does
**not** fix the complete path: ReadIndex requests subsequently exceed1s. All
experimental variants failed; none is a production fix or performance result.

This is a local diagnosis of the deployed source's failure mechanism, consistent
with the server's first2s error. The earlier server cohort lacked stage timing, so
we cannot assign its exact per-stage time or claim every server failure has the
same exclusive cause. No new server pressure/deployment was performed.

## Reproduction and observations

Production diagnostic source34fd5a6 adds timing only, preserving timeouts, retries,
W2, ReadIndex, all-live installation barriers and fsync completion. Independent
runtime/control worktrees supplied7ab0383 and41a9ebe, integrated asc27dbc0/34fd5a6.

macOS27 arm64,10logical CPUs,3cohosted Controller+RAM data processes,4Tokio workers
each, fresh data directories and loopback. Existing CombinedMemoryFleet handles
startup, membership, limits and cleanup. Each case makes900MQTT5 CONNECT attempts,
clean start/expiry0, planned1000/s, at most384Python handshake threads. Actual last
arrival was2.86–3.18s, because waiting threads change the offered schedule. This is
**a finite failure reproduction, not a sustained1000/s benchmark**. Results retain
actual start times; successful-only mean latency has severe selection bias and is
not used to compare capacity. No artificial storage delay or weakened durability.

| Case | Change | Successful /900 |
|---|---|---:|
| staged900 | Normal dependency; phase timing only |4|
| core-a900 | Original Core scheduling + batch timing |3|
| core-b900 | Drain up to64ready notifications after each API |49|
| core-c900 | Return from API round after5ms |0|
| readsplit-c900 | Same5ms policy, extra ReadIndex phase timing |124|
| core-a2-900 | Same binary as previous row; original scheduling restored |0|

Case-to-case variation is substantial. B/C change actual request timing and
commit batch sizes; they are diagnostic interventions, not benchmark optimizations.
The5ms experiment cannot preempt a single blocking-flush await. All processes were
closed; the failed cases are retained rather than rerun until a favorable outcome.

## Evidence for the first trigger

In staged900/node1:

- First sampled Controller Write/Acquire failure at13:30:13.117090Z after2001448us.
- Same-time client TakeMutate errors classify the result as unknown, followed by
  RecoveryMissing. No earlier sampled Acquire barrier or quorum error caused this
  first failure. First events in each error category are always logged.
- Some owned Raft operations still complete after the RPC deadline: a sampled
  Acquire RaftResult took2097698us. Counts from different stages are independent;
  they are **not** request correlation identifiers.
- WAL append index127 completed11.218531Z and index511 completed12.714904Z:
 384additional entries in1.496373s. This interval is not maximum throughput.
- Sampled leader WalAppend times commonly3–4ms, maximum sample9.480ms; sampled
  ApplyState maximum5.100ms, often under1ms. These are power-of-two samples, not
  distributions/p99 or upper bounds. WalAppend includes validation/encoding,
  append and durable-flush callback, not just a raw fsync syscall.

In core-a900/node1 the Core directly records717API messages processed in1318106us,
with an already-ready Notify queue left unconsumed for at least1311035us. Restoring
original scheduling in core-a2-900 records759API messages/1403113us with the same
observed notification wait. Numeric consensus indices are logged; no client IDs,
credentials or MQTT payloads enter diagnostics.

OpenRaft0.9.25 source path:

- core/raft_core.rs runtime_loop starts Balancer(10000), initially1000API messages;
  it runs process_raft_msg before process_notify. The budget can increase to5000.
- Every API invokes run_engine_commands. AppendInputEntries awaits append_to_log,
  which awaits the durable LogFlushed callback before proceeding.
- handle_notify(StateMachine::Apply) invokes handle_apply_result/send_response;
  state-machine execution alone does not resolve client_write's reply.
- Metrics publication also happens at the outer loop. This is a possible further
  wait, but the split experiment below prevents misidentifying it as the main one.

## Why the partial scheduling changes failed

B made early Acquire replies faster but ReadIndex/TakeBarrier failed at1s. C also
failed; shortening rounds changed commit batching (observed2–3entry apply batches
versus24entry samples in A), so synchronous commit-watermark work became more
frequent. The original individual-append cost and API queue remain.

A second timing split gives direct evidence rather than relying on the metrics
hypothesis. In readsplit-c900/node1 the first failed linearizable read has:

```
get_read_log_id: 998917us
total:          1001428us
read target:143; applied at returned read ID:140
```

Only2511us remained for waiting on applied metrics. Many later1s failures never
returned from get_read_log_id at all. Thus **metrics publication is not established
as the dominant cause**. The time is in obtaining ReadIndex, which includes Core
request scheduling/quorum confirmation; these experiments do not separately time
network ACK transport versus processing of its notification.

## Recovery amplification

Durable.take poisons cached authority before a possibly submitted mutation and
only installs the record after Acquire/barrier/Fence/Write/authority all succeed.
This fail-closed behavior is necessary. After an unknown reply, however, the owner
may already be ours while the local record is missing. Background Work detects
RecoveryMissing/Poisoned, then one() performs a fresh Acquire(generation0).

That creates another epoch/control write/barrier/quorum sequence. Metadata install
fences the previous epoch's connection, including a pending CONNECT. Recovery
therefore increases queueing and can turn failures into EOF/0x88. Existing keyed
backoff and single-flight are present; simply adding a generic backoff is not a
complete correction. The Actor existed before its original Acquire was submitted,
and Work is FIFO behind that Take: an 'owner arrives before Actor exists' race is
not the demonstrated cause.

The low aggregate CPU is compatible with this path: serial durable IO/queue waits
do not use all cores. Increasing quotas or changing checkpoint databases does not
remove these waits. Earlier high-rate append-log benchmarks exercise a different
batching/acknowledgement path and cannot be substituted for end-to-end Raft capacity.

## Required fix, not yet implemented

1. Freeze and implement real Controller command batching across ready independent
   requests, including per-command answers/epochs/replay/errors. Amortize WAL sync
   without early acknowledgement. No new reject-on-capacity quota. Putting group
   commit only inside Store.append is ineffective while Core awaits each callback.
2. Coordinate API/Notify/progress scheduling fairness. Avoid a long request batch
   starving consensus completion and linearizable reads. The experimental patch
   is evidence only; it is not an approved dependency fork or a complete fix.
3. Reconcile unknown Acquire outcomes using operation identity/current authority
   before issuing another epoch. Keep fencing and fail-closed ambiguous ownership;
   do not blindly retry or treat timed-out requests as uncommitted.
4. Review coalescing concurrent ReadIndex/Applied barriers without admitting a read
   before its own linearization point or weakening all-live visibility. Measure
   effective throughput after batching, then reconsider deadlines from evidence.

Only after correctness/regression checks should the first server cohort be repeated.
No10k/100k connection or QoS message throughput claim is made here.

## Reproduce and inspect

`reproduce.py /absolute/fresh/run-directory 900 1000` uses `LYRNORMQ_BIN` and the existing
repository fixtures. `raw/*.gz` includes results, complete node logs and final
status only; tokens/data-store contents are excluded. `results.json` records all
case results, environment and immutable binary SHA256 values.

For Core experiments copy the locked registry openraft0.9.25 into a scratch
location, apply experiment-only-openraft.patch, and build in an isolated checkout
with Cargo's command-line `patch.crates-io.openraft.path` pointing there. Default
flags leave scheduling unchanged; `LYRNORMQ_DIAG_INTERLEAVE_NOTIFY=1` selectsB,
`LYRNORMQ_DIAG_TIME_SLICE=1` selectsC (never set both). The recorded initialA/B binary
predates the optional timeslice/read-split instrumentation; the final readsplit-c900/core-a2-900 pair uses
exactly the same binary and differs only in the time-slice environment flag.
Production Cargo.toml/Cargo.lock remain unchanged; no experimental dependency is
integrated. Production release output is restored to the diagnostic-only binary.

Final validation is recorded in validation.json and the attached logs. Source
instrumentation is reviewed to preserve results, typed errors and cancellation;
experimental scheduling is intentionally not shipped.

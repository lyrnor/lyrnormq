# Phase3 local performance and reliability baseline

Contract: [phase3](../contracts/phase3.md). The initial baseline uses an immutable
release executable, raw MQTT5 asyncio clients and a single loopback host. These
are finite closed-loop measurements, not saturation throughput, WAN latency or a
production SLA. Driver and sampling overhead share the host with the broker.

## Reproduction and accounting

Use [driver instructions](../../benches/phase3/README.md). The formal profile uses
256-byte payloads,4096 measured publications plus128 separate warmup publications,
three independent fresh-broker repetitions per case/storage mode, Clean Start1 and
Session Expiry60. Formal producers wait for both PUBACK and all recipient receipts before their
next publication; ACK-only offered-load stress is recorded separately.
Publisher QoS1 and subscriber QoS0 isolate durable routing from
outbound QoS1/2 handshakes; this is not offline-message throughput. SQLite uses the
existing WAL/synchronous FULL implementation. Each run has a fresh temporary DB.

Cases:128 simultaneous connections(64 producers+64 subscribers), one hot exact
topic with32 subscribers/8 producers, and4096 exact filters(64 subscribers x64)
with8 producers. Many-filter4096 publications exercise every installed filter.
Subscriber identities/payloads/counts and every PUBACK are checked. Each ordered
(topic,publisher) stream is checked without requiring a global producer order.
Additional PING barriers reject preceding unexpected frames, not every possible
arbitrarily late duplicate. Missing data or failed assertions abort the campaign.

Timing starts before measured tasks are scheduled and ends when all expected
ACKs and deliveries complete. Ingress publications/s and egress deliveries/s use
the same interval; a fanout copy is never counted as another ingress publication.
End-to-end latency is monotonic publish-write to complete subscriber-frame receipt;
ACK latency is separate. Nearest-rank p50/p95/p99/max uses every sample. Summary
values are medians/ranges of per-run metrics, not pooled percentiles. Individual
latency samples are reduced in memory; per-run counts/distributions are saved.

Broker-only RSS is obtained from ps at idle/setup/premeasure/end and nominally
50ms during work. Actual timestamp spacing includes ps overhead. The reported
sampled maximum includes setup and is not true process/allocator high-water RSS.
CPU is cumulative process time end minus premeasure, a slightly wider window than
the throughput interval. Client RSS is excluded. No leak/soak or CPU utilization
claim follows from these short runs. Results preserve each run and RSS sample.

## Reliability coverage

Independent Transport tests combine explicit worker barriers with full socket
output queues, uncompleted ACK credits, writer termination deadlines and healthy
control progress. Memory+SQLite actor traces cover ReceiveMaximum1, full durable
queues, repeated atomic rejection, takeover/old-generation ACK rejection, exact
DUP replay and renewed admission. Real TCP tests additionally verify nonreading
peers, full-window queues, healthy independent topics, connection-capacity release
and full-window SQLite process-crash recovery.

The overload policy rejects an entire fanout proposal when a recipient queue is
full; it does not promise other recipients on that topic continue independently.
Real TCP tests use exact application-window limits, not a machine-specific kernel
buffer threshold. Deterministic blocked-writer tests complement this observation;
they do not claim the real TCP test saturates the OS socket buffer. RSS samples do
not substitute for bounded state invariants. Existing SQL fault and Fuzz oracles
are reused; process SIGKILL is not physical power-loss certification.

## Review and changes

Protocol, runtime APIs, schema7/container1 and lib.rs structure are unchanged.
Three independent worktrees produced load-driver, transport and TCP/CI changes.
Independent review found fixture/measurement-tool issues and fixed them before
formal runs: EOF precedes eventual connection-permit release; a stale passed file
must be overwritten with running before the first case; ps subprocesses must be
reaped after timeout/cancellation; each publisher/topic needs order validation.
Four driver regressions failed before correction;11 driver tests then passed, later extended to14 tests.
No confirmed production defect was found in the assessed finite scenarios.

See the dated result report and machine-readable campaign in this directory for
actual measurements, environment, commands, source versions and remaining work.


## Static cost review and initial offered-load attempt

The first ACK-only campaign completed three memory/concurrent runs before its
first hot case failed on a non-PUBACK reply. The original driver lacked response
bytes in that assertion; the incomplete JSON and trace are preserved alongside
this report, never labeled a passed campaign. Diagnostic response capture and a
separate completion-gated sustainable profile were frozen before continuing.
Different offered-load policies cannot support a runtime-speedup claim.

Read-only code review identifies profiling candidates, not measured CPU shares:
worker ordinary requests deep-clone the engine; ensure_bounds serializes the live
registry; snapshot adds another bounds check and persistent serialization; ACL
reconciliation also serializes bounds even when unchanged. Each publication rebuilds
the ordinary route index from registered subscriptions. Broadcast watch updates can
induce empty polls, which also clone and validate. Hot fanout32 requires at least
one Handle,32 PollDelivery and32 BeginDelivery requests on the serialized worker,
plus notification-induced work. SQLite commits complete snapshot BLOBs in immediate
transactions with WAL/FULL. Memory mode still pays clone/serialization costs.

QoS0 successful writes do not have a third durable CompleteDelivery transaction;
no such actor API exists. QoS1/2 subscriber ACKs take the Handle path. Next profiling
should measure actor/empty-poll counts, clone/serialize CPU, BLOB sizes and commit
time before changing indexes, notification scope or persistence. Every optimization
must retain proposal/commit/ACK atomicity and expiry/recovery semantics.

Actual results: [2026-10-29 report](phase3-2026-10-29.md), [evidence index](phase3-evidence.json).
The same-configuration ACK-only diagnostic reached600s;the original response remains
unclassified. These failed profiles are excluded from the passed controlled baseline.

Postmeasurement metadata-only followup: Linux CPU model no longer uses architecture
as its label; two failing fixture subcases were reproduced before the fix. Darwin
sysctl and measured workload/timing logic are unchanged. Source a8a2b53 is integrated
as 3f77761; final14 driver tests pass. Formal examples specify600s.

# Phase7m — remove cluster concurrency admission quotas

Explicit user instruction: remove the cluster quotas outright. Supersedes Phase7e/l
fixed active/queued/waiting/aggregate-body permits for cluster transactions and RPC.
Do not replace32/256/512/1024 with larger magic values or unbounded per-job spawns.
No server pressure in this phase; preserve the failed48823b2 cohort as evidence.

Runtime owner: broker/durable.rs and broker/durable/** only, fresh independent WT.
Remove global executing/queued/waiter/body Semaphore gates and750ms admission timer.
Per-client bounded mailbox remains FIFO with async backpressure: full queue waits
instead of rejecting business/Work/Received requests on capacity. Submitted jobs
continue after caller cancellation; pre-send cancellation submits nothing. Keep
actor lifecycle, reply deadline, existing scheduler worker pool/backoff, authority/
quorum/epoch/repair/receipt/outbox semantics, typed safe/unknown error handling.
Physical directory/session/record/packet bounds remain business/protocol limits.
Do not introduce an unbounded channel or unbounded task per waiting job.

Control owner: controller.rs/controller/**,rpc.rs/rpc/**,broker.rs and existing
broker/control_tests.rs only in fresh independent WT. Remove Controller business,
health,Applied,watch permit gates and Broker command/legacy-forward/delivery count/
aggregate-body gates. Remove explicit tonic per-connection concurrency/HTTP2 stream
quotas (max_concurrent_streams(None) when required to disable tonic's default).
Keep normal transport flow control; preserve owned submitted Raft completion after
caller cancel/deadline without permits. Keep read/write deadlines, all-live barrier,
ReadIndex, Node presence, safe retry classification, same-client ordering, W2 ACK.
Repair maintenance/blocking-IO worker pools and finite mailbox buffers remain work
scheduling, not reject-on-admission quotas. Preserve protocol/body/storage sizes.

Regression-first: >old32/256/512 workload must not get capacity rejection; a full
per-client mailbox must suspend and progress after drain, cancellation safe andFIFO.
No test may demand formerly rejected overload be silently dropped. Existing owned
completion, unknown-write-once, lease/repair/recovery/PING tests stay relevant.
Each owner commits tested changes. Coordinator integrates fmt/strictClippy/workspace
and focused actual-process recovery tests, updates architecture/progress/preflight;
no hidden configuration compatibility claim or new MQTT compatibility claim.

Owned submitted Raft completion may retain its existing task-per-submitted-RPC
mechanism; do not add separate tasks merely waiting for capacity. The existing
101024 actor-identity bound follows physical session directory size and remains,
as do replica repair/maintenance worker pools. These are not global concurrent
transaction permits and are not converted to larger admission magic numbers.

Legacy forwarding receipt clarification:8192 is completed receipt-cache retention,
not an in-flight admission quota. Pending deliveries must not be rejected merely
because the map has8192 pending entries; never evict pending receipt identities.
Enforce finite completed retention when deliveries complete, keep duplicate pending
idempotence and cancellation cleanup. Durable per-record receipt/schema bounds stay.

Deployment acceptance followup (explicit user instruction after integration): deploy
source950fa70/Linuxmusl SHAe8f2b38b… to fresh47.84.224.186 run directories and use
47.84.241.200 over private addresses. Repeat first1000/s→10000 cohort with10s hold
and validatedPING; retain identical probe and configuration. Record actual failures,
latencies/resources and stop first failed cohort. Preserve all prior failed data.

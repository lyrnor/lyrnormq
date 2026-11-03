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

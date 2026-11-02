# Phase5e — Client-ordered outbound delivery (frozen 2026-11-02)

**Volatile queue/lifecycle superseded by [Phase5f](phase5f-durable-outbound-queue.md).**

Supersedes phase5d scheduling and ordering only. User requests same-client FIFO,
different-client concurrency, retries block only their client, terminal failure
continues to next message. No durable queue, schema/API shape, MQTT ACK/session/
outbox, SQL semantics, HTTP body policy or credential changes.

## Ordering

Key is (outbound ID, exact source MQTT Client ID), across topics and matching rules
in one live configuration generation. Carry client identity as internal metadata;
never derive it by parsing rendered SQL data. Preserve each client's admitted
observer/dispatcher enqueue order. A client has at most one batch in progress,
including permit waits and retry backoff. Batch events all belong to one client
and retain FIFO order. No later batch for that client starts until predecessor
has succeeded or terminally failed. Retries keep delivery IDs; terminal failure
increments failed and allows next batch. No cross-client/global order guarantee.
Client identity is independent of MQTT TCP connection. Configuration replacement
still cancels old generation; no delivery continuity or durable guarantee across
replacement/restart. Local timeout ends local attempt; remote work may outlive it,
so remote business ordering/idempotency requires receiver cooperation.

## Scheduling/resources

Use exact client lanes, not fixed hash buckets that serialize unrelated clients.
Ready clients must make progress while another client is held/retrying, provided
shared attempt capacity and queue budget remain. Backoff must hold neither global
nor outbound HTTP-attempt permits, including concurrency1. Per-outbound configured
concurrency1..16 now limits active attempts, rather than sleeping retry jobs.
Global16 and inflight_requests semantics stay unchanged. Reacquire attempt permits
for each retry; do not reserve a global permit while waiting for local capacity.

All queues, lane metadata, jobs and timers have explicit finite bounds. Idle lanes
are removed; no permanent registry of every seen client, unbounded per-client
channel multiplication, detached tasks, or busy loops. Enforce aggregate admission
before draining into client queues so pending work cannot escape queue bounds.
An explicit per-outbound resident event cap may replace the old channel-only cap;
use queue_capacity + concurrency * min(batch_size, sink.max_events) + 1 to preserve
the former channel + active batches + carry-over worst-case allowance. Use checked/
saturating bounded arithmetic and validated limits. Every admitted delivery holds
its global byte reservation and resident-slot permit until terminal/cancel/drop.
Metadata and task counts bounded by resident events; client ID bytes are included
in reservation. Overflow rejects new work and records dropped, never evicts a
preceding event to send its successor. Keep one bounded admission channel; collector
and lane scheduling must not block MQTT observer/dispatcher. Fairly rotate ready
clients; one hot client must not monopolize available slots forever.

Batching remains bounded by configured count/wait, adapter count/bytes/overhead/
separators. Batch wait is a maximum gathering delay, not a required delay and not
an HTTP attempt; no cross-client batching. Retain shared adapter Client/pool.
Cancel/replacement/storage error/shutdown must drop queued lane records, cancel/
join all jobs and release counters, byte/resident slots and attempt permits.
lib.rs remains module declarations/docs/reexports only; new modules encouraged.

## Ownership and independent acceptance

Backend owns crates/mqtt-egress/** (and internal egress-core changes only if agreed).
Public API/schema and adapter interface should stay compatible; internal Delivery
constructor and Target admission can change. Coordinator owns docs/root/integration.
UI owner owns dashboard/web/**: update help to client FIFO/different-client parallel,
retry blocks only that client, terminal failure proceeds; no new setting/control.
Quality owns tests/** and .github/workflows/** only.

Regression tests precede fixes. Unit and real MQTT->HTTP acceptance cover same
client sequential arrival across topics; two clients overlap; held/retrying A
blocks A2 but B progresses; backoff releases local capacity with concurrency1;
terminal failure allows A2; stable retry IDs; same-client homogeneous batching and
size/count splitting; multiple matching rules sharing outbound; high client
cardinality cleanup and bounded overload; cancellation/replacement/storage failure
release all resources. Adapt old concurrency tests to use multiple source clients
where parallel requests are intended; do not delete or weaken cap/pool/cancel tests.
Existing concurrency1 global ordering assumptions become only per-client ordering.
Run fmt, strict Clippy, Rust/frontend suites, embedded Release and real HTTP tests.
No unmeasured TPS/p99 claims. Outbound queues remain volatile/best effort.

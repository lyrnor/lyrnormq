# Phase5d — Bounded concurrent outbound requests (frozen 2026-11-02)

**Volatile queue/lifecycle superseded by [Phase5f](phase5f-durable-outbound-queue.md).**

**Scheduling/ordering superseded by [Phase5e](phase5e-client-ordered-outbounds.md).**

Scope: generic outbound batch scheduling and existing Webhook connection-pool
capacity, advanced UI and real concurrency acceptance. MQTT connection concurrency,
SQL shard admission, session/ACK/outbox schemas and volatile queue semantics stay
unchanged. Response-body policy is unchanged (only headers/status are used); this
phase does not add arbitrary body collection or a durable action queue.

## Configuration/API

Add `delivery.concurrency: usize`, default8, valid1..16. Missing fields in old
version2 SQLite/config/API input default8; GET emits the normalized value. Legacy
connector/action SQLite migration initializes8. No schema_version or SQLite
container change; explicit batch/wait/timeouts/retries/queue/IDs/secrets preserved.
New Dashboard factory includes8 with batch1/wait0. Global HTTP-attempt cap remains16
shared by ALL outbounds and ALL generations in one Runtime. Defaulting a previously
missing concurrency field enables parallel requests for existing outbounds too.

Concurrency>1 allows request arrival/completion order to vary, including within a
publisher. SQL evaluation/enqueue order remains client-hashed; MQTT delivery order
is unchanged. Set concurrency1 for sequential outbound batch processing including
retries. Document this in advanced settings without implementation banners.

## Scheduler/backend ownership

Backend owns crates/mqtt-egress/**, crates/mqtt-webhook/** (pool argument only),
crates/mqtt-egress-core/** if required. Keep generic scheduling protocol-independent.
Use ONE bounded input queue and ONE batch collector per outbound, preserving exact
batch-size/byte/separator behavior. Up to concurrency batch jobs may be executing,
waiting for global permits, or in retry backoff. Acquire a job slot before pulling
another batch; at most one oversized carry-over event remains from batching.
Use structured concurrency (e.g. bounded JoinSet), never detached per-message
spawn, unbounded task accumulation or a mutex receiver held across await. Completed
jobs are reaped. Each batch owns its Delivery/Budget records until terminal result
or cancellation, with exactly-once delivered/failed/dropped accounting. The existing
global16MiB reservation covers pending/inflight/retrying event buffers.

Each attempt separately acquires the existing global permit, releases it before
retry backoff, and applies the shared timeout. Retries of one batch are sequential,
reuse exact delivery IDs, and do not stop another job of the same outbound. Error
classification remains adapter-owned. Cancellation, replacement, storage-failure
stop and shutdown cancel/join/abort every child before owner exits; no lingering
network jobs, permits or budget after cleanup. Keep callback admission nonblocking.

One reqwest Client per Webhook sink is shared across its concurrent jobs. Change
factory Endpoint::sink to take concurrency in addition to timeout, and adapter
WebhookSink::new(id,config,timeout_ms,concurrency). Idle pool limit per host matches
concurrency, idle timeout30s remains. Do not create a Client per request; retain
strict rustls, no redirects/proxy, sanitized status errors and old-generation pool
release. Do not expand response-body handling in this phase.

## Observability

Add `inflight_requests` to total Runtime Status and each OutboundStatus. It counts
actual attempts holding global permits while inside sink.send, excludes queued
batches/backoff, and uses a cancellation-safe RAII guard. Counts are sampled; sums
across separate atomic reads need not match during transitions. Global count<=16;
each outbound count<=its configured concurrency. Existing queued/delivered/failed/
retried/dropped remain message counts. Retried increments when a retry attempt
actually starts after permit acquisition, not when entering backoff. Queued is the
existing pending-message reservation count (includes sends and retry backoff), not
just receiver-channel occupancy. No counter changes on MQTT ACK path.
Counters limit local attempts; remote business work may continue after a timeout/
cancellation, so receivers still need delivery_id idempotency.

## UI owner

Dashboard owner owns dashboard/web/**. New factory concurrency8; DeliveryPolicy
and RulesStatus/OutboundStatus expose new fields. Advanced setting uses shared Field/
Input, label `并发请求数`, min1/max16. Basic fields remain name/address/headers only;
queue stays hidden/system-managed. Closed saves preserve concurrency/other policy.
Invalid hidden concurrency opens advanced and focuses invalid input like other
policy errors. Missing legacy frontend fixture field may normalize8, but real new
server GET always includes it. Add `进行中（请求）` to outbound execution table;
explain count units briefly, and advanced note that1 is needed for sequential
sending. Preserve auto-ID/redaction/stopped/polling/mobile/CSP behavior.

## Independent quality owner

Quality owns tests/**. Freeze new input/status shape, fixture defaults8, reject0/17,
preserve omitted/explicit concurrency on SQLite restart. Existing tests requiring
HTTP arrival order explicitly set concurrency1. Do not weaken MQTT requirements.

Required real HTTP tests: same-outbound simultaneous requests reach configured cap
and never exceed it; global16 with multiple outbounds; one blocked/retrying request
does not stall another; single mode sequential; per-attempt retries keep IDs; batch
limits/counts retained; replacement/shutdown with blocked jobs releases budgets and
permits, old sockets close, new generation progresses. Verify common204 keepalive
pool reuse across bursts. MQTT ping/ACK remains live during held HTTP requests.
Use barriers/active counters, finite waits, no performance assertion from sleep
alone. Unit/fake sink tests additionally verify retry backoff releases global slot,
structured cancellation, failure accounting and no buffer leakage.

Report measured peak active requests and functional completion from the local test
receiver; do not claim production TPS/p99 or a speedup without a real benchmark.
Run Fmt/Clippy/tests, frontend tests/build and final Release HTTP/wire acceptance.

# Phase7g — event-driven replicated forwarding

Supersedes Phase7f's background 100ms directory scan, not replication/ACK semantics.
Successful actor-side Acquire/Commit/Host/Release and restored activation schedule
that key AFTER authoritative quorum commit, even if caller drops its reply. One
forwarding worker per key; different keys bounded16 concurrent. Coalesce dirty keys
in a finite scheduler bounded by existing100k directory/actor limit; never silently
lose a committed notification on a full transient channel. Use notification plus
bounded keyed state, not unbounded task creation or timer entries.

Worker drains consecutive intents without sleeping between success. Bound each
worker turn to32 intents then requeue immediately behind already-ready keys, so16
hot keys cannot permanently starve other clients. This preserves single-key work
and FIFO; no polling or intentional success delay. Worker respects FIFO
and target generation, retains receipt idempotence and quorum completion markers.
RPC failure/unknown completion keeps intent and arms a finite per-key retry timer
(backoff); no all-directory periodic retry scan. New events cannot start a second
worker for same key or defeat retry backoff. Avoid self-notification spin on idle
work/empty snapshot commits; poisoned local records still reacquire when required.

Metadata install compares ownership/node liveness/data generation/repair state to
schedule lost-owner recovery, local ownership changes and repair resume. Readiness
recovery must wake retained work even without new publish. Metadata events may
reconcile directory on relevant transitions, never periodically scan every100ms.
Remove obsolete caches on disown. Global shutdown wakes scheduler; bounded close,
no locks across await. No new persistent schema or public wire API.

Runtime owner: crates/mqtt-cluster/src/broker.rs and broker/durable.rs and new
broker/durable/** (lib.rs modules/reexports only); tests first, isolated worktree.
Quality owns tests/cluster_event* and its .github/workflows/ci.yml gate in separate worktree, reuse MemoryFleet.
Coordinator owns contract/docs/integration/local Zig build/server reproduction.
Acceptance: deterministic test fails old polling (idle no scans, immediate event,
coalescing/no lost wake, FIFO drain, retry/ownership/readiness); real-process both
MQTT versions/QoS, offline takeover, duplicate RPC/retry and existing RAM17 retained.
fmt/strictClippy/workspace tests. Same60TCP100/s128B QoS0/1/2 latency reproduction
with fresh RAM cluster after source freeze. Never claim RAM is disk durable.

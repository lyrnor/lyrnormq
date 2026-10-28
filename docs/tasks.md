# Task dependency graph and remaining work

## Current batch: Phase2a
Protocol QoS codec ─┐
Session outgoing ──┼─> Transport live QoS integration ─> independent wire/Paho checks ─> review/merge
Router QoS index ──┘
Memory transaction batch ─> standalone storage tests (independent of network)

Contracts: docs/contracts/phase2a.md. Historical Phase1 contract is retained for
traceability and is superseded only where phase2a explicitly says so.
Three child agents maximum: wave1 protocol/session/router; wave2
transport/quality/storage. Each gets a separate worktree+branch from the frozen
contract commit. Coordinator alone integrates commits and edits root/docs.

## Outstanding after this batch
- Phase2b: versioned durable records and atomic state/outbox transaction contract,
  SQLite WAL backend, crash/torn-write/disk-full recovery, durable ACK ordering.
- Persistent sessions: Clean Session lifecycle, offline bounded queues, reconnect
  retransmission and session-present correctness, generation-safe session takeover.
- Retained storage/delivery/replacement/deletion and Last Will trigger conditions.
- MQTT5 codec/properties/reason codes; Clean Start/session expiry, receive maximum,
  shared subscriptions, topic aliases, limits and AUTH semantics.
- Authentication/ACL extension traits and policy enforcement, tracing/metrics.
- Config file/CLI limits and operational documentation; graceful write/shutdown
  responsiveness under slow readers and explicit overload observability.
- Continuous fuzzing and property tests, client interoperability matrix,
  concurrent/slow-consumer/backpressure/hot-topic workloads; reproducible latency,
  throughput and memory baselines. Remove codec redundant copies, assess lock
  contention only from measurements.
- TLS, WebSocket, container/admin/config reload after single-node correctness.
  Cluster design remains blocked on durable single-node semantics.

These are tracked requirements, not completed features. Performance figures must
come from measured reproducible runs. Full MQTT conformance remains unclaimed.

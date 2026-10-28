# Architecture — revision 1
Phase 1 is a bounded Tokio TCP MQTT 3.1.1 QoS 0 broker. MQTT 5 is explicitly rejected (CONNACK unsupported version), not claimed supported. Persistent sessions and Will are rejected in Phase 1. Retain support may be deferred by disconnecting retained publishes. No authentication is claimed.
Data flow: TCP -> protocol decoder -> transport connection state -> router -> bounded per-client outgoing queue -> protocol encoder -> TCP. Router is synchronous, in-memory, with a short mutex owned by transport; never hold it across await. One connection task multiplexes reads, delivery, shutdown and keepalive. Coordinator binary supplies listener and shutdown.
Dependency DAG: protocol and router and storage can be built in parallel after contract freeze; then transport depends on protocol+router; session is independent Phase 2 groundwork; quality black-box tests depend on runnable binary. Storage and session groundwork is not runtime persistence.
Limits: packet bytes, max connections, outgoing queue capacity, subscriptions per connection, handshake timeout, write timeout. Slow consumers must be disconnected. Duplicate client ID takeover must not let old cleanup remove new state.
Acceptance: fmt/clippy/test pass; actual MQTT client CONNECT/SUBSCRIBE/PUBLISH/PING/DISCONNECT; fragmented/malformed packets, wildcard/system topic semantics and resource limits tested. No invented performance claims.
Standards: https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html and https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
Future: QoS1/2 state machines, durable sessions, retained/Will, MQTT5 properties, auth/ACL, metrics, measured benchmarks, then TLS/WebSocket; clustering deferred.

## Change log
- Revision 1 (2026-10-28): frozen Phase 1 contracts, six isolated specialist
  branches, iterative routing trie, shared `Arc<Packet>` outgoing deliveries and
  short synchronous routing lock. Lock-protected connection-generation checks
  prevent stale connections changing subscriptions after takeover. Packet-size
  limits are checked from advertised length before payload allocation.
- Session/QoS and Storage modules are intentionally separate groundwork: their
  APIs need a coordinated durable transaction/outbox extension before runtime
  QoS2 recovery. Storage assessment selects SQLite WAL for the next design pass.

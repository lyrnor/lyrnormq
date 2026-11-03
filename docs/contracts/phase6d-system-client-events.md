# Phase 6d — subscribable client lifecycle system topics

Frozen initial scope: only `$SYS/events/client/connected` and
`$SYS/events/client/disconnected`. No `schema_version` field, no metrics, retained
presence, historical event API, or cluster work.

These are real MQTT publications and the same publication observer feeds existing
SQL rules and generic durable outbounds. No second rules-only event namespace.
Publication QoS 1, retain false; recipient subscription QoS caps delivery normally.
Metadata client_id is the affected MQTT Client ID for same-client egress ordering.
JSON: event_id, event (`client.connected`/`client.disconnected`), timestamp_ms,
boot_id, sequence, username (null for anonymous), client_id, connection_id,
protocol (`3.1.1`/`5.0`). Disconnected additionally carries reason,
connected_at_ms and duration_ms. Boot ID random per broker runtime; connection ID
boot ID + unique connection generation; sequence monotonic per boot. No secrets.
Node/transport fields are deferred rather than populated with misleading defaults.

Emit connected only after successful CONNACK socket write, not failed CONNECT or
AUTH. Emit disconnected once for each emitted connected on network task cleanup,
including normal DISCONNECT, EOF/errors, keepalive timeout, takeover and shutdown.
A stale connection must never detach a newer generation. Reason vocabulary:
normal, disconnect_with_will, network_closed, network_error, keep_alive_timeout,
session_taken_over, protocol_error, not_authorized, resource_limit, server_shutdown.
Connection takeover events identify distinct generations; do not infer username
presence from one client connection. Wall timestamps use existing broker clock;
duration must be nonnegative. No synthetic offline events on crash recovery.

Lifecycle hooks submit owned bounded metadata with try_send; dedicated broker-owned
bounded consumer serializes and calls existing dispatch, never a session/registry
lock across await. Queue overflow increments a visible counter (and/or existing
structured warning); accepted lifecycle does not wait for Webhook/Kafka/storage.
Use fixed finite queue/byte budget and bound per-event identity sizes by protocol
limits. Best effort admission, duplicates/recovery beyond durable outbound admission
follow existing phase5f/5g; no exactly-once, crash-complete or MQTT ACK coupling claim.
Shutdown closes/drains the event worker with a finite bound before actor teardown.

Client PUBLISH and CONNECT Will to `$SYS` or `$SYS/` descendants are unconditionally
rejected regardless of HTTP/PG/static allow. Reject pre-effects incl retained/storage
and aliases. Existing pending accepted QoS flows remain untouched. System dispatch
bypasses client Publish ACL but recipient subscription admission remains unchanged;
root # does not match $. Explicit system subscriptions use existing configured ACL
(no auth still inherits existing anonymous defaults; do not claim default deny).
Legacy client Will records targeting the reserved namespace remain decodable but
are retired durably before deferred/recovered Will dispatch; no forged lifecycle
event may emerge from such a record. Preserve core Will/QoS/session/outbox schemas. lib.rs only declarations/reexports.

Acceptance: both protocol versions; live MQTT explicit wildcard subscription and
root wildcard isolation; SQL observer/result identity, normal/abnormal close,
keepalive, takeover, shutdown, refused authentication no events, QoS/reconnect
compatibility; forbidden Publish and Will with permissive policy; event JSON no
schema_version/secrets/retention; finite queues and no repeated HTTP checks.

Integration shutdown ordering: after MQTT connections and lifecycle consumer finish,
the executable gives existing rules/outbound work up to 2 seconds to reach idle
(queued_events==0, inflight_requests==0, durable pending_records==0) before calling
Runtime::shutdown. This is a bounded best-effort opportunity, not a remote-delivery
transaction or guarantee at exit. On timeout warn and retain the existing durable
queue's restart semantics; never wait indefinitely for a down destination. No
mqtt-egress public API or queue/storage schema changes are required.

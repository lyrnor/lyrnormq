# Client lifecycle system topics

This phase exposes real MQTT topics, also usable by SQL rules:

| Topic | Meaning |
| --- | --- |
| `$SYS/events/client/connected` | Successful MQTT connection established |
| `$SYS/events/client/disconnected` | An established connection ended |

Subscribe explicitly to `$SYS/events/client/+` (QoS1 recommended). Root `#` does not
match `$` topics. System events have publication QoS1 and retain=false. They are
notifications, not a retained online-device directory or historical query API.

With configured authorization, grant the business account Subscribe permission on
`$SYS/events/client/#`. Existing authorization policy applies; enabling these topics
does not add a default-deny policy to an otherwise anonymous broker. Clients cannot
publish to `$SYS` or `$SYS/` descendants, including CONNECT Will messages, regardless
of a permissive external ACL response. `$SYSfoo` is a different namespace.

## SQL and outbounds

```sql
SELECT payload.event_id AS event_id,
       payload.username AS username,
       payload.client_id AS client_id,
       payload.connection_id AS connection_id,
       payload.timestamp_ms AS timestamp_ms
FROM "$SYS/events/client/connected"
```

```sql
SELECT payload.event_id AS event_id,
       payload.username AS username,
       payload.client_id AS client_id,
       payload.connection_id AS connection_id,
       payload.timestamp_ms AS timestamp_ms,
       payload.reason AS reason
FROM "$SYS/events/client/disconnected"
```

Select an existing Webhook or Kafka outbound. Rules use the same event publication;
there is no separate `$events` namespace. Observer metadata client_id is the affected
client, preserving existing per-client outbound scheduling. System publications
originate at the broker, so MQTT No Local does not hide an affected client's event.

## Identity and delivery

The payload has event_id, event, timestamp_ms, boot_id, sequence, username, client_id,
connection_id and protocol. Disconnected adds reason, connected_at_ms and duration_ms.
There is no schema_version or password/token field. Anonymous username is null.
A successful PLAIN authentication uses the admitted username, not the raw CONNECT
username. Protocol is `3.1.1` or `5.0`.

Use event_id for deduplication and connection_id for connection identity. Never clear
a newer connection just because an older connection of the same Client ID closed.
A username may have multiple Client IDs. A connection going offline does not mean
its persistent session has expired. Network failures are detected on IO failure or
Keep Alive expiry; they are not instantaneous device reachability checks.

Lifecycle admission and existing rule admission are bounded best effort. After an
outbound delivery is accepted by the existing durable queue, its normal persistence
and retry guarantees apply. MQTT QoS1 does not turn lifecycle generation into a
crash-complete transaction. The executable first stops MQTT producers and drains lifecycle notifications, then
gives rules/outbounds up to two seconds to reach idle before normal shutdown. A
destination outage cannot indefinitely delay exit; pending durable records retain
their normal restart semantics. This is not an unconditional delivery-at-exit guarantee.
A killed process cannot generate offline notifications;
reconcile current state using the management connection API after broker outages.
No performance measurement or cluster event ordering is claimed by this phase.

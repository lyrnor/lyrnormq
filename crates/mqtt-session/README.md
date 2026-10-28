# Session primitives (Phase 2 groundwork)

This crate is not connected to the Phase 1 broker. It supplies a bounded outgoing
packet identifier allocator and the MQTT 3.1.1 incoming QoS 2 method A exchange:

| Input | State | Result |
|---|---|---|
| PUBLISH, unused ID | Store message, await PUBREL | PUBREC, no delivery |
| PUBLISH, awaiting ID | Keep original message | PUBREC, no delivery |
| PUBREL, awaiting ID | Remove message, record bounded completion | PUBCOMP, initiate delivery once |
| PUBREL, unknown/completed ID | Unchanged | PUBCOMP, no delivery |
| PUBLISH, completed ID | Start new exchange regardless of DUP | PUBREC, no delivery |

DUP is not a business-message deduplication key. Completed history can be evicted
without affecting repeated PUBREL handling; unknown PUBREL produces no delivery.
Incoming identifiers are independent of the outgoing allocator. Caller validates
packets, maintains ordering, sends ACKs, and closes on rejected resource limits.

An eventual persistent runtime must durably accept a PUBLISH before PUBREC. It
must atomically commit the PUBREL state transition together with onward-delivery
intent before PUBCOMP. Returning a Vec here is an in-memory action, not a durable
outbox or application exactly-once guarantee. Caller must process the action
before accepting another PUBLISH for the same ID. No session expiry, offline
queue, reconnect, outgoing QoS exchange, storage recovery or Will is implemented.

Reference: [OASIS MQTT 3.1.1 section 4.3.3](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html).

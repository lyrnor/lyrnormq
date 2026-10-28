# MQTT 3.1.1 QoS primitives and durable sessions

The Phase 2a transport uses this crate for live clean-session QoS1/QoS2 exchanges.
Transport now delegates incoming QoS2 ownership and outgoing QoS1/QoS2 ACK
transitions to these bounded state machines; it keeps each connection’s incoming
and outgoing identifier namespaces independent. The existing packet identifier
allocator and MQTT 3.1.1 incoming QoS 2 method A exchange remain unchanged:

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

The basic incoming state machine performs no IO. The Phase 2b transport worker
uses `durable::SessionEngine` and SQLite to commit an accepted PUBLISH before
PUBREC, and to atomically commit the PUBREL transition with recipient outboxes
before PUBCOMP. Returning a Vec from the primitive is still an in-memory action;
callers using it directly must arrange those commits and process the action
before accepting another PUBLISH for the same ID. Persistent subscriptions,
offline QoS1/2 queues and reconnect replay are implemented by the durable model
and transaction worker. MQTT5 session expiry and Will remain unsupported.

Reference: [OASIS MQTT 3.1.1 section 4.3.3](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html).


## Outgoing exchange API

`OutgoingQos::start(qos, encoded_message)` accepts only QoS1/2, checks encoded
message size and allocates an unused outgoing Packet ID from a bounded window.
The caller puts that ID into the outgoing PUBLISH and writes it before allowing
its acknowledgement to advance the state. Incoming and outgoing identifiers are
independent namespaces.

| State | ACK | Action / next state |
| --- | --- | --- |
| Await PUBACK (QoS1) | PUBACK | Complete, release message and ID |
| Await PUBREC (QoS2) | PUBREC | Drop encoded message, send PUBREL, await PUBCOMP |
| Await PUBCOMP (QoS2) | repeated PUBREC | Send PUBREL again, retain ID |
| Await PUBCOMP (QoS2) | PUBCOMP | Complete, release ID |
| Unknown ID | any ACK | Ignore |
| Existing ID, any other ACK kind | mismatched ACK | UnexpectedAck, preserve state |

No acknowledgement mutation performs IO. `is_full()` lets transport stop taking
new deliveries while continuing to read ACKs. Invalid QoS, oversize messages and
full-window failures leave accepted exchanges and ID allocation unchanged. The
window caps at 65,535 entries; a zero window accepts no exchanges. Memory for
stored encoded messages is bounded by window times maximum encoded size, plus
bounded metadata and the fixed identifier bitmap. QoS2 discards stored PUBLISH
bytes upon PUBREC, so repeated PUBREC cannot trigger PUBLISH redelivery.

The outgoing primitive alone supplies volatile state and no IO or retry timer
within the current connection. The Phase 2b model snapshots its stages and IDs,
then restores and replays them on persistent reconnect (MQTT3.1.1 section 4.4).
The transport worker commits the versioned state/outbox transaction before
sending its packets. Clean-session state remains volatile. Neither these state
machines nor the worker establish application-side exactly-once processing or
physical power-loss validation.

## Phase 2b durable model

`durable::SessionEngine` reuses the same incoming/outgoing QoS primitives and
allocator. It models persistent subscriptions, offline QoS1/2 queues, restored
inflight IDs, ordered DUP PUBLISH replay and PUBREL-only replay after PUBREC.
The schema1 JSON envelope contains only persistent records; online flags and
replay cursors are runtime-only and all restored sessions start offline. The
SQLite-enabled transport integrates this model for durable session recovery;
the model itself remains a pure, IO-free component.

Every mutation is a proposal: clone the engine, apply an event, validate and
serialize with snapshot, commit through the storage worker, then install and
send the resulting packets. Discard the complete proposal on **any** error.
This is required because a failure can follow partial mutation inside a proposal.
In particular, PUBREL source removal and all recipient outboxes must share one
external SQLite transaction. The model performs no IO and cannot itself promise
durable application processing. Clean sessions are never part of durable state.

Max sessions/subscriptions/queued/inflight/full packet bytes constrain records.
A bounded JSON writer enforces max_state_bytes against the full live registry
(including clean sessions), then emits its persistent subset. Metadata allocation
is additionally bounded by record counts; the packet allocator has fixed 65,536
bytes per session. Snapshot staging and engine cloning retain additional bounded
copies. Whole-record JSON and rebuilding the existing trie for each routed
proposal are intentionally simple and expensive; no performance claim is made.

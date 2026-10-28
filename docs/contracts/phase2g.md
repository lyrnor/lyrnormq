# Phase2g frozen packet-size negotiation and assigned identifiers

Normative: OASIS MQTT5 OS3.1.2.11.4,3.1.3.1,3.2.2.3.6/7; MQTT-3.1.2-24/25
and3.2.2-16. https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
Supersede only previous explicit refusal of client MaximumPacketSize and empty V5
ClientID. Reuse schema5/SQLite/Clock/QoS/expiry/outbox, no competing session store.
lib.rs remains documentation, module declarations and reexports only.

## Ownership and dependencies
Session owns mqtt-session/** in isolated agent/p2g-session worktree. Transport
owns mqtt-transport/** in agent/p2g-transport. Quality owns tests/** and workflows
in agent/p2g-quality. Coordinator owns docs, root Cargo.lock and integration. All
start at frozen contract; no shared checkout writes. Transport implements against
specified model API, cherry-picks completed Session dependency before final tests.
Codec already supports both properties and empty V5 identifiers: reuse it. A later
independent protocol review can add focused codec tests in its own worktree.

## Connection-only maximum
Add ConnectionOptions.maximum_packet_size:u32. It is the peer's advertised inbound
maximum, defaults u32::MAX on V5 absence and V311; zero is a protocol error. Session
holds runtime-only corresponding maximum, resets on every CONNECT/restore, never
serialized in schema5. Existing server configured max_packet_size remains an inbound
and allocation/output resource limit; effective output bound is min(server,peer).
No schema migration or persistence of a negotiated connection limit.

Size means complete encoded frame (fixed header, Remaining Length and properties
length integers included). Never truncate topics/payload/application properties.
Peer limit MUST NOT cause publication rejection for other recipients, nor refusal
of resume just because old data exceeds its new limit. Preserve existing server
resource-cap/version-growth admission policy separately. At Poll, skip oversized
queued/replay PUBLISH and atomically retire all matching QoS IDs/order/properties/
expiry/tickets/sent quota, even previously-started PUBLISH: MQTT3.1.2-25 explicitly
requires discard-as-completed. Released AwaitComp is control, never converted back
to PUBLISH. Skip must report changed and commit before any wire/ACK/next delivery.
Bound work by existing finite queues/inflight; following fitting messages progress.
Check again at Begin using final remaining-expiry properties: rejected/stale output
never writes bytes or leaks quota. Existing TTL semantics remain otherwise intact.
Global retained state and other recipient copies survive a peer-specific discard;
retained replay subscription succeeds even if every copy is too large for that peer.
Offline enqueue is not constrained by an old connection's peer maximum.

## Wire/handshake and controls
Transport parses positive CONNECT maximum before worker request and applies it to
EVERY outbound frame: accepted/refused CONNACK, control replies, data, takeover,
protocol errors, timeout/shutdown terminal. Final writer encoded-length check is
mandatory. A control frame that cannot fit closes without writing any oversized or
partial bytes. Compact standard ACK/DISCONNECT encodings may be used if semantically
identical; never erase mandatory reason arrays or assignment.

Successful CONNACK retains nondefault required capability advertisements (bounded
server MaximumPacketSize/ReceiveMaximum, SharedSubscriptionAvailable0 and
SubscriptionIdentifierAvailable0). TopicAliasMaximum0 may be omitted since default0.
No silent omission of unavailable capabilities to fit a tiny peer maximum. Build
and size-check exact success CONNACK BEFORE mutating session/takeover/Will state.
If it cannot fit, refuse with fitting negative CONNACK reason0x95 (minimal5 bytes),
or close silently if even that cannot fit; prior sessions/Wills unchanged.
Rejected authentication/authorization and malformed CONNECT replies must also obey
known peer maximum; invalid/malformed properties cannot be trusted beyond parsing.
No acceptance followed by inability to send the mandatory assignment/capabilities.

## Assigned Client Identifier
Accept zero-length MQTT5 ClientID with either CleanStart value. In serialized worker,
generate a new bounded UTF8/alphanumeric key (implementation target23 characters,
OS entropy via cached getrandom0.2, bounded16 collision attempts). Session adds public
client_id_in_use(&self,id:&str)->bool covering BOTH sessions and global Will intents;
allocation checks before any mutating transition, never hijacks a guessed existing ID.
Entropy/collision exhaustion fails closed without state or takeover side effects.
Use resolved ID for authentication ConnectInfo, Connect ACL, Will ACL/ownership,
registration, cleanup, messages and durable records. Generation is not authorization.
A successful empty-ID CONNECT returns AssignedClientIdentifier exactly once with
that same key. Explicit nonempty ClientID returns no assignment. No assignment in
refusal. Worker reply carries resolved key/assignment to network; network must not
keep its original empty client string. Existing private legacy test reply may remain.

Commit persistent session/Will/outbox before success CONNACK. Restart needs no ID
allocator metadata: stored IDs and pending Wills are collision checked. Client can
resume assigned ID explicitly on5 or311 under normal ownership/expiry policies;
no repeated assignment property on explicit reconnect. Assigned-ID persistent state
supports existing offline delivery, QoS1/2 retransmit, takeover and expiry/recovery.
Do not reject an otherwise valid original CONNECT merely because substituting its
assigned ID would enlarge an internally synthesized validation packet; actual wire
input is already checked before normalization. MQTT311 empty-ID behavior unchanged.

## Acceptance
Unit and independent wire/Paho tests in memory/SQLite: zero/duplicate/context-invalid
maximum, tiny limits1..mandatory CONNACK size, exact-boundary/boundary+1, Remaining
Length/property-length width changes, control SUBACK/UNSUBACK/error/takeover limits,
large→small→fitting ordered queue, QoS0/1/2 discard and quota/ID cleanup, retained
preservation/replay, TTL coexistence, smaller-limit resume of started/unstarted and
PUBREL-only state, V311↔V5 bridging. Assigned both clean values/positive expiry,
unique concurrent IDs, explicit-ID property absence, generated-key ACL/ownership,
no-state on authentication/CONNACK-fit failure, SIGKILL offline/inflight recovery,
Will/clean reset/takeover, restart collision handling. Run workspace fmt/Clippy/test/
rustdoc, old feature regressions and actual clients. Record actual counts and limits;
no full MQTT5 compatibility or unmeasured performance claims.

## Protocol diagnostic prefix helper (additive freeze)
Protocol specialist owns mqtt-protocol/** in separate agent/p2g-protocol worktree.
Add public peek_connect_maximum_packet_size(input:&[u8])->Option<u32>, reexport
from thin lib.rs. Reuse existing Reader/property parser, no duplicate MQTT decoder.
Inspect canonical CONNECT header and complete V5 prefix/property block within its
declared Remaining Length even if later payload is incomplete/malformed. Return
Some(positive maximum), Some(u32::MAX) if valid properties omit it; None for invalid
or incomplete prefix/properties, wrong version or other packet type. Never consume
another coalesced packet to complete this packet's declared property area. This
helper does not accept a CONNECT or change decode error classification. Network
uses a known prefix maximum for pre-handshake diagnostic bounds; malformed properties
remain untrusted and existing typed decode-error behavior is retained when no
trustworthy maximum is available. Test incomplete prefixes, boundary isolation,
invalid/duplicate/zero properties and malformed later payload plus tiny maximum.

## Admission clarification from independent bounded-queue reproduction
Poll/Begin safety alone is insufficient: an online peer's guaranteed-oversize copies
must not fill its queue and reject the source/healthy peers before it polls. Skip
known oversized candidates before queue-capacity admission; retire preexisting
oversized online backlog before admitting fitting traffic or retained replay. Apply
the same rule to each retained copy before retained replay queue limits. Offline
queues ignore old peer maxima. Preserve global configured resource/version-growth
preflight and genuine fitting-message backpressure. All pruning/skips belong to the
existing proposal transaction, retain global retained records and report real Poll
changes as before. Regression must exercise max_queued=1 without polling small peer,
healthy peer progress, fitting follow-up after reconnect backlog and multiple large
retained matches; do not merely increase queues or weaken capacity requirements.

# Phase2i frozen connection-local Topic Alias contract

OASIS MQTT5 OS3.1.2.11.5,3.2.2.3.8,3.3.2.3.4:
https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
Reuse existing codec (already accepts empty topic with alias), session/QoS/outbox,
schema6, Clock/TTL, NL/RAP/RH/IDs, ACL and full-frame writer. lib.rs stays thin.

## Isolation and API freeze
Transport owns mqtt-transport/** in agent/p2i-transport isolated worktree. Protocol
specialist owns mqtt-protocol/** and read-only session trust review in independent
agent/p2i-protocol. Quality owns tests/** and CI in agent/p2i-quality. Coordinator
owns bin/lyrnormq/**, docs, root lock and integration. No shared source writes.
Add BrokerConfig.topic_alias_maximum:u16 default32, zero valid. It bounds advertised
inbound aliases and the local maximum outbound map entries. CLI coordinator adds
--topic-alias-maximum0..65535. Client CONNECT TopicAliasMaximum (default0) separately
limits outbound IDs; actual outbound budget=min(clientMaximum,localMaximum).
No ConnectionOptions/session public API or durable schema change is necessary.

## Canonical boundary and inbound aliases
Maps are per NETWORK CONNECTION, separate inbound/outbound, runtime-only. Resolve
incoming V5 PUBLISH after bounded decode and before worker/ACL/session handling.
Nonempty topic+alias binds/replaces; empty topic+known alias resolves; no alias plus
nonempty topic leaves maps alone. TopicAlias0, above advertised maximum or unbound
empty reference closes with94, fitting existing peer output maximum. Codec handles
duplicate singleton82 and empty-without-alias82; reuse parser/reasons. Do not decode
Topic Alias in Will/other packets where context is forbidden.
Strip TopicAlias from normalized properties and pass full canonical topic to worker.
Preserve all other properties/order, original receipt timestamp, flags and PacketID.
ACL, NoLocal, retained, subscription matches and QoS2 storage see resolved topic.
A duplicate QoS2 packet can update a wire alias binding, but must not replace the
original stored QoS2 payload/topic/properties/deadline; PUBREL uses original value.

Check actual original wire packet against advertised server max in decoder, before
expansion. Expanded canonical frame remains subject to existing configured resource
cap; if expansion exceeds it, fail QUOTA_EXCEEDED97, not a false wire-size95. Do not
bypass bounded memory/canonical storage simply because a compressed wire fits.
Topic strings are UTF8 and at most65535 encoded bytes; map count bounded, sparse
large alias IDs must not trigger unbounded/vector allocation. Limits0 disable maps.

## Outbound aliases and write ordering
Extract peer TopicAliasMaximum before connection data can write; defaults0. Each
writer independently chooses bounded aliases1..budget with deterministic replacement
(e.g.round-robin). First binding uses full topic+alias; reuse may use empty topic+
alias; reassignment always full topic+alias. Never use a stale/unsent binding.
Prepare transformation only after Delivery Begin committed, deadline refresh and
terminal-priority check, in actual writer order. Record binding/replacement ONLY
AFTER successful complete frame write. Failed/partial writes terminate connection,
no subsequent frame. No alias state in worker/outbox or queued preencoded frames.

Encode exact candidate with all property/RemainingLength widths and enforce
min(serverResourceCap,peerMaximumPacketSize). If adding alias to a binding would
exceed limit, send fitting full canonical frame without alias and DO NOT install
mapping. Reuse an existing alias only if it fits and reduces size; preserve all
non-alias application/subscription properties and their ordering. Other controls
have no TopicAlias. Final complete-frame guard remains mandatory for every path.

Existing SessionEngine canonical peer-size admission/discard remains unchanged:
aliases are optional wire compression, NOT a way to salvage a canonical recipient
copy previously dropped by resource/peer limits. Document this policy explicitly.
No payload/property truncation or retroactive durable compression. Retransmit/replay
always begins from canonical state, with full-topic binding on a NEW connection;
never restore a prior connection's map, even persistent SessionPresent1/takeover.
V311 wire cannot contain aliases and routing across versions carries canonical topic.

## Handshake, persistence and failure
Successful CONNACK advertises exact configured TopicAliasMaximum, including0.
All other mandatory capability/assignment properties preserved. This adds3 bytes
(default success20,assigned46); update exact preflight and tests BEFORE session/
Will/takeover mutation. Too-small success still negative95 if5-byte refusal fits,
otherwise EOF. Do not omit a nonzero maximum merely to fit and then accept aliases.
Malformed CONNECT diagnostics respect known peer packet max as before.

Every connect/disconnect/reconnect/takeover/shutdown/crash drops both maps. Durable
snapshots stay schema6 with full topics and NO TopicAlias in incoming, retained,
Will, queued or outgoing properties. Existing session validator rejection of alias
is defense against noncanonical worker input/corrupt durable records, not unsupported
wire behavior. No aliases/limits serialized; no migration. Stored replay marker,
identifiers, TTL, QoS stages/IDs, receive credit and source ownership unchanged.

## Acceptance
Failure-first unit/blackbox/Paho BOTHmemorySQLite: bind/reuse/rebind, maximum0/1/
32/65535 and range/zero/unbound/duplicate/wrong context, empty-without-alias, separate
map directions/clients, sparse IDs/bounded replacement, output first-bind before
references, cap exact/one-over/varint width+fallback/no phantom binding, NL/RAP/RH/
SubIDs+TTL+ACL on resolved topics, incomingQ2 alias rebind beforePUBREL (including
restart), outgoingQ1/Q2 retry and PUBREL-only, retained replay/full canonical snapshots,
SessionPresent1/reset/takeover/crash map reset, V311↔5 routing. Reuse old suites and
strictfmt/Clippy/test/rustdoc. Record actual counts, commits and remaining scope;
no full conformance/performance claims.

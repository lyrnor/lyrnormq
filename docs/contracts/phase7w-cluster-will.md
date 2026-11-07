# Phase7w — durable-cluster Will

Supersedes Phase7a/7b/7f refusal of **non-retain** Wills on durable (Phase7f RAM
W2) replicas only. Legacy non-durable cluster still refuses Will and persistent
sessions. Will-Retain, retained PUBLISH, and shared subscriptions remain refused
until their own gates. Do not claim full cluster MQTT compatibility.

Standalone Will, delay, takeover and graceful-cancel semantics are unchanged.

## Frozen capability

Durable cluster admits CONNECT with a Will when `retain=false`. MQTT 5 CONNACK
has no Will-available property; success is CONNACK 0x00. Will-Retain CONNECT is
refused: MQTT 5 CONNACK `0x9A` Retain not supported, MQTT 3.1.1 CONNACK `3`.
Legacy (non-durable) cluster CONNECT with any Will remains MQTT 5 `0x83` /
MQTT 3.1.1 `3`.

A Will is one ordinary publication: persist the SessionEngine snapshot (active
or pending record) in the existing durable session mutation, then fire at most
once through the existing ordinary `ClusterMessage` path. No Controller body
storage, no new replica record type, no Will-Retain, no shared-subscription
delivery.

Delay, MQTT 5 DISCONNECT `0x00` cancel, DISCONNECT `0x04` fire, delay `0`
immediate fire, session-expiry cap, same-session resume cancel, and clean-start
session-end force reuse `mqtt-session` SessionEngine. Transport must not invent
a second scheduler.

## Failover

CONNECT acquisition restores the snapshot then applies `connect_with_options`;
do not `recover_wills` before that apply, or a resume would publish a crash
Will the session engine is required to cancel.

Replica `activate_session` without a CONNECT is a new owner of a disconnected
or crashed session. That path MUST `recover_wills` (anchor active-at-crash
intents once, keep known `due_at`) and commit recovered pending state plus any
due ordinary publications before serving. Lost lease / invalid generation must
not fire. A later CONNECT on the new owner still uses SessionEngine takeover
rules.

Whole-group RAM loss cannot recover pending Wills from control metadata. A
crashed replica is a new identity.

## Ownership

Coordinator owns this contract and docs pointers. Transport owns CONNECT
admission and replica-activation recovery in `mqtt-transport`. Session engine
and durable frames stay in `mqtt-session`. Cluster runtime keeps rejecting
`retain` and non-ordinary `ForwardScope`. Quality owns wire tests. Retained and
shared remain follow-up gates.

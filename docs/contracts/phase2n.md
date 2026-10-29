# Phase2n — RAP, PFI and explicit protocol profiles

Authority remains pinned MQTT5 OS body; MQTT311 OS is also required for the dual
listener. Coordinator owns contracts/CLI/docs/matrix/integration. Isolated RAP
specialist owns mqtt-session; PFI specialist owns mqtt-protocol; version specialist
owns mqtt-transport; released slot starts independent Quality in its own worktree.
All prior contracts apply except explicit amendments below. Keep schema7, container1,
public import paths and thin lib.rs; no production change precedes this freeze.

## N1: conservative mixed-RAP forwarding policy

Supersede any-eligible-RAP ordinary collapse only for live source RETAIN1 delivered
to an MQTT5 recipient with BOTH eligible ordinary RAP0 and RAP1 matches. Emit one
copy PER eligible matching subscription; each copy uses min(sourceQoS,that grant),
sourceRETAIN && that RAP, and only that entry's identifier if present. This selects
the multiple-copy behavior permitted by3.3.4-2 while independently satisfying
3.3.1-12/13 and3.3.4-3/5. At least one planned copy respects the maximum eligible
QoS. Do not claim OASIS uniquely mandates this splitting algorithm; it avoids
relying on ambiguous one-copy mixed flags or grouped multi-ID copies.

No Local and current Subscribe/Receive/Connect ACL eligibility apply BEFORE deciding
mixedness. Homogeneous RAP, source RETAIN0 and recipient311 retain existing one-copy
max-QoS/all-eligible-IDs policy (311 strips wire IDs and live RETAIN). Retained replay
always remains RETAIN1 with its triggering entry; shared selections remain separate.
Ordinary output is bounded by maxSubscriptions and admitted as one atomic batch.
Count EVERY planned ordinary copy plus shared copies against recipient queue caps;
any genuine capacity failure rolls back all outboxes/cursors/retained mutation under
the existing proposal contract. Existing peer-cap discard/global-cap rules apply per
copy. Respect inflight/ReceiveMaximum, expiry, QoS2 idempotence and canonical aliases.

No schema/public API change: new copies capture metadata in existing queued/outgoing
records; accepted pre-upgrade old copies keep their captured retain/IDs and are not
retagged. Add failure-first mixed-copy test; cover repeated IDs, sourceQoS/grant
mixes, NL/ACL eligibility, offline/restart/311 bridging, repeated PUBREL and capacity.
Old tests asserting any-RAP collapse must be updated to the explicit new policy.

## N2: PFI role boundary

PFI1 is the producer's payload-format assertion. Server forwarding preserves the
property and bytes (3.3.2-4); receiver verification is MAY, not an implicit mandatory
broker validation step. Client supplies CONNECT Will as Binary Data (3.1.3.4), and
3.1.3.2.3 explicitly makes Server Will validation optional. Section5.4.9 describes a
Server not validating when a Client does. Current no-validation forward policy is
therefore retained; it does not certify malformed producer data as valid UTF8.
Payload UTF8 rules are RFC3629, not MQTT UTF8-String NUL/noncharacter restrictions.
No new broker-origin PFI1 payload synthesis is introduced. Preserve live/retained/
Will/QoS/recovery metadata. Record scoped evidence and applicability, not a waiver
of a producer MUST or a blanket assertion that all PFI1 bytes are well formed.

## N3: explicit listener protocol profiles

Add public mqtt_transport::ProtocolMode::{Both,V311,V5} in config.rs, re-export via
lib.rs. Add BrokerConfig.protocol_mode default Both. Runtime-only, never persisted.
Coordinator CLI adds --protocol both|3.1.1|5. Both preserves accepted levels4/5 and
legacy unsupported-level CONNACK1. Explicit V311 accepts4 and rejects other levels
with legacy CONNACK1. Explicit V5 accepts5 and rejects other levels with minimal
MQTT5 CONNACK84. Wrong protocol names still close as malformed. Tests must include
known wrong levels and unknown0/3/6/255, never infer a future payload grammar.

Gate known wrong-version frames before worker Connect/authentication/assignment or
session/Will/takeover effects. Header-error unsupported-version rejection selects
the configured profile; unknown properties/peer caps cannot be guessed from an
unsupported payload. Known decoded peer caps still constrain replies; local packet
caps always apply, otherwise close without partial output. Only one refusal and EOF.
Existing valid-version malformed packet handling/credentials and Both behavior stay.
Cross-profile restart of a durable session uses existing versioned resume semantics.

Matrix3.1.2-2 can be verified for explicit V5 profile; Both's pre-negotiation fallback
remains a documented compatibility boundary, not universal MQTT5 proof. Do not claim
arbitrary unknown bytes reveal intended version or waive either protocol's MUST.

## Evidence scope

Broad U4.7.3 describes the routing model and optional dynamic topic/ACL choices;
use inspected implementation and independently bounded matcher/ACL tests, rather
than demanding an impossible universal proof of every program input. This does not
replace other wildcard/order/security obligations. Overall U7 remains an acceptance
gate until comprehensive applicable MUST evidence is established. No automatic full
conformance claim follows from resolving the six specific boundary rows.

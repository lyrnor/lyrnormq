# Phase2k — normative conformance audit and corrections

Authority: OASIS MQTT5 OS (2019-03-07), numbered body requirements and unnumbered
normative constraints. AppendixB is a non-normative index, not a replacement for
body text. Prior phase contracts apply except corrections explicitly frozen here.
Do not equate feature presence, parser acceptance or a nearby test with conformance.

## Evidence and ownership

Coordinator owns matrix/provenance/status definitions/docs, CLI and integration.
Isolated Protocol, Session and Transport writers own only their crate paths.
Independent Quality owns tests/**/CI in a separate worktree after a slot releases.
Auditors submit reviewed exact IDs, symbols/cases, actual commands/results and
limitations. Unreviewed and missing evidence remain explicit; no guessed green rows.
No durable/public API change without a new frozen subsection. Keep schema7, existing
clock/outbox/persistence model and thin lib.rs structure. Tests precede bug fixes.

## K1: server response before successful CONNACK

MQTT-3.14.0-1 explicitly forbids server DISCONNECT before an accepted CONNACK;
MQTT-3.2.0-1 constrains ordering and MQTT-3.2.0-2 permits at most one CONNACK.
Supersede Phase2j initial-auth error/timeout terminal packet expectations: while
initial enhanced AUTH is pending, reject/timeout using a non-success CONNACK with
appropriate legal reason (82 for protocol sequence, 87 for authentication deadline),
SessionPresent0, or close silently when no legal/fitting response is possible.
Never send server DISCONNECT before successful CONNACK. Authentication failure
CONNACK may omit method. Established reauth failure stays DISCONNECT87, protocol
error82 and real KeepAlive timeout8D. Provider deadline/cap/state/Will/takeover
isolation and basic authentication behavior otherwise remain unchanged. Exact cap
checks and one terminal response still apply; add actual TCP regression before fix.

## K2: ordered multi-filter UNSUBSCRIBE

MQTT-3.10.4-6 requires processing multiple filters as successive requests with one
UNSUBACK. MQTT-3.11.3-1/2 constrain result order and per-filter reason values.
Apply each filter removal to current state before computing the next result;
[existing, same-existing, missing] yields [00,11,11] in hex, not [00,00,11].
Retain existing exact-key semantics, shared membership pruning, started QoS
obligations, packet identifier, one response and transactional proposal boundary.
No public API or schema change. Include ordinary/shared duplicate filters and
existing invalid-filter validation behavior in regression coverage.

## K3: AUTH method presence at the codec boundary

MQTT-4.12.0-5 and unnumbered section3.15.2.2.2 require the method on AUTH.
The codec can enforce presence without knowing connection state; Transport still
checks equality to the negotiated value. Reject AUTH without AuthenticationMethod
on encode and decode with Protocol Error82, including RemainingLength0 and data-only
forms. Keep valid method-only success/continue/reauth encodings accepted and leave
CONNECT/CONNACK failure-context rules unchanged. No API/schema change. Update only
invalid generic codec fixtures; do not weaken runtime negative-path expectations.

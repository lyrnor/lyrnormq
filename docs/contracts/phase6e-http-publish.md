# Phase6e — privileged HTTP downlink publish

This contract supersedes the canceled Basic/auth-cache design. User explicitly
requires the same Bearer token as Dashboard, bypassing MQTT authentication and ACL.
No Basic parsing/cache, new mqtt-auth API or HTTP publisher client_id context.

POST /api/v1/mqtt/publish on existing admin HTTP listener. HTTP supported directly;
HTTPS optional via reverse proxy. Same exact Bearer validation as every management
API: one Authorization: Bearer <admin-token> header, invalid/missing401 plus Bearer
challenge. Token grants privileged server-origin publishing, no source HTTP/PostgreSQL/local
MQTT auth or ACL call and no new receive-time backend check. Existing recipient delivery
semantics and subscription ACL remain unchanged; no new auth backend or token store.
All other management routes retain existing protection. Existing loopback listener
CLI/token-file pairing stays unchanged; remote use through an HTTP/HTTPS proxy.

Strict JSON: topic:string,payload:string,qos?:0|1|2(default1),retain?:bool(defaultfalse).
No client_id/username/password/property/batch fields. UTF8 payload, empty permitted
for retained deletion. Body<=1MiB, existing HTTP parser/header limits, plus actual
MQTT packet-size configured limit. Validate valid nonempty UTF8 topic<=65535bytes,
no wildcard/NUL; reserved exact $SYS or $SYS/ descendants cannot be forged even by
this privileged ingress. Unknown fields/malformed body400, credentials401,
reserved topic403, oversized413, publisher/broker/resource unavailable503, method405.
Existing error envelope {error:{code,message}}. Success202 {accepted:true} means
broker accepted publication, not delivery/execution; no subscribers still202.
A disconnected/timed-out HTTP caller has an ambiguous outcome; no automatic dedup,
cross-recipient transaction, event-specific queue or new durability claim.

Separate transport API (owned mqtt-transport), never add mutations to read-only
ManagementHandle: PublishHandle::new()/Default + Clone, BrokerConfig.publisher:
Option<PublishHandle>, PublishMessage{topic:String,payload:Vec<u8>,qos:u8,retain:bool,
source:String}; PublishHandle::publish(message)->io::Result<()>.
Handle is privileged host API. Only installed running broker accepts; Weak/fenced
startup/shutdown, fixed32concurrent ops16MiB resident input, failfast boundedbudget,
5soperation timeout and6sfinite shutdown drain. Validate resources before sideeffects.
Reuse retained durability and dispatch/complete, publisher=None (NoLocal remains
broker-origin), observer metadata source="http". No synthetic MQTT session, Connect,
Will or lifecycle event. Recipient QoS/offline/persistence/shared and rule/outbound
pipelines reused without source ACL. No durable schema changes.
IO mapping InvalidInput400,PermissionDenied403,InvalidData413,WouldBlock/other503.

Root owns bin/rmqtt HTTP/CLI/state/wiring/docs. Transport owns mqtt-transport in
isolated worktree. Quality owns tests in isolated worktree. Auth-cache work canceled
and reverted, mqtt-auth/mqtt-auth-http and lockfile dependencies must remain unchanged.
lib.rs contains only docs/module declarations/reexports.

Acceptance: real Bearer HTTP->MQTT311/5 QoS0/1/2, retain update/delete/replay,
offline session recovery/restart, shared/NoLocal broker origin, no synthetic session
or client event; tokenmissing/wrong/Basic/duplicates401; scope same as Dashboard;
unknownbody/limits/SYS protections; configured MQTT auth+ACL deny/outage MUST NOT
block privileged downlink or receive any API-source backend call. Existing MQTT
clients continue to authenticate and SUBSCRIBE/PUBLISH with configured permissions.
No performance claim without measurement; fmt/strictclippy/tests/nativeRelease.

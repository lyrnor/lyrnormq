# Phase6e — HTTP downlink publish with Basic authentication

Frozen scope: POST /api/v1/mqtt/publish on the existing admin HTTP listener, accepts
HTTP directly (no mandatory TLS); HTTPS can terminate at a reverse proxy. This exact
route uses Basic only, never the dashboard Bearer token. Existing management/routes
retain Bearer policy and read-only query semantics. Admin listener/token CLI remains
required to run this listener; MQTT itself is unaffected.

Body strict JSON: topic:string,payload:string,qos?:0|1|2(default1),retain?:bool(default
false). No client_id/username/body credentials. Payload is UTF8 bytes; binary/base64,
batch, properties, expiry, scheduling, idempotency and new UI are outside scope.
Body<=1MiB plus bounded HTTP parser/header limits. Validate actual MQTT packet size
against configured limit, valid nonempty UTF8 topic without wildcards/NUL, reserved
exact $SYS or $SYS/ descendants prohibited. Malformed body400, credentials401 +
WWW-Authenticate Basic realm="rmqtt", denied403, oversized413, backend/broker/budget
unavailable503, wrong method405. Error JSON existing {error:{code,message}}.
Success202 {accepted:true}: broker publish accepted, not device delivery/execution.
No subscribers still202. No hidden MQTT client/session, connection events or Will.
Publish may reach a subset if a caller disconnects/times out; no fanout transaction
or retry dedup claim. Device QoS/offline/retained/shared semantics are reused.

Basic parse exactly one header, case-insensitive scheme, canonical Base64 payload
with first colon separating nonempty UTF8 username and raw password bytes (colon in
password permitted), username<=256bytes,password<=65535bytes, no logging/echo of
credentials. Anonymous is never a Basic authenticated identity. HTTP publisher
client_id is fixed empty string for auth and ACL, action Publish on concrete topic;
no synthetic CONNECT ACL/session or ability to claim an existing device Client ID.
Static policies requiring client_id prefix may therefore reject this context.

MQTT CONNECT authentication remains uncached. New HTTP-only successful-auth cache:
default30s, configured CLI --http-publish-auth-cache-ttl-seconds 0..300. Key includes
config generation, cache-clear epoch, username and HMAC-SHA256(process random secret,
length-framed exact username/password). No plaintext/password-bearing cache entries,
Debug/metrics/logs. Bounded4096entries and16MiB including keys/principals, finite
inflight32 with same-credential single-flight. Full cache may evict or bypass;
backend failures and denials never enter successful cache. TTL0 bypasses completed
cache (concurrent single-flight allowed). TTL measured from authentication completion.
Configuration replacement and existing cache-clear invalidate success cache and
inflight results; shutdown prevents cached admission. Accepted snapshots must not
retain old provider generations. Same-secret followers are bounded by HTTP slots;
no global lock across await; static Argon2 hooks run on bounded blocking pool even
when caller cancellation occurs, retain slot until real completion.

Auth coordinator API (owner mqtt-auth-http):
Runtime::authorize_http_publish(&self, username:&str, password:&[u8], topic:&str,
    ttl:std::time::Duration) -> Result<Arc<dyn mqtt_auth::AccessAttempt>, RemoteAccessError>
Authenticates via current remote backend or bounded static fallback, uses existing
exact topic ACL cache (default30s config TTL) and returns resolved UNSEALED attempt.
No repeated HTTP at transport dispatch. MQTT unchanged. Caller validates body and
reserves HTTP capacity first; transport calls attempt.seal immediately before first
side effect, stale attempt fails503. Existing AccessAttempt trait unchanged.

Transport API (owner mqtt-transport), separate from read-only management:
PublishHandle::new()/Default + Clone, added BrokerConfig.publisher:Option<PublishHandle>.
PublishMessage{topic:String,payload:Vec<u8>,qos:u8,retain:bool,source:String}.
PublishHandle::publish(message, Arc<dyn mqtt_auth::AccessAttempt>)->io::Result<()>.
Install into running broker only, unavailable before startup/after stop, fail-fast
bounded32 concurrent publications and16MiB resident inputs, validate before sealing.
Use existing retained durability and dispatch/complete; broker origin publisher=None,
observer metadata source="http:<username>" (same service lane, not a device identity).
No changes to session/outbox schema. source max<=1024bytes; core payload/packet limits
apply. Structured io errors: InvalidInput400, PermissionDenied403, InvalidData413 for
size limit, WouldBlock/other503. Do not introduce synthetic session or global locks.

Root owns bin/rmqtt HTTP/CLI/state/wiring and docs; auth owns crates/mqtt-auth-http;
transport owns crates/mqtt-transport; quality owns tests in separate Git worktrees.
No edits to root Cargo/lock by subagents; root integrates lock. lib.rs thin.

Acceptance: real HTTP->MQTT311/5 QoS0/1/2; retained update/delete/replay; offline durable
resume; shared delivery and NoLocal origin; no auth/session lifecycle effects; Basic
malformed/bearer/duplicate/invalid credentials; ACL topic denial and backend failure;
success auth+ACL cache request counts; wrong-password cache miss; configuration and
manual invalidation; TTL0; concurrent single-flight; bounded capacity and canceled
work; mandatory auth no anonymous fallback; crash/restart existing durable queue.
Strict fmt/clippy/workspace tests and native release; no unmeasured TPS claims.

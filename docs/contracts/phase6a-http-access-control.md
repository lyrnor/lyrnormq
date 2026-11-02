# Phase6a — HTTP access control and Dashboard

Frozen before parallel implementation. External wire semantics are normative in
../http-auth-api.md: username is the stable device identity; authentication204/401,
authorization204/403, no responseJSON. Other statuses unavailable. Preserve all
existing MQTT/session/outbox/Will/storage contracts and thin lib.rs.

## Ownership and isolation

Backend: crates/mqtt-auth-http/** only. Integration: coordinated generic hooks in
crates/mqtt-auth/** plus crates/mqtt-transport/** and crates/mqtt-session/** only.
Dashboard: dashboard/web/** only. Coordinator: bin/rmqtt, root lock/workspace, docs
and independent tests after the implementation writers finish. Each writer uses
its assigned independent worktree/branch, commits tested sources, reports hash.

## Management JSON

All management endpoints require existing admin bearer auth, reject querystrings,
bound64KiB JSON bodies, return existing error envelopes and never raw credentials.
GET/PUT /api/v1/access-control/config; GET /api/v1/access-control/status;
POST /api/v1/access-control/test/authentication;
POST /api/v1/access-control/test/authorization;
POST /api/v1/access-control/cache/clear (body{}).

Config = {schema_version:1,authentication:BackendConfig,authorization:AuthorizationConfig}.
BackendConfig fields (defaults):

```
{ "type":"http", "enabled":false, "name":"HTTP 认证", "url":"",
  "service_auth":{"type":"none"}, "headers":{}, "timeout_ms":1000,
  "max_concurrency":128, "allow_http":false,
  "tls":{"ca_pem":null,"certificate_pem":null,"private_key_pem":null} }
```

AuthorizationConfig has the same flat fields, name="HTTP 授权", plus
cache_ttl_seconds:30. Only HTTP type this phase, future tagged backend extension.
service_auth tagged variants: none; bearer{token}; basic{username,password}; headers.
Header values, bearer token and basic password/private_key_pem are redacted as
[redacted], restored only for the same stage/kind/header key. No arbitrary IDs.
Headers map <=32, total<=16KiB; reject framing/Host/Content-Type and duplicate builtin
Authorization overrides. PEM each<=16KiB, paired cert/key; unencrypted private key
only, no insecure verify. URL<=2048, no userinfo/query/fragment, HTTPS unless allow_http.
Disabled empty URLs allowed; enable/probe validates complete configuration.
timeout100..5000ms, max_concurrency1..256, TTL0..300. Config<=64KiB.

Status={authentication:StageStatus,authorization:StageStatus,cache:CacheStatus}.
StageStatus={enabled,requests_total,allowed_total,denied_total,errors_total,
last_result:null|"allow"|"deny"|"unavailable",last_http_status:null|number,
last_latency_ms:null|number}. CacheStatus={entries,bytes,hits,misses,inflight}.
No requests means unknown, not healthy. Business denial is not service failure.

Auth test body={config:BackendConfig,client_id,username,password?:string|null,
password_base64?:string|null}; authorization test={config:AuthorizationConfig,
client_id,username,action:"publish"|"subscribe",topic}. Password fields exclusive.
Test uses unsaved config, restores matching redacted secrets, force-enables target.
No session/publication/formal-cache/livecounter side effects. Both return HTTP200
JSON {result:"allow"|"deny"|"unavailable",http_status:number|null,elapsed_ms:number,
error_code:string|null}. External401 never becomes admin401. Cache clear returns
JSON CacheStatus; enables no MQTT mutation. Client401 handling remains admin login.

## Runtime and generic interfaces

New crate exports Config, BackendConfig, AuthorizationConfig, Runtime, Status,
ProbeResult and typed Error(InvalidConfig,Busy,Storage,Stopped), plus AclQuery and
AclAction(Publish,Subscribe). Runtime::open(path, fallback:Arc<dyn AccessControl>,
local_accounts:bool)->io::Result<Arc<Runtime>>. Async configuration/status/replace,
clear_cache, probe_authentication(BackendConfig,ConnectInfo),
probe_authorization(AuthorizationConfig,AclQuery). Runtime implements RemoteAccess.
Config persistence reuses mqtt-storage ConfigStore in a separate private SQLite,
not rules/session DB; blocking owner handles IO/drop/cancellation. root CLI
--access-control-config PATH defaults to full rules-config path+.access.sqlite;
export-state must not open/create it. Build/validate then durable save then publish
one immutable generation. Uncertain save stops the controller, never falls open.

Coordinated hooks in mqtt-auth:

```
type AccessFuture<'a,T> = Pin<Box<dyn Future<Output=T>+Send+'a>>;
enum RemoteAccessError { BadCredentials, NotAuthorized, Unavailable, Busy }
trait RemoteAccess: Debug+Send+Sync { fn begin(&self)->Arc<dyn AccessAttempt>; }
trait AccessAttempt: Debug+Send+Sync {
  fn policy(&self)->Arc<dyn AccessControl>;
  fn remote_authentication(&self)->bool;
  fn authenticate<'a>(&'a self,info:ConnectInfo<'a>)
    ->AccessFuture<'a,Result<Principal,RemoteAccessError>>;
  fn pending(&self)->bool;
  fn resolve(&self)->AccessFuture<'_,Result<(),RemoteAccessError>>;
  fn seal(&self)->bool;
}
```

Add default AccessControl::authorize_for_client(principal,client_id,action,resource)
delegating to legacy authorize, and SessionEngine::set_access_control. BrokerConfig
adds remote_access:Option<Arc<dyn RemoteAccess>>, defaultNone. No durable schema change.
Backend and integration coordinate exact trait signatures before incompatible edits.

begin pins immutable generation and ephemeral answers. policy is synchronous,
cache-only and records ALL used receipts/misses. Unknown optimisticallytrue permits
discovery ONLY: that proposal/outcome (including error) is discarded on anymiss.
resolve does bounded async HTTP outside synchronous core; rerun unchanged state.
TTL0 still pins answers for this one bounded attempt. pending also covers overflow/
unavailable, never maps unknown to false. seal atomically validates config/cache
generation and receipts before FIRST retained/fanout/storage side effect; after
seal existing admitted QoS transaction may finish despite later TTL/config change.
Hold attempt through accepted pending transaction; do not replay partial fanout
because authorizationexpired. No locks across await. Max4096 requirements/1MiB per
attempt, finite live generations/attempts/global memory. Process HTTP cap256 across
generations, cache64MiB shared, independent of outboundHTTP16. Disabled fastpath
must preserve legacy behavior and avoid unnecessary remote/cache budget use.

HTTP authentication is async with rate/handshake limits, not local Argon CPU4 cap.
PLAIN uses async parsing/exchange state without manager lock across HTTP; preserve
custom synchronous providers. Even local CONNECT carries generation admission so
turningHTTPon while a local hash runs cannot admit stale policy. Assigned IDs are
resolved before HTTP; late/cancelled connects cannot orphan or take over a newer
generation. Preserve all prior admission/cleanup semantics.

Composition: bothHTTP stagesoff exactlylegacy. HTTPauthnON+ACLoff uses configured
StaticPolicy ACL if local_accounts, otherwise permits trusted authenticated user:*
owners; HTTPACLON uses point Topic decisions, Connect identity stays local. HTTPACL
alone requires local_accounts, otherwise reject save/start, not anonymous bypass.
Runtime passes fallback actual policy, not guessed from flag. Trusted username
derived from Principal user:<username>, validated1..256bytes. Anonymous cannot be
queried as another device. Publish/Subscribe/Receive map external actions as documented.

## Reliability gate (must implement, not TODO)

Cover Request/Deliver/Tick/startup/finish prepare paths before any effects. All core
checks use contextual client ID, including legacyaggregate recovery. Decode restore
without remote-denial cleanup or writes first; mark crashed online Will anchors once,
then prepare/reconcile/recover/commit. SQLite owners never drop on async workers.
Unavailable must preserve durable rows/intents; ordinary recipient false cannot
silently drop unknown-authorized QoS data then ACK publisher. Design bounded pending
continuations/explicit failures and replay-safe tickets, test partial fanout. Required
ACK/control flow must not wait on whole-session remote reconciliation. Original
absolute expiry/Will delay timestamps must not slide through retries. The internal
continuation details are integration-owned but must satisfy these acceptance rules.

## Dashboard and verification

Use existing #security route with authentication/authorization tabs. Basic name,
URL, service auth; optional requestheaders and advanced timeout/concurrency/TLS,
authorizationTTL/cacheclear. Components for all controls, file inputs for PEM.
No editable request body, queues, IDs, SDK/DB internals. Test drawer uses drafts,
clears device password on close, no browser persistence. Enabled, no requests,
allow/deny and unavailable distinct; local policy summary must stay truthful.

Run fmt, strict all-target workspace Clippy, Rust/frontend/tests, real HTTP/TLS/mTLS
and MQTT3.1.1/5 QoS0/1/2, binarycredentials, asyncPLAIN, cache0/expiry/fault/reconfig,
takeover, retained/Will, durable restart/kill/recovery. Verify embedded Release and
locked amd64musl zigbuild. Record actual results and unfinished limitations; no
PostgreSQL implementation, no invented performance or full MQTT compliance claims.

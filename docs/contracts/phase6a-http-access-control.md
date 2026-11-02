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
  "max_concurrency":128, "allow_http":false }
```

AuthorizationConfig has the same flat fields, name="HTTP 授权", plus
cache_ttl_seconds:30. Only HTTP type this phase, future tagged backend extension.
service_auth tagged variants: none; bearer{token}; basic{username,password}; headers.
Header values, bearer token and basic password are redacted as
[redacted], restored only for the same stage/kind/header key. No arbitrary IDs.
Headers map <=32, total<=16KiB; reject framing/Host/Content-Type and duplicate builtin
Authorization overrides. HTTPS uses the default trusted roots and hostname verification;
no custom CA, client certificate, private key, or TLS configuration fields. No insecure
verify. URL<=2048, no userinfo/query/fragment, HTTPS unless allow_http.
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

Coordinated refinement: AccessControl::reconciles_stored_authorization()->bool
defaults true. HTTP ACL probes return false: a point decision must not trigger
whole-session pruning or refresh every stored permission on ACK. Check actual
deliver/poll/begin targets and due Will only; deny retires only that operation.
Pure protocol ACK paths retain progress when unrelated remote decisions expire.
BrokerConfig.remote_plain:bool defaults false; coordinator sets it only for the
existing --enhanced-auth-plain builtin, preserving custom enhanced providers.

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

Composition clarification: when BOTH HTTP stages are enabled, local Connect checks
only validate trusted user:* identity/client syntax; do not require that HTTP device
to also exist in the old static account file. With HTTPauthnON+HTTPACLoff and a static
policy, its Connect/Topic ACL constraints still apply. With HTTPauthnOFF, local
credential and Connect policy remain authoritative. Status requests_total counts
completed formal calls (probes excluded), UI labels that meaning explicitly.

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
URL, service auth; optional requestheaders and advanced timeout/concurrency,
authorizationTTL/cacheclear. Components for all controls; no certificate or private-key upload controls.
No editable request body, queues, IDs, SDK/DB internals. Test drawer uses drafts,
clears device password on close, no browser persistence. Enabled, no requests,
allow/deny and unavailable distinct; local policy summary must stay truthful.

Run fmt, strict all-target workspace Clippy, Rust/frontend/tests, real HTTP and HTTPS trust-rejection
and MQTT3.1.1/5 QoS0/1/2, binarycredentials, asyncPLAIN, cache0/expiry/fault/reconfig,
takeover, retained/Will, durable restart/kill/recovery. Verify embedded Release and
locked amd64musl zigbuild. Record actual results and unfinished limitations; no
PostgreSQL implementation, no invented performance or full MQTT compliance claims.

### Durable recipient quarantine refinement

For HTTP ACL point mode, an existing accepted subscription permits durable enqueue
into its recipient's existing bounded queue without a fresh Receive RPC. This is
quarantine, not permission to send. Publisher ACK still follows recipient durable
acceptance. Actual Poll/Begin must obtain exact recipient username/client/topic
Receive permission before wire output: denial retires only the affected record,
unavailable preserves it and retries within existing bounds/absolute expiry.
Retained replay and shared selection must preserve their protocol semantics;
unauthorized shared candidates must not bypass selection/fallback or duplicate rules.
Do not convert unknown receiver authorization to false and lose data before sourceACK.
Detached bounded prepare continuations let pure ACK/control commands progress while
application decisions wait; installed engine must release completed probe objects.

Shared refinement: preauthorize a candidate's exact Receive before enqueue so an
explicitly denied member cannot consume the group's only copy. Use tri-state
Accepted/Rejected/Unavailable delivery outcomes. Try other members; when all still
potential candidates are unavailable, retain a bounded source pending fanout and
retry the unresolved group with backoff without repeating already-dispatched
ordinary recipients. No success sourceACK while unknown; stop on original message
expiry, superseded source generation or shutdown without extending deadlines.
All explicitly denied/noeligible remains no-subscriber. Preserve QoS2 stable delivery
ticket dedup across partial ordinary/shared fanout, disconnect and crash recovery;
this needs an independent regression, not only an in-memory retry test.

Shared preflight supersedes the previous ordinary-first retry wording: before ANY
retained update or fanout, preflight shared groups and select a currently authorized
candidate (or establish all explicit denials/noeligible). If a group has no allowed
candidate and unresolved availability, defer the whole publication with bounded
continuations; no ordinary recipient is dispatched yet. Carry admitted candidate
permits through fanout without a new RPC. This avoids introducing an HTTP wait/crash
window after ordinary delivery. Existing cross-client source/recipient storage
transactions are not magically made one atomic transaction: audit and report the
preexisting generic crash gap separately; do not claim durable delivery_tickets
(ephemeral today) or global exactly-once. The HTTP integration adds no durable schema
change and must prove no new partial-fanout window with a deterministic regression.

Runtime::is_stopped()->bool is a nonsecret read accessor. Management GETstatus must
return503/access_stopped when a cancelled/uncertain configuration save has fenced
the controller, so a stopped service cannot masquerade as disabled/no requests.
GETconfig may still expose the last redacted configuration for operator review.

Parallel pipeline split after the HTTP backend's tested followup: pipeline owner
uses a NEW isolated worktree and writes ONLY mqtt-transport/src/durable/actor.rs,
broker.rs and new actor_remote.rs/broker_remote.rs. Integration owner retains all
mqtt-auth/session changes and transport authentication/admission/config/durable.rs/
enhanced/remote_plain, and must not edit pipeline-owned files. New private boundary:
ConnectRequest.access_attempt carries the same authentication attempt to actor
admission; Pending retains accepted attempt, source finish uses that admitted policy.
Coordinator integrates each tested dependency commit. This supersedes the broad
single integration-owner assignment for these four pipeline files only.

Shared membership race clarification: preflight may admit a selected candidate and
known-authorized backups. Actual delivery still checks current membership, NoLocal,
capacity and session generation. After first side effects, use only admitted
candidate permits; never issue a new RPC to unknown backups. If all admitted members
became ineligible meanwhile, handle the normal noeligible membership race rather
than delivering to a stale generation or restarting ordinary fanout. This does not
promise an atomic subscription-membership transaction across actors.

Post-backend handoff audit ownership: the quality specialist may use a separate
new audit worktree for targeted regression-first corrections in mqtt-auth-http
attempt/cache/tests/Cargo dev-test features only, after the original backend writer
has moved exclusively to pipeline files. This never overlaps another writer.
Legacy both-HTTP-disabled attempts must not acquire remote-only deadlines or limits;
source generation admission still applies to prevent stale local credential commits.

Startup recovery split: recovery owner writes ONLY new transport durable/
broker_recovery.rs in a separate worktree. Pipeline owner keeps broker.rs and
extracts/replaces old restore call with child module recovery::restore(config,
clock,seed).await returning existing Restored. The child can access broker's
private Restored/row helpers via super, so no persisted schema or public API change.
Recovery owner owns pure decode, cancellation-safe blocking store ownership,
prepared restore/migration and point-mode due-Will deferral. Pipeline owner must
not independently implement that new module. Mark crash Will anchor exactly once;
point-mode startup must not consume due intents before actors/shared preflight.
Only nondestructive anchoring may be persisted without a remote grant; unavailable
must not commit permission-pruning or reset existing absolute deadlines.

Recovery test-harness exception: in its own recovery worktree ONLY, recovery owner
may apply temporary broker.rs module/call glue to compile and run the new child
module tests while awaiting pipeline's real glue. Restore that parent file before
commit and report the temporary harness. Never commit it or edit the pipeline
worktree; final parent remains exclusively pipeline-owned and root retests actual
integrated files. This is an isolated dependency harness, not competing source.

## Simplified HTTP TLS configuration (2026-11-02)

User-directed revision: remove BackendConfig.tls and its entire custom CA/client
certificate/private-key feature from authentication and authorization, including
Dashboard, persistence DTOs and probe inputs. HTTPS remains supported through normal
reqwest trust roots and mandatory hostname verification. Unknown tls input is rejected
by strict request DTOs. Do not silently reuse or deserialize old certificate-bearing
configuration as a weaker policy. For persisted configuration only, old tls objects
containing exclusively the three known keys with null values (or no keys) may be
removed while loading; retain all other settings. Requests still reject any tls
field. Old nonempty/unknown TLS configuration must fail closed without rewriting
the stored configuration. MQTT listener TLS/mTLS and Kafka TLS are unchanged.

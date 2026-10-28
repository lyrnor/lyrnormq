# Frozen Phase2d authentication / ACL contract
Implement MQTT3.1.1 credential support, pluggable synchronous bounded access policy,
local Argon2id PHC policy and fail-closed enforcement through existing transactional
SessionEngine/outboxes. Keep lib.rs docs/mod/reexports only. No TLS/remote auth/hot
reload or full security/conformance claim. Default no-policy behavior remains open
anonymous MQTT; supplied unknown credentials are refused, not silently ignored.

## mqtt-auth (new owned crate)
Public API via root reexports, implementations in modules:
`Principal {pub name:String}` Clone/Debug/PartialEq/Eq; `Principal::anonymous()->Self`
returns name="anonymous". Static users use name="user:"+username to avoid collisions.
`ConnectInfo<'a> {pub client_id:&'a str,pub username:Option<&'a str>,pub password:Option<&'a [u8]>}`; custom Debug must redact password (or don't implement Debug).
`Action {Connect,Publish,Subscribe,Receive}` Copy/Debug/Eq.
`AuthError {BadCredentials,NotAuthorized}` Error/Display, no credential values.
`trait AccessControl:std::fmt::Debug+Send+Sync {fn authenticate(&self,info:ConnectInfo<'_>)->Result<Principal,AuthError>;fn authorize(&self,principal:&Principal,action:Action,resource:&str)->bool;}`.
`AllowAnonymous` unit struct Debug/Default, authenticates only absent username AND
password, rejects supplied credentials as BadCredentials, authorizes all actions
only for anonymous principal (including $ topics, preserving previous defaults).
`MAX_POLICY_BYTES:usize = 1024*1024` root-reexported for bounded CLI IO.
`StaticPolicy::from_json(bytes:&[u8])->Result<Self,PolicyError>` typed errors, Debug
redacts hashes. No IO inside policy; caller loads bounded bytes on blocking worker.

Policy JSON required allow_anonymous:boolean and users:array; optional anonymous
rule-set defaults to empty deny lists/prefix"". Each user requires username,
password_hash,client_id_prefix,publish,subscribe. Rule-set fields client_id_prefix,
publish,subscribe required. Deny unknown fields, duplicate/empty users, invalid topic
filters, invalid PHC algorithms/versions/parameters. Users<=1024, each name<=256
UTF8 bytes/no NUL, each publish/subscribe list<=128, total file<=1MiB, MQTT filter
length/validity rules reused (shared filters still unsupported). Password hashes
must be Argon2id version19 with bounded m<=65536KiB,t<=10,p<=4 and valid minima;
validate all parameters BEFORE verification. Use maintained RustCrypto argon2
(prefer cached0.5.3 for stable API), serde/json and existing mqtt-router matcher.
No plaintext password in policy or logs; password_hash values are PHC strings.
Authentication requires username+password for configured users, verifies hash, then
checks literal ClientId prefix. Missing/unknown/wrong credentials ->BadCredentials;
valid identity but wrong prefix/disabled anonymous ->NotAuthorized. Anonymous allowed
only when explicit flag; its own rule-set controls permissions. Static missing user
principals never get rights. ActionConnect checks ClientId prefix; Publish exact
topic uses publish grants/matches; Receive exact topic uses subscribe grants/matches;
Subscribe requires requested filter language safely contained within one grant.
Containment may conservatively reject equivalent/union coverage but MUST never
allow broader access (+/#/empty levels/$SYS/root wildcards). Add exhaustive bounded
reference-topic tests proving no false grant; no matching requested filter as a
literal topic. Expose `covers_filter(grant:&str,requested:&str)->bool` for testing.
Provide an owned-crate example hash_password reading bounded binary stdin, printing
only randomly salted Argon2id PHC, so users can create actual policy files. Use
OS randomness and normal recommended defaults; do not print plaintext or read it
from command args. No external Python crypto dependency needed for client tests;
supply fixed synthetic PHC/password fixtures to Quality after implementation.

## Protocol
Packet::Connect adds username:Option<String>,password:Option<Password>.
Password wrapper private bytes, new(Vec<u8>),as_bytes()->&[u8], Clone/Eq and custom
Debug redacted. Prefer Zeroizing<Vec<u8>> using zeroize1.x; raw wire buffers are not
claimed fully wiped. Root reexport Password. MQTT username is UTF8 u16 string, password
opaque binary u16 bytes after Will fields; absent and empty distinct. Validate
password flag requires username flag, reserved/Will flags unchanged. Encode/decode
both, include lengths/truncation/fragmentation, update owned constructors with None.
No authentication in codec. Packet Debug must not expose password bytes.

## Session / persisted ownership
Engine holds Arc<dyn mqtt_auth::AccessControl>, not serialized. Existing new/restore
constructors wrap AllowAnonymous. Add `new_with_access(config,Arc<dyn AccessControl>)`
and `restore_with_access(config,bytes,Arc<dyn AccessControl>)` with existing return
types. Existing connect/connect_with_will use anonymous principal; add
`connect_authenticated(&mut self,id:&str,clean:bool,will:Option<Will>,principal:Principal)->Result<ConnectResult,EngineError>`.
Add EngineError::NotAuthorized. Check valid principal (nonempty/bounded/no NUL),
ActionConnect, existing Session/Will owner equality and new Will Publish right
BEFORE takeover, retry, reset or any mutation. Cross-principal same ID is denied
for BOTH clean flags, even when a pending Will has no SessionRecord. No owner binding
is silently transferred. Passwords/hashes/CONNECT credentials never enter snapshots.

Session and Will records gain owner:string, queued/outgoing message formats stay.
Business JSON schema_version becomes3 (SQLite opaque container remains1). Explicitly
migrate valid v1/v2 owners to anonymous, never first-authenticated claimant. V1 retains
its old record rules/empty globals, v2 its required globals; v3 requires owner for
all session/Will records. Reject malformed owner/unknown schemas; preserve IDs/stages,
retained state, pending Will and send order. Existing default anonymous DBs still
resume; enabling authenticated-only policy can deny legacy anonymous records.

Enforce Publish exact topic before any PUBLISH mutation/ACK and again on known
incoming QoS2 PUBREL before removing pending original payload. Denial closes client
without successful ACK/retained/outbox effects. Unknown PUBREL remains harmless.
SUBSCRIBE unauthorized filters get0x80 individually, no inserted/replaced subscription
or retained replay for that filter; accepted filter ordering/QoS behavior unchanged.
Unsubscribe/control ACK/PING/disconnect allowed for the established owner/generation.
Route only to recipients with current Connect(clientId) and Receive(topic) rights.
Retained replay additionally checks Receive; offline delivery/replay cannot bypass
policy after restart with changed config. Retained records themselves are global,
not automatically removed when publisher loses rights; recipient rules protect reads.
Will Publish/Connect rights are checked before registration and before firing/retry;
revoked active/pending Wills are suppressed/removed atomically, not deferred forever.
This explicit fail-closed revocation policy takes precedence over old permissions.

Add `reconcile_authorization(&mut self)->Result<bool,EngineError>` to prune revoked
subscriptions, queued messages, outgoing payload-bearing AwaitAck/AwaitReceipt state
and Wills; free discarded IDs and remove replay cursors together. Incoming QoS2
pending is retained so revoked PUBREL is still denied rather than acknowledged as
unknown. Already accepted AwaitComp may finish PUBREL/PUBCOMP (no new payload).
Provider is immutable per run in this phase, but checks occur at operation boundaries.
Add `PollOutcome {pub packet:Option<Packet>,pub changed:bool}` and
`poll_with_changes(&mut self,id:&str)->Result<PollOutcome,EngineError>`; old poll wraps
packet for compatibility. This signals discarded unauthorized data even when no
packet returns, so worker commits pruning before installing memory. Respect existing
clone/apply/snapshot/commit/install rollback; no independent QoS/ACL routing FSM.

## Runtime
BrokerConfig adds `access_control:Arc<dyn AccessControl>` default AllowAnonymous.
Worker authenticates CONNECT before engine mutations, checking Will permission and
session owner through connect_authenticated. OperationConnect carries username and
Password; Online principal may be tracked but Session owner is authoritative. Bad
credentials -> CONNACK4; unauthorized ClientId/owner/Will -> CONNACK5, close after
bounded flush. No processing pipelined packets after rejection, no registered Will,
no old client cancellation or clean reset. Use explicit rejection-close flag for
any actually queued failure CONNACK, preserving established-error abort behavior.
Typed worker rejection result preferred over parsing error strings. Credentials
never appear in errors/debug/snapshots; hooks/Argon2 execute on existing bounded
blocking worker, not async IO tasks. Other model/resource errors retain current behavior.

Startup attaches configured policy, reconciles, recovers authorized Wills and commits
schema3 before ready. Per-event pruning and Will retry changes force commit/notify,
including empty poll. Use poll_with_changes; discard proposal on denied ops. Session
owner/gen checking protects takeover and offline recovery. Preserve bounds, writer
cancellation, graceful DISCONNECT, pending-self-Will recovery and both storage modes.
Update owned Rust CONNECT constructors with None credentials. No policy file IO in
transport; CLI loads before binding. Root trait APIs allow embedded custom policy.

## Coordinator CLI
Add --auth-policy PATH into Options, parse path without IO. Load <=1MiB+1 using a
bounded Read on spawn_blocking before bind, StaticPolicy::from_json, install Arc in
BrokerConfig. Add mqtt-auth dependency to binary, CLI tests/example/docs. No credentials
via CLI arguments/env or plaintext config. Root workspace wildcard includes new crate.
Default anonymous case unchanged; production TLS/rate limiting remain future work.

## Independent Quality
Own tests/**/CI. Add auth_acl_blackbox.py and Paho auth script BOTH memory/SQLite.
Use fixed synthetic PHC fixtures (argon2 agent supplies) so no Python crypto install.
Check username/binary password malformed flags, valid/wrong/missing credentials,
anonymous config, codes4/5+close, prefix checks, denied takeover/clean reset/Will/
pipelined packet no effects. Publish deny at QoS0/1/2 including retained deletion
must not ACK/mutate; subscribe mixed grants and broad wildcard escape rejected,
$SYS explicit grants, retained replay safe. Persistent owner cannot be stolen by
other user with same ClientId, even clean=true; same user resume works. Restart with
restricted ACL prunes queued/awaiting payload, retained/Will/pending/QoS2 PUBREL rights
rechecked, passwords absent from SQLite/Debug/errors. Legacy schema2 migration binds
anonymous and does not transfer to authenticated claimant. Existing all suites remain.
CI builds hash example/tests as necessary; no real secrets committed. Actual Paho
username_pw_set verifies auth+ACL. No claims about physical power loss/timing-attack
resistance/rate limiting/TLS from this scope.

## Isolation / scheduling
Wave1 Auth provider + Protocol credentials + Session ownership/model in separate
worktrees; Session uses frozen APIs and picks dependency commits before tests.
Quality starts after an early slot release; Transport starts from integrated real
model/APIs. Coordinator owns root CLI/docs/manifest. Each writer owns only assigned
crate; lib.rs stays thin. API changes require coordinator contract update first.

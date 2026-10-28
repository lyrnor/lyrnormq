# Phase2j frozen Shared Subscriptions and Enhanced AUTH contract

Normative OASIS MQTT5 OS4.8.2/4.12/4.12.1,3.8.3.1 and SASL PLAIN RFC4616.
https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
Reuse existing trie/ACL/Clock, schema records, canonical outbox/QoS, alias writer,
packet caps and blocking worker. lib.rs stays documentation/mod/reexports only.

## Isolation/dependencies
Shared specialist owns mqtt-session/** and coordinated mqtt-router/** in isolated
agent/p2j-shared. Auth specialist owns mqtt-auth/** in agent/p2j-auth. Transport
specialist owns mqtt-transport/** in agent/p2j-transport, codes against frozen Auth
API then cherry-picks its real commit before final gates. Coordinator owns CLI/docs/
root lock. Released Auth slot enables independent Quality writer tests/**/CI in its
OWN worktree. All code commits only owned paths, no shared concurrent source writes.

## Shared filter and selection contract
Add router split_shared_filter(&str)->Option<(&str,&str)> (valid full shared filter
only), validate_subscription_filter(&str)->bool, validate_topic_filter(&str)->bool
(pure normal syntax including literal $share prefix), and
Router::subscribe_topic_filter_qos(u64,String,u8)->bool for indexing underlying
plain patterns. Existing validate_filter/subscribe_qos preserve ordinary behavior.
Parse exactly $share/{nonemptyName}/{nonemptyFilter}; name excludes /+#, underlying
uses normal wildcard/$/UTF8 semantics, never recursively unwrap. Nested-looking
underlying $share names are literal Topic patterns. Invalid shared filter SUBACK8F;
shared NL1 remains protocol82. V5 supports creation; V311 creation getsSUBACK80;
V5 persistent memberships can resume over311 and unsubscribe full shared key.

Store full shared key in subscription record; ACL Subscribe checks underlying
filter, Receive checks exact publication Topic, Connect checks owner ClientID.
Apply this normalization to admission, restored/current policy reconciliation and
routing. Do NOT unwrap Publish topics or policy grants as groups. Shared RH is
ignored for initial replay: NO retained messages on new/replaced shared subscribe.
Respect granted QoS/RAP and attach only that chosen shared subscription's ID.

Groups are keyed by FULL shared filter (groupName plus underlying filter). Each
matching group independently selects ONE member; ordinary matching contributes its
separate copy, and separate groups may select the same client and produce copies.
No cross-group/ordinary dedup or identifier mixing. Use existing indexed trie for
underlying matching; build bounded group/membership views from registry as needed.
Deterministic round-robin by client ID, online eligible members preferred, skipping
revoked/disconnected/full-queue/peer-oversize candidates. If no eligible online fit,
QoS1/2 may select a surviving persistent offline member; QoS0 never queues offline.
All genuine fitting queues full => atomic Capacity, no cursor/data partial commit;
all peer-oversize => discard that group's copy without poisoning other copies.
Preserve server-global size policy distinctly. Count ALL planned copies against a
recipient's queue capacity, including multiple groups/ordinary overlap.

Selection is atomic with outbox commit and becomes owned by that chosen session.
QoS2 MUST resume only there through PUBREC/PUBREL/PUBCOMP; never reassign on drop,
negative ACK or session end. QoS1 policy deliberately also waits for that recipient,
with no automatic redistribution after its session ends: this documented SHOULD
exception retains existing bounded session-owned delivery semantics and avoids
ambiguous duplicate reassignment; group-owned redelivery spool is future work.
No claim of seamless QoS1 reassignment or cluster load balancing.

## Schema7 and group lifetimes
Business JSON7, SQLite container1. Required nullable shared_subscription:Option<String>
on queued Message and outgoing record, matching runtime per-ID sidecar; ordinary
copies None, shared copies valid full group key. Comp mustNone/no payload metadata.
Legacy1..6 explicitly migrate None preserving all fields/IDs/deadlines/owners/replay
markers/options; strict6 DTO rejects shared subscription filters/new markers that
old runtime could not create. New7 accepts shared records only NLfalse. Existing
source incoming/retained/Will do not gain recipient-group metadata.
Cursors are runtime-only, bounded by current groups, reset on restart, advanced only
with real selection and pruned when groups disappear. Fairness across crashes is
not durable. Membership survives while sessions survive. When LAST member leaves,
prune undelivered queued/unstarted outgoing group copies; started QoS exchanges
remain protocol obligations. Existing accepted deliveries are not retroactively
retagged after membership/ID/RAP replacement. Validate marker shape before pruning.

## Enhanced auth provider API (mqtt-auth exports)
Use these exact names/types so Transport and Auth can code independently:
EnhancedConnectInfo<'a> {pub client_id:&'a str,pub username:Option<&'a str>,
pub method:&'a str,pub data:Option<&'a [u8]>} with redacted Debug.
EnhancedAuthError::{UnsupportedMethod,BadCredentials,NotAuthorized,Protocol}.
EnhancedAuthStep::{Continue{data:Option<Vec<u8>>},Complete{principal:Principal,
data:Option<Vec<u8>>}}; Debug MUST redact response data.
trait EnhancedExchange:std::fmt::Debug+Send {fn step(&mut self,data:Option<&[u8]>)
->Result<EnhancedAuthStep,EnhancedAuthError>;}
trait EnhancedAuthenticator:std::fmt::Debug+Send+Sync {fn start(&self,
info:EnhancedConnectInfo<'_>)->Result<(Box<dyn EnhancedExchange>,EnhancedAuthStep),
EnhancedAuthError>;}
PlainAuthenticator::new(access:Arc<dyn AccessControl>)->Self implements provider.

PLAIN is the real initial mechanism, not a mock cryptographic protocol. Method
exactly"PLAIN"; absent initial data => Continue(None), supplied data => verify now.
Format [authzid] NUL authcid NUL password, exactly2separators, UTF8, nonempty authcid/
password; authzid empty or exactly authcid (no delegation), CONNECT username ifgiven
mustmatchauthcid. Max data65535bytes, username256; reuse access.authenticate with
resolved ClientID/password and existing Argon2 policy/ACL. Exchange completed only
once, invalid sequences/error finish fail closed. Password bytes never logged,
serialized or retained by exchange; any owned temporary secret zeroized. No new
crypto algorithm/credential database. PLAIN has no encryption; default listener is
loopback and protected transport remains operational requirement. SCRAM/other SASL
mechanisms are not claimed implemented; provider API permits later addition.

## Enhanced wire lifecycle and worker integration
BrokerConfig.enhanced_auth:Option<Arc<dyn EnhancedAuthenticator>>, defaultNone.
CLI coordinator adds --enhanced-auth-plain flag, initializes provider after loading
existing access policy. Basic username/password behavior unchanged without method.
Unknown/disabled method8C; enhanced CONNECT Password field is rejected82 to avoid
ambiguous credentials. AuthenticationData withoutmethod remains protocol error.
Every AUTH and successful enhanced CONNACK includes SAME negotiated method; include
provider response data when present. No method in basic success. Initial exchange
AUTH18/clientAUTH18 -> successfulCONNACK0; client0/19 beforeinitialsuccess invalid82.
No PUBLISH/SUBSCRIBE/session access before authentication completes. DISCONNECT/EOF
before success only cancels handshake; no Will/session created or old connection
modified. All provider calls run on bounded serialized blocking worker.

Store pending initial CONNECT/exchange by connection generation, bounded by max
connections; reserve generated IDs against BOTH pending requests and engine session/
Will keys. Strip consumed AuthenticationData from stored request, redact all Debug.
Complete authentication, exact final CONNACK+method/data/assignment preflight, then
use authenticated Principal for existing owner/connect/Will checks and durable
proposal; commit before success/registration/takeover. No fallback basic-auth or
second password verification. Failed/abandoned auth leaves old state intact.
ALL connection exit paths cancel pending auth even with no established identity.

Connected client with method may startAUTH19. AUTH18 thenvalidates active exchange;
server finishesAUTH0; basic AUTH, missing/changed method, overlapping/nested19 or
unsolicited18/0 invalid82. Reauth failure disconnects86/87 asappropriate, same source
Will behavior as other abnormal ends. Require same Principal and current Connect
ACL on success; do not change durable owner/Will/session or reset alias/quota/QoS
state. Normal publish/sub/ACK/control flow can continue under previous principal
while reauth pending. Empty/malformed/wrongcredentials fail closed; no retries
silently accepted. Steps bounded8, bytes cappedpacket/u16 limits. Initial auth uses
absolute handshake deadline (AUTH traffic cannot extend it); reauth has separate
absolute handshake_timeout deadline, independent of keepalive/other traffic. Check
expiry after provider returns and before any session mutation. Clean all pending
state on timeout/shutdown/cancel. Provider hooks must have bounded CPU/no unboundedIO.
Exact encode+peer cap applies to challenges, final data, refusals and allAUTH frames;
preflight mandatory method success before starting challenge. BasicCONNACK stays
20/46; enabled shared capability value1 does notchange length.

## Acceptance
Failure-first unit/wire/Paho/dedicated AUTHclient memory+SQLite, real commits:
shared group separation/onecopy/roundrobin/ordinary overlap, wildcard/$, deniedACL,
memberlimits/no-retained/IDs/RAP/invalidNL, slow/full/oversize/offline choices,
last-member cleanup, durableQ1/Q2/restart/negativeACK/no-Q2-reassign/expiry;
PLAIN direct/challenge/reauth/malformed/methodchange/unsupported, no-preauth mutation/
Will/takeover, assignedID, pending cleanup/boundedrounds/timeouts/pipeline/control
progress, identitychange denied, caps/alias/SessionPresent/restart/secretredaction.
Use independent rawclient where SDK lacksAUTH; explicitly disclose SDK adapters.
Run old full regression, fmt/Clippy/test/rustdoc; report actual data and limitations.

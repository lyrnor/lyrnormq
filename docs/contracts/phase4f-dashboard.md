# Phase4f — React Dashboard and read-only management API

Frozen 2026-10-30 before parallel implementation. Supersedes Phase4e's exclusion of management work only for this explicitly requested phase. Preserve MQTT semantics, durable schema, ACK/outbox atomicity, authentication and thin lib.rs. Existing design under dashboard/design is the visual reference; no synthetic data in live UI.

## Transport contract

`mqtt_transport::management` is public. Add `BrokerConfig.management: Option<ManagementHandle>` default None. `ManagementHandle` is Clone + Debug + Default, `new()`; supplied before serve, observes exactly that broker, no second broker/storage reader. Public async methods return `std::io::Result<T>`: `overview() -> Overview`, `sessions(query: Query) -> Page<SessionView>`, `session(client_id: &str) -> Option<SessionView>`, `subscriptions(query: Query) -> Page<SubscriptionView>`, `retained(query: Query) -> Page<RetainedView>`. Query {offset:usize,limit:usize,search:String,state:String,protocol:String}; default limit25, empty strings mean all. Enforce limit1..100, offset<=1_000_000, search<=256 bytes, state empty/online/offline, protocol empty/3.1.1/5.0. Stable client_id/filter/topic ordering, total after filter. Reject invalid query. Not ready/stopped => io WouldBlock; no stale ready result.

DTOs derive serde Serialize with JSON names as Rust fields. Page<T> {items:Vec<T>,total:usize,offset:usize,limit:usize}. Overview {ready:bool,uptime_seconds:u64,online_sessions:usize,offline_sessions:usize,subscriptions:usize,retained_messages:usize,messages_in_total:u64,messages_out_total:u64,bytes_in_total:u64,bytes_out_total:u64,state_bytes:usize}. Message counters are PUBLISH frames received/successfully flushed, including retransmissions; bytes are encoded MQTT frame bytes (not TCP/TLS/WS overhead). Process-lifetime non-durable counters reset at restart; sessions count committed model presence/online status, not accepted sockets. Overview has no fake rate or latency. Frontend derives per-second rates from monotonic counter differences and elapsed sample time; resets clear history.

SessionView {client_id:String,online:bool,protocol:String,principal:String,persistent:bool,subscriptions:usize,queued:usize,inflight:usize,receive_maximum:u16,session_expiry_interval:u32}. Principal is authenticated principal identifier, never password, hash, auth data, certificate or secret path. Do not label it username unless actually known. SubscriptionView {client_id:String,filter:String,qos:u8,no_local:bool,retain_as_published:bool,retain_handling:u8,identifier:Option<u32>}. RetainedView {topic:String,qos:u8,payload_bytes:usize,expires_at_millis:Option<u64>}; no payload exposure. Hide expired records using existing clock, do not mutate timers on read.

Bound queries/concurrency (at most8 outstanding management queries, fail fast WouldBlock); no snapshot serialization/storage export/full state cloning on read or per PUBLISH. Obtain only requested metadata, page allocations bounded; no unbounded topic/time-series or event tracking. Prefer bounded actor queries and existing registry/index; no locks held across await. Read views may span concurrent commits and are explicitly eventually consistent, each session must reflect installed committed state. Disabled management has minimal/no observer overhead. Recovered persistent sessions and retained rows appear after startup; transient QoS messages and pending Will are never exposed. No lifecycle mutation/disconnect/publish/delete endpoints this phase.

## HTTP/CLI contract (coordinator)

CLI `--admin-listen IP:PORT --admin-token-file PATH [--dashboard-dir PATH]`, complete pair mandatory; only numeric loopback addresses allowed. Disabled by default. Reject duplicates/empty/incomplete/export-state combinations. Token file regular <=4096 bytes, trimmed ASCII token32..256 bytes, visible ASCII except whitespace; load on blocking pool before accepting connections, never log/return token. API `Authorization: Bearer TOKEN` for every /api route, no query/cookie fallback, no CORS, no mutation endpoints; API responses Cache-Control:no-store. Constant-time token comparison. Body/URI/header/time/connection/query bounds; all errors redact internals. Existing MQTT auth separate from administrator token. Remote access via SSH tunnel or authenticated TLS reverse proxy, never expose plain HTTP externally.

GET /api/v1/overview => Overview plus `sampled_at_millis:u64`, `instance` {version,listen,transport,protocol,storage,tls,mtls,auth_policy,enhanced_auth}, `resources` {cpu_percent:null,rss_bytes:null}, `capabilities` {events:false,resource_metrics:false,mutations:false}. GET /api/v1/sessions, /subscriptions, /retained => typed Page per query. GET /api/v1/session?client_id=... => SessionView or404. GET /api/v1/configuration => {instance,limits:{max_connections,max_sessions,max_packet_size,max_subscriptions,max_retained,max_inflight,outbound_capacity,max_handshakes,max_auth_tasks,auth_rate_per_second,auth_burst,handshake_timeout_ms,write_timeout_ms,topic_alias_maximum}}. No token, hashes, keys, certificate/auth/storage paths or policy contents. HTTP errors {error:{code,message}} status400 invalid request,401 unauthorized,404 absent,405 method,429 query capacity or503 not ready/unavailable. No events endpoint yet; UI must say unavailable. Unknown API routes never fall through to HTML.

Serve React built static assets same-origin from startup-loaded bounded file set (<=256 files, each<=4MiB,total<=16MiB), reject symlinks/traversal and unknown assets404. Default dashboard directory dashboard/web/dist, optional if API-only explicit directory absent? Default missing => API-only with clear startup warning; explicitly supplied invalid directory => startup error. CSP self scripts/styles, no framing/referrer, nosniff. No inline scripts, third-party assets or runtime CDN. Bound HTTP connections64, idle/request timeout10s, URI4096 bytes, headers16KiB, no waiting management requests beyond transport8. Graceful shared shutdown and fail if management bind/start fails.

## React UI contract (frontend owner dashboard/web/**)

React + TypeScript + Vite; lock dependencies. Match light design, desktop/sidebar/mobile layout, no mock runtime fallback. Same-origin API client; token entered in password form and kept only in React memory (not URL/storage/cookies). HTTP401 clears token and returns login. Logout clears all cached sensitive data. Poll visible active page every5s, prevent overlap, abort on route/logout/unmount; explicit loading/error/empty/disconnected/stale state. No fake uptime/resource/traffic/username/IP/latency data. Bounded history60 samples, show traffic rates only after two successful samples, handle server restart and negative deltas. API pagination/search filters and session details, subscriptions/retained metadata, actual listener and read-only startup limits/security. Hide or clearly label unsupported events/rules/resource metrics and mutations. No apparent save/disconnect/delete controls. React escaping for all broker strings. Unit tests for API/rate/reset/auth/error/paging logic and production build/typecheck.

## Ownership/dependencies and acceptance

1. Coordinator freezes contract, sets worktrees; owns bin/rmqtt/**, its Cargo.toml, Cargo.lock, root integration/docs.
2. Telemetry agent owns crates/mqtt-transport/** and crates/mqtt-session/** only (explicit extended scope for pure metadata projections); commit tested implementation. No root lock edits.
3. Frontend agent owns dashboard/web/** only; reads design but does not alter it. Independent branch/worktree, commit tests/build.
4. Quality agent owns tests/dashboard* only; independent blackbox HTTP+raw MQTT tests, no implementation changes, commit tests. Depends only on frozen CLI/API until integration.
5. Coordinator integrates telemetry then HTTP/CLI, frontend then quality; reviews all diffs, fixes through owning agents where possible. fmt, clippy --all-targets -- -D warnings, workspace tests, real MQTT311/5 QoS0/1/2 mutations observed via API, retained update/delete, subscriptions, graceful/abnormal disconnect and SQLite restart recovery; negative token/query/path/method tests. Real browser verify login/search/details/errors/responsive with live broker and save screenshot. No performance/deployment/complete dashboard admin claims without evidence.

### Frozen metadata traversal addendum

Telemetry ownership additionally includes `crates/mqtt-router/src/concurrent.rs` for a payload-free retained metadata visitor (topic, QoS, payload length and borrowed metadata). It must hold only the existing synchronous shard guard during each non-await callback and never clone payloads or change routing behavior. This avoids a second retained index and unbounded payload copying for management reads.
The visitor addendum also covers `crates/mqtt-router/src/retained.rs`, whose private retained records back ConcurrentRetained; add only the read-only metadata traversal needed by the wrapper, with no payload copies or altered routing semantics.

### Frozen recovered-negotiation clarification

Existing durable schema does not persist negotiated protocol or Receive Maximum.
For an offline session recovered at process startup, expose `protocol: "unknown"`
and `receive_maximum: null` until reconnect; SessionView.receive_maximum is therefore
`Option<u16>`. Never relabel the model's restore defaults as a previously negotiated
MQTT3.1.1 session. Known protocol filters exclude unknown; empty filter includes it.
No durable schema change is authorized. UI must render unknown/not available.

### Frozen static-secret containment

Reject startup if any loaded dashboard asset equals the trimmed administrator token
(including a token stored inside the asset root or a hard-link alias under an asset
extension). Never publish a management secret as an unauthenticated static asset.
Also reject any dashboard asset that aliases configured token, TLS certificate/key,
client CA, auth policy, or existing storage file (including hard links under a .js
name). Accept only known static build asset extensions; do not serve arbitrary
configuration/database files from a misconfigured asset directory.

### Frozen busy/error and inflight clarification

Expose public `mqtt_transport::management::ManagementError::{Busy,Unavailable}`
as the inner error of io::ErrorKind::WouldBlock. Implement Error+Display; HTTP
maps Busy to429 and Unavailable to503 without string parsing. `SessionView.inflight`
counts outbound QoS exchanges only, not incoming QoS2 records; label it accordingly.

### Frontend UI foundation — user amendment

Use Tailwind CSS (Vite integration) for layout/responsiveness and shadcn/ui source
components for reusable controls. Replace the large handwritten stylesheet rather
than wrapping it with another styling layer. Keep only theme tokens/base styles and
small genuinely custom chart/brand rules. Preserve frozen API, memory-only auth,
query cancellation, all real-data/unsupported states and tests. Add accessible
standard Button/Card/Input/Badge/Table/Sheet or Dialog/Tabs/NativeSelect primitives
as appropriate; retain same light visual direction. No backend/CSP weakening for UI
library convenience. Frontend owner continues isolated dashboard/web/** worktree.

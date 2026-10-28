# Frozen Phase2c — MQTT3.1.1 Retained and Will
Supersedes retained/Will boundaries of phase2b; other protocol guarantees remain.
lib.rs ONLY docs/module declarations/reexports. No new business code there.
Reuse existing protocol, Router, IncomingQos2/OutgoingQos, SessionEngine, SQLite
single-registry transaction/outboxes. MQTT5 and credentials remain unsupported.
Normative: OASIS3.1.1 sections3.1.2.5–7,3.3.1.3,3.8.4.

## Protocol (mqtt-protocol ownership)
Add public `Will {pub topic:String,pub payload:Vec<u8>,pub qos:u8,pub retain:bool}`
(Debug/Clone/PartialEq/Eq), root reexport. Packet::Connect adds `will:Option<Will>`.
Encode/decode CONNECT WillFlag/WillQoS/WillRetain, topic UTF8 then binary u16-length
Will payload after ClientID. Payload may contain invalid UTF8/NUL and be empty;
length<=65535. Topic nonempty/no wildcard, QoS0..2, no-Will implies zero QoS/retain
bits. Credentials still explicit UnsupportedFeature. Reserved flags and truncation
still fail. PUBLISH retain bit now accepted, represented and encoded exactly.
QoS0 DUP forbidden, IDs/remaining-length/UTF8 rules unchanged. Update own fixtures
with will:None; codecs don't define lifecycle or storage policy.

## Retained store (mqtt-router ownership)
Add public module retained, no new dependencies. Public types:
`RetainedMessage {pub topic:String,pub payload:Vec<u8>,pub qos:u8}` Clone/Debug/Eq.
`RetainedStore` Clone; `new(max_entries:usize,max_bytes:usize)->Result<Self,RetainedError>`;
`apply(&mut self,topic:String,payload:Vec<u8>,qos:u8)->Result<(),RetainedError>`;
`matching(&self,filter:&str,qos:u8)->Vec<RetainedMessage>`;
`entries(&self)->Vec<RetainedMessage>`; `len`, `is_empty`, `used_bytes` accessors.
Typed RetainedError Error/Display. apply validates topic/qos and checked logical
bytes(topic UTF8+payload); limit errors preserve old record. Empty payload deletes
rather than storing empty. Nonempty replaces exact topic. matching reuses current
filter/matches/system-topic rules, sorted deterministic topics, effective QoS=min
stored,qos; invalid filter/qos gives empty. Shared subscriptions still rejected.
Store is global, independent of client session lifetime. No new wildcard algorithm.

## Session model (mqtt-session ownership)
SessionConfig adds `max_retained:usize`. Engine owns RetainedStore configured with
max_retained/max_state_bytes, plus bounded Will records. Preserve public APIs;
`connect(id,clean)` delegates `connect_with_will(id,clean,None)`, `disconnect(id)`
delegates graceful close. Add:
`connect_with_will(&mut self,id:&str,clean:bool,will:Option<mqtt_protocol::Will>)->Result<ConnectResult,EngineError>`;
`disconnect_with_reason(&mut self,id:&str,graceful:bool)->Result<(),EngineError>`;
`recover_wills(&mut self)->Result<(),EngineError>`;
`retry_wills(&mut self)->Result<bool,EngineError>` true iff pending Will published.
No disk/socket IO. Existing clone/apply/snapshot/commit/install rollback contract.

On retained QoS0/1: atomically replace/delete retained record AND enqueue ordinary
live/outbox deliveries, before ACK if QoS1. QoS2 does this only on PUBREL, in same
transaction as incoming completion; repeats never update/reroute twice. RETAIN=0
never changes the retained store. Live established-subscription deliveries always
retain=false, including empty deletion publications. Clean reset/disconnect never
removes retained records. The retained flag on queued replay must survive snapshot
and outgoing QoS retransmission.

Handle SUBSCRIBE filters in order (3.8.4): each accepted filter, including identical
replacement, enqueues all matching retained records with retain=true and QoS=min
retained,granted for that filter. Multiple overlapping filters may each replay a
copy, as sequential subscription requests; ordinary live matching stays deduplicated.
Rejected filters have no replay. SUBACK before queued replay on wire. Full replay
queue aborts whole event with no success ACK (no installed partial subscriptions).

Will belongs to accepted connection, not persistent-session flag. Validate full
CONNECT and generated publication sizes. Successful connect stores its new Will
before CONNACK. Normal DISCONNECT clears only that connection Will before closing.
Unexpected EOF/error, malformed established packet, keepalive timeout, shutdown
and successful takeover publish old Will once, before installing replacement Will.
Takeover publication uses old subscription/lifetime state and stale cleanup is
ignored by transport generations. Closing clean session is removed before routing
its Will; persistent source goes offline and may itself queue its own QoS>0 Will.

Publication and removal of Will plus retained update/outboxes are atomic. If a
Will cannot publish due to capacity, keep a bounded pending record (do not drop or
partially fanout) while finalizing disconnection. retry_wills tries pending records
on proposed clones, installs only successful complete publications, defers capacity
errors but propagates invalid/corrupt data errors. Bound Will records by max_sessions
and serialized max_state_bytes. Reject same-ID new CONNECT if an older pending Will
cannot be drained (never overwrite it). retry after queue space is freed/events;
no busy loop when none can progress. Active wills survive SQLite process crash,
including clean-session source wills, and publish on startup before serving clients.
recover_wills marks all loaded active Wills pending then attempts publication once.
With no SQLite, all global state is volatile across process death.

## Snapshot/schema migration (session owner; storage opaque API unchanged)
SQLite container user_version/table remain1; JSON payload schema_version becomes2.
Explicitly accept valid old schema1, initialize retained/wills empty and old queued
retain=false; snapshot always emits2. Unknown versions/malformed records fail.
Schema2 requires envelope fields sessions,retained,wills (no silent missing globals).
Message adds retain:bool. Retained records {topic,payload,qos} with nonempty payload
and unique valid topics. Will records {client_id,pending,will:{topic,payload,qos,retain}}
unique IDs, valid full CONNECT/publication limits; clean source need not have a
persisted SessionRecord. Preserve v1 session/outgoing fields/stages/send order and
strict IDs. No silent serde-rename migration. Global retained/Will state is included
in both live byte bounds and persisted snapshot independent of clean sessions.
Refuse restored quotas/invalid records; count+byte bounds before mutation installs.

## Runtime (mqtt-transport owner)
BrokerConfig adds max_retained default10000, validated positive. Reuse existing
bounded blocking worker for BOTH volatile and SQLite modes, store is Option.
No SQLite path still rejects non-clean CONNECT as before; clean-session Retained/
Will work in volatile mode. Don't duplicate lifecycle in legacy router path; retire
unused live path and preserve meaningful regression tests against shared runtime.
Split implementation into responsibility modules if necessary; keep lib thin.
Worker startup loads/migrates/recover_wills/snapshot+commit before ready when SQLite
exists. No fallbacks on commit/schema error. Each mutation clones/applies, calls
retry_wills, snapshots/commits if persistent store then installs. Notifications on
Will/end/retry progress wake recipients; an empty unchanged poll doesn't commit or
notify. Poll that enqueues/removes a Will due to retry DOES commit even if its own
packet result is None. SQL failures stop service. Model capacity failure discards
proposal. Startup pending-capacity wills remain recorded, retried after progress.

Pass CONNECT Will to connect_with_will and retain generation gate for all operations.
DISCONNECT must submit a graceful worker end command and await commit before closing,
not merely set a local flag for late cleanup (takeover race). After its success,
final cleanup must not unexpectedly fire that Will. Unexpected cleanup submits
non-graceful end, stale generations ignored. New registration/cancel predecessor
only after successful connect+old-Will transaction. Shutdown/end persists queued/
pending Will and joins worker. Preserve fair reads, full-frame writer ownership,
explicit rejection flush, stale replay invalidation and bounded cancellation.
Retain old public BrokerConfig/serve APIs except additive field/CONNECT shape.
Update own Rust constructors with will:None and SessionConfig.max_retained.

## Coordinator CLI/docs
Add --max-retained N (default10000), corresponding limits/help/tests. Record schema2
migration, volatile/durable differences and failed/deferral semantics. Own root files.

## Independent quality (tests/** and CI owner)
Update old retained-unsupported expectations to malformed QoS/DUP errors without
weakening validation. Add retained_will_blackbox.py and Paho tests in BOTH modes:
retained update/replace/delete (empty live delivery but no replay), non-retained
non-mutation, +/#/$SYS matching, resubscribe replay/QoS/retain flags, retained QoS2
not visible before PUBREL, persistence independent clean sessions and restart,
retained Will update/delete. Will QoS0/1/2 binary payload, normal DISCONNECT silence,
EOF/RST, protocol error, keepalive timeout, same-ID takeover exactly once/old Will,
replacement's graceful clear and pre-CONNECT rejection no Will. Test pending Will
under full outbox then retry, active clean-source Will after SIGKILL/restart,
Will transaction+offline delivery, schema1 migration compatibility. Use ACKs/PING
or confirmed recipient output as barriers; sleeps only bounded silence/timeouts,
not commit proof. Existing QoS/persistence tests remain. CI includes new suites.

## Scheduling and write isolation
Freeze first. Wave1 Protocol + RetainedStore + Session Engine in independent owned
worktrees; model uses frozen APIs and cherry-picks dependency commits before tests.
Once Protocol finishes, Quality uses its concurrency slot. Transport starts from
integrated APIs/model after ready; coordinator owns CLI/docs, reviews and final
real-client verification. No conflicting file writers or business code in lib.rs.

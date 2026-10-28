# Frozen Phase2b contract v1
Supersedes persistence boundaries of Phase2a only. MQTT3.1.1 sessions; MQTT5,
retained, Will and auth remain unsupported. Extend/reuse existing codec, trie and
IncomingQos2/OutgoingQos/PacketIdentifiers; no independent second QoS implementation.

## Atomic durable record and outbox
SQLite stores one versioned bounded snapshot for this increment. It contains only
persistent (CleanSession=false) sessions: client ID, subscription grants, incoming
QoS2 pending encoded messages, outgoing QoS1/2 stages and IDs in original send
order, and bounded queued application messages. Runtime online flags, connection
generations and replay cursors are excluded. JSON envelope has schema_version=1;
unknown versions/corrupt records/invalid QoS, IDs, topics, filters or quota breaches
fail restore rather than resetting state. All restored sessions start offline.

The unit of commit covers the entire persistent registry. Receiving QoS1 routes
and enqueues durable targets before PUBACK. Receiving QoS2 commits pending message
before PUBREC. PUBREL atomically removes pending source state and enqueues durable
recipient outboxes before PUBCOMP; duplicate/unknown PUBREL never reroutes. A queue
or state limit failure rolls back the complete event and sends no success ACK.
Outgoing first PUBLISH is recorded in inflight before writing. PUBREC commits
AwaitComp before PUBREL; PUBACK/PUBCOMP remove inflight and free ID before next
allocation. Commit failure does not install proposed memory state. Disconnect
must not remove persistent sessions. CleanSession=true atomically discards any old
session and replies session_present=false; non-clean reconnect reports true iff a
persistent record already exists. DB transaction/outbox guarantees do not imply
application-side exactly-once processing or physical power-loss testing.

On reconnect/resume: pending outgoing QoS1/2 PUBLISH retransmits with original ID,
DUP=true, original send order; AwaitComp retransmits PUBREL (0x62), never PUBLISH.
Queued unsent messages get fresh IDs and DUP=false. If an outgoing ACK advances
the exchange before its local replay is queued for writing, discard that obsolete
pending PUBLISH/PUBREL; never enqueue PUBLISH after its successful PUBREC. Already
queued frames retain strict writer order before the ACK response. No active-connection retry timer.
Offline persistent subscriptions queue QoS1/2 only (QoS0 may be dropped offline).
Full persistent outbox aborts publisher event rather than silently acknowledging
and discarding it. Inbound/outbound namespaces remain separate. Clean sessions are
volatile even when the SQLite-enabled server path is used.

## mqtt-storage (owned storage crate)
Add `pub mod durable` with synchronous `pub struct SqliteStore`:
`open(path:&std::path::Path,max_snapshot_bytes:usize)->Result<Self,DurableError>`;
`load(&self)->Result<Option<Vec<u8>>,DurableError>`;
`commit(&mut self,snapshot:&[u8])->Result<(),DurableError>`.
DurableError implements Error/Display with typed IO/SQLite/limit/schema cases.
Use rusqlite0.41 bundled SQLite, WAL and synchronous=FULL; verify PRAGMAs.
Own a lifetime exclusive OS advisory broker lock on a fixed sidecar beside the
canonical database path (std File::try_lock or equivalent released-on-crash lock).
Never delete the sidecar: its existence is not the lock. This avoids interference
with SQLite locks on the DB file itself, as reproduced on Darwin. Reject a second
broker for the same canonical DB path; reject dangling symlinks before opening a
new DB, resolve existing symlinks to the shared canonical lock. Hard-link aliases/external DB renames while
running are unsupported. No stale create_new existence lockfile. Refuse unknown nonempty database schemas and unsupported
user_version; do not silently reset. Query blob length before allocating on load,
reject oversize before commit, preserve old committed value on any failure.
Session snapshot is opaque to storage. No Tokio dependency or async IO in storage.
Tests reopen, writer exclusion, unknown schemas, oversize rollback, SQL rollback
on real write failure if feasible. Preserve MemoryStorage and existing tests.

## mqtt-session (owned session crate)
Add `pub mod durable`; may add owned-crate deps protocol/router/serde/serde_json.
`pub struct SessionConfig {pub max_sessions:usize,pub max_subscriptions:usize,pub max_queued:usize,pub max_inflight:usize,pub max_packet_size:usize,pub max_state_bytes:usize}` derives Clone/Copy/Debug.
`pub struct SessionEngine` implements Clone; `pub struct ConnectResult {pub session_present:bool}`.
`new(config:SessionConfig)->Result<Self,EngineError>`;
`restore(config:SessionConfig,bytes:&[u8])->Result<Self,EngineError>`;
`snapshot(&self)->Result<Vec<u8>,EngineError>` (bounded schema1 JSON persistent records);
`connect(&mut self,client_id:&str,clean_session:bool)->Result<ConnectResult,EngineError>`;
`disconnect(&mut self,client_id:&str)->Result<(),EngineError>`;
`handle(&mut self,client_id:&str,packet:mqtt_protocol::Packet)->Result<Vec<mqtt_protocol::Packet>,EngineError>`;
`poll(&mut self,client_id:&str)->Result<Option<mqtt_protocol::Packet>,EngineError>`.
EngineError typed implements Error/Display. Runtime must clone engine, apply event,
validate/snapshot, commit, and only then replace current engine/send response. Thus
no partial method mutation is installed on failure. Public docs explain transaction
caller responsibility. Max_state_bytes must bound logical live state too, including
clean sessions, not just persisted subset. Client IDs/encoded packets bounded at
ingress. max_sessions includes online and offline records. Subscribe grants, QoS
routing use existing Router; rebuild index on restore/clone if needed. Extend
existing QoS primitives with snapshots/restoration/order support, not parallel FSMs.
Restore validates states/quotas and reserves allocated IDs; never reuse inflight ID.
Handle Subscribe/Unsubscribe/Publish/ACK/Ping; invalid directions error. Poll first
replays outstanding records after connect once, then starts queued messages when
window permits; QoS0 may be queued online. No data-plane socket IO or disk IO here.
Tests serialize/restore all ACK stages, atomic proposed PUBREL fanout, clean reset,
persistent disconnect/offline queue, ID lifetime, corrupt/versioned snapshot limits.

## mqtt-protocol
Allow clean_session=false CONNECT encode/decode, preserve client ID/flags checks.
No persistence policy in codec; legacy transport without DB may reject non-clean.
No other packet shape change. Update own obsolete persistent-rejection tests only.

## mqtt-transport
BrokerConfig adds `storage_path:Option<std::path::PathBuf>` defaultNone,
`max_sessions:usize` default1024, `max_state_bytes:usize` default64*1024*1024.
serve routes to new durable module when storage_path Some, otherwise preserves
existing live clean-only path. Add dependencies storage as needed. Dedicated
spawn_blocking worker owns SqliteStore + SessionEngine and bounded command channel;
clone/apply/snapshot/commit/install each serialized mutation before replying. Poll
must also commit before packet is sent. Startup validates DB before serving; corrupt
state is an error. No blocking disk IO on async workers. Commit/schema failures
must not fall back to memory-only success.
Network connection generation is checked in worker for every event, poll and
cleanup; successful takeover cancels old task, stale disconnect cannot remove new
session. Notifications wake connected recipients after committed handle/connect,
but not empty poll (no busy loop). CONNECT must await committed response before
CONNACK; bound outgoing buffers, reads, writes, keepalive, shutdown and work queue.
Avoid write/read starvation. On takeover/shutdown/timeout/error, terminate the dedicated socket writer rather
draining old data frames; persisted outboxes are replayed on reconnect. Only a
protocol-rejection CONNACK needs a bounded explicit flush before close. Graceful
shutdown joins network and worker. Preserve
legacy tests; add durable worker integration tests as needed.

## Coordinator executable
Add --storage-path PATH, --max-sessions N, --max-state-bytes N, pass config through.
DB parent directory must already exist; do not create arbitrary directory trees.
Storage optional for backward compatibility. Add *.sqlite*, *.db* to gitignore only
for data artifacts (do not hide source files generically); docs show local DB path.

## Independent quality
Own tests/** .github/workflows/**. Add persistence_blackbox.py launching broker with
--storage-path temporary DB. Actual TCP clients + SIGKILL/restart tests: persistent
session-present/subscriptions, clean reset, bounded offline QoS1/2, outgoing QoS1
and QoS2 AwaitRec DUP+sameID, AwaitComp PUBREL-only, incoming QoS2 restart before
PUBREL, committed PUBREL no duplicate onward delivery after lost PUBCOMP/restart.
Use observable protocol ACKs as durability barriers; no arbitrary sleeps as commit
proof. Check restored session generation/takeover, malformed DB fails startup, and
uncommitted SQL transaction killed -> prior record retained where feasible. Paho
persistent reconnect interoperability. CI executes new suite plus existing suites.
No production fault injection switches; no physical power-loss claims.

## Dependencies / scheduling
Wave1 storage + session + protocol in separate branches/worktrees. Protocol finishes
first allowing independent quality work; transport starts from integrated dependency
commits after session+storage ready. Coordinator owns CLI/docs, final review and
all suites. API changes require updating this contract before dependent edits.
Whole snapshot cloning/serialization is intentionally simple and costly; benchmark,
row-level migrations, disk quota/compaction and cross-process readers remain future.

Contract clarification 2026-10-28: storage tests reproduced DatabaseBusy from OS-locking the SQLite DB inode; use a permanent canonical-path sidecar advisory lock instead. API and commit semantics unchanged.

## Schema1 concrete representation (implementation freeze)
SQLite application_id=0x524d5154, user_version=1. Exactly one application table:
`broker_snapshot(id INTEGER PRIMARY KEY CHECK(id=1), schema_version INTEGER NOT NULL CHECK(schema_version=1), data BLOB NOT NULL)`.
The store's canonical CREATE TABLE definition is checked on reopen. The optional
singleton row has id1/schema_version1 and a bounded UTF8 JSON BLOB.

JSON envelope fields: `schema_version:1`, `sessions:SessionRecord[]`.
Each SessionRecord has `client_id:string`, `subscriptions:[string,u8][]`,
`incoming:[u16,u8[]][]`, `outgoing:OutgoingRecord[]`, `queued:Message[]`.
Messages have `topic:string`, `payload:u8[]`, `qos:u8`.
OutgoingRecord has `packet_id:u16`, `stage` using serde's externally tagged values:
`{"Ack":[wire bytes]}` waits PUBACK; `{"Receipt":[wire bytes]}` waits PUBREC;
`"Comp"` waits PUBCOMP with no PUBLISH bytes. Outgoing array order is original send
order, not sorted Packet ID order. Wire bytes are complete protocol-validated MQTT
PUBLISH with actual assigned nonzero ID and DUP=false; incoming bytes hold original
QoS2 PUBLISH. JSON u8 arrays contain integers0..255. Unknown fields/duplicates or
invalid values fail restoration where model invariants require uniqueness. Any
future incompatible record change requires explicit schema migration/versioning;
no serde/private-enum rename may silently reinterpret existing version1 data.

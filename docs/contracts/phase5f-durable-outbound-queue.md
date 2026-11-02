# Phase5f — Generic durable outbound segmented queue (frozen 2026-11-02)

Supersedes volatile-delivery and generation-cancellation disposal in phase5c/d/e.
Exact client FIFO, generic Sink boundary, concurrency8/default1..16/global16 and
lib.rs structure remain. No Kafka/PostgreSQL/Redis adapters, SQLite message rows,
MQTT ACK/session transaction changes or distributed storage in scope.

## Guarantee and lifecycle

ALL outbound types use the same generic journal before invoking Sink::send.
Journal stores opaque serialized delivery bodies and explicit source Client ID,
outbound ID and immutable binding revision. It knows nothing about HTTP/URLs/SQL.
Admission before durable append remains bounded/nonblocking at MQTT observer;
MQTT ACK does NOT imply journal acceptance. Only successful append+sync is durable
acceptance. After that, unfinished records survive graceful shutdown/process crash;
local append storage failure stops dispatch and exposes failure, never sends an
uncommitted record. Durability relies on filesystem/device honoring sync calls;
SIGKILL tests do not simulate physical power loss.

At least once for recoverable delivery until success or explicit terminal failure.
An HTTP success followed by crash before durable completion may be sent again with
same payload/event_id/delivery_id. Begin-attempt marker is synced before network;
recovery of an uncertain attempt retries it without counting it as a known failure.
Known retryable failures persist failure count and absolute next retry timestamp.
Crash during backoff retains delay; crash must not reset finite retry exhaustion.
Permanent error or exhausted policy persists terminal completion before advancing
client lane. Final failure intentionally removes the record and increments failed;
this phase does not add dead-letter UI/replay. No completed confirmation may be
reported as durable before sync; unknown commit outcome fails closed and recovery
replays whatever actually committed. Cancellation/timeout never implicitly ACKs.

FIFO key remains(exactoutbound,exactClientID), now across restart too. One batch per
client through attempt/backoff/terminal commit; batch homogeneous, bounded by Sink
and delivery policy. Different clients progress even during another client's retry.
No global FIFO. A blocked client's disk backlog must not fill the entire memory
window and prevent other admitted clients' head messages from being scheduled.
Retries and terminal state are generic, not in Webhook adapter. Adapter pool reused.

## Configuration safety

Default journal directory: append '.outbound-queue' to full rules SQLite path.
CLI may override directory and positive segment/physical-byte/pending-record limits.
SQLite stores rules/config ONLY. Bind each outbound to an opaque canonical descriptor
(endpoint + delivery policy; exclude displayname/enabled) and a persisted revision.
Descriptors are stored once per binding (may contain credentials; privatefiles).
Queue accepts appends only for current descriptor revision, rejecting stale old
config dispatch. Pending bindings cannot be removed or changed: config PUT returns
Busy/409 before SQLite commit. Renaming, rule changes, disable/enable allowed;
disabling PAUSES claims but retains backlog. All claimed jobs cancel/join before a
new generation begins, no overlapping sameclient sends across config replacement.
Persist new empty binding before SQLite save; if crash between stores, old config
can rebind an EMPTY binding; nonempty mismatch fails startup closed, no rerouting.
All binding updates are atomic as one journal transaction. Cannot silently discard
pending records on deletion, errors, config replacement, shutdown or restart.

## Resource/security/storage requirements

New crate mqtt-egress-log owns sync-only disk IO, no Sink/Tokio/HTTP/SQLite dependency.
Egress actor runs disk operations on bounded blocking worker/channel; never fsync
in async tasks or observer. Group append when commands available, finite command
count/bytes. Message payloads read from segments only for bounded delivery windows;
no full payload reload into RAM at startup, no full-state clone per mutation or
per-send scan of all payloads. Metadata indexed for exact perclient heads. All
record/count/length/file limits checked before allocation and mutations; bound
metadata, channels, task/lane/claim counts, resident bodies and aggregate memory.
Defaults segment1MiB, physical256MiB, pending100000. Valid ranges:
segment4096..64MiB,max_bytes>=4*segment and<=1TiB,max_records1..1000000.
Encoded live queue capacity is at most half physical max_bytes, reserving state
progress; atomic compaction may transiently use up to2*max_bytes plus small control
files. Durable runtime batch count additionally capped64 (configured count remains
an upper bound), matching atomic journal state-operation bounds. Max delivery payload1MiB;
client<=65535B,outbound<=64B,descriptor<=64KiB,at most32bindings. Journal group max
64records/8MiB payload; metadata operations likewise finite. Full queue rejectsNEW
appends, increments admission drops/errors, retains old records. Reserve enough
physical headroom for retry/terminal state and reclamation; full payload capacity
must not prevent finishing queued work. CLI options validation guards overflow.

Versioned binary/framed append records with bounded lengths and checksum, monotonic
record IDs. Segmented files; exclusive lifetime ownership lock, private dir/files,
reject symlink and foreign/malformed files/unsupportedformat. New files/rotations,
atomic checkpoints/manifests and deletions must obey file+directory sync ordering.
Recover complete valid prefix on incomplete latesttail write, truncate only that
incomplete tail; checksum corruption/invalid fullframe/sealedsegment damage fails
closed rather than silently deleting accepted data. Checkpoint/reclaim only after
state durable; never delete live payload segments. Completed segments reclaimed so
sustained sends don't inevitably fill disk; compaction/checkpoint protocol must be
crash-safe. Disk accounting includes state records/checkpoints/scratch; documented
bounded compaction headroom allowed, no hidden unbounded disk use. Monotonic IDs,
bindings, pending content/failure/deadline survive checkpoint/reclamation. Error
poisons owner after uncertain writes; no continuing with stale in-memory index.

## Frozen storage API (sync, typed errors, opaque descriptors/payloads)

QueueOptions { directory:PathBuf, segment_bytes:u64, max_bytes:u64, max_records:usize }
QueueOptions::new(directory) supplies defaults; validate onopen.
QueueError enum includes Io,Corrupt,Locked,InvalidOptions,Full,BindingConflict,StaleBinding.
Binding { outbound_id:String, descriptor:Vec<u8> }
NewRecord { outbound_id:String, revision:u64, client_id:String, payload:Vec<u8> }
RecordMeta { id:u64,outbound_id:String,revision:u64,client_id:String,
             failures:u32,not_before_ms:u64,inflight:bool,payload_len:usize }
Record { meta:RecordMeta,payload:Vec<u8> }
QueueStats { pending_records:usize,pending_bytes:u64,disk_bytes:u64,
             max_bytes:u64,recovered_records:usize }
Completion enum { Delivered,Failed }
Journal::open(options)->Result<Self,QueueError>
Journal::bind_all(&[Binding])->Result<BTreeMap<String,u64>,QueueError>
Journal::append_batch(Vec<NewRecord>)->Result<Vec<u64>,QueueError>
Journal::heads(outbound_id:&str,after:Option<u64>,limit:usize)->Vec<RecordMeta>
  Returns perclient EARLIESTpendingrecord only, increasingheadID aftercursor,
  boundedlimit; caller wraps cursor at end forfairness. No claims in disk layer.
Journal::batch_metas(head_id:u64,limit:usize,max_payload_bytes:usize)->Result<Vec<RecordMeta>,QueueError>
  Preview same prefix/state criteria as read_batch, metadata only/no payload IO.
  Coordinator-approved addition before implementation: reserve exact planned body
  and identity bytes, then read only that planned count/byte window. Avoid both
  1MiB reservation for tiny messages and heuristic truncation of valid batches.
Journal::read_batch(head_id:u64,limit:usize,max_payload_bytes:usize)->Result<Vec<Record>,QueueError>
  Sameclient prefix fromhead, do not combine differing failure/deadline/inflightstate.
Journal::begin_attempt(ids:&[u64])->Result<(),QueueError>
Journal::retry(ids:&[u64],not_before_ms:u64)->Result<(),QueueError>
  Records one known failure, increments failures,clearsinflight,setsabsolutedeadline.
Journal::complete(ids:&[u64],Completion)->Result<(),QueueError>
Journal::stats()->QueueStats
Journal::pending_for(outbound_id:&str)->usize
Journal::checkpoint()->Result<(),QueueError>
All mutators atomic per call, durable onOk, checked referenced IDs/revisions; no
partial batch acceptance reported as success. Begin attempts keep failure count;
recovery inflight allows repeat. Explicitcheckpoint provides recovery/GC tests;
engine may checkpoint automatically. Debug/errors never expose payload/secrets.
Interface changes require coordinator agreement before implementation changes.

## Runtime/admin/UI boundary

Runtime::open(path) retains publicpath; new Runtime::open_with_queue(path,QueueOptions)
forCLIlimits, reexport QueueOptions from mqtt-egress. RuntimeError stays source
compatible. Journal errors mapped to Storage/Busy as appropriate, stopfailclosed on
IO/corruption. Add Status.durable_queue:QueueStats; existing queued_events/bytes
remain memory reservations, counters reset on restart. mode best_effort remains
admission classification, not a durability denial. Expose pending_for in existing
outbound queued count or additive durable_queued field, document exactunit.
Dashboard uses durable pending totals for backlog when present, labels normal
productterms; explain saved messages resume but pre-admission overload may lose.
No credentials/payload readAPI. Queue limits in CLI, not peradapter UI.

## Ownership/dependencies/acceptance

Storageagent owns crates/mqtt-egress-log/** in independentworktree. Runtimeagent
owns crates/mqtt-egress/**; mayaddpathdependency in itsCargo.toml, no rootedit.
Qualityagent owns tests/**,.github/workflows/**. Coordinator owns docs,bin/rmqtt,
Cargo.lock/workspace and small Dashboardstatusintegration for thisphase.
Freeze -> parallel logengine + runtimeagainstAPI + independentREDtests -> integrate
engine/runtime -> realReleaseHTTP + strictworkspacechecks -> review/fix -> docs.

Unit/faulttests: framedlength/checksum/partialtail/sealedcorruption/ownership/symlink;
atomicgroup,limitsandfull->complete->reclaimprogress,checkpoint/reopen;
retrycount/deadline,inflightuncertainty,arbitraryopaque(nonHTTP)payload/typeindependence.
Integration: diskbacklog>bounedRAM,shutdown/reopen,SIGKILLheldsend,before/afterremote
successwithstableIDs,backoffrestart,exhaustionrestart,terminalcontinuation;
sameclientFIFOwithanotherclientprogressbefore/afterrecovery;disable/enablepause;
409pendingtargetchange/removal;configreplacementdoesnotdrop;diskfailurefailclosed.
Keepoldordering/cap/pool/batch/canceltests but supersede assertions that demanded
pending records disappear on replacement. Run fmt,clippy -Dwarnings,workspace tests,
frontend/build,Release realHTTP. Report exactresults, not inventedperformance.

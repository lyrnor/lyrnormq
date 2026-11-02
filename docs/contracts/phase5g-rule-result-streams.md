# Phase5g — Shared rule-result streams and independent consumers (frozen 2026-11-02)

Supersedes per-outbound payload copies in phase5f and cross-rule serialization in
phase5e. Preserve phase5f durability/fsync, bounds, opaque payloads, checksums,
blocking owner, checkpoint/manifest safety and no MQTT ACK coupling. Same one
physical journal directory/generations/segments; logical stream key is rule ID.
No original publication payload is stored unless explicitly selected by rule SQL.

## Data and delivery semantics

Evaluate each matching rule once. Persist ONE rule-result body plus ALL eligible
outbound consumer references in ONE durable transaction. Different rule outputs
remain different records even if same event/client/outbound; do not dedup across
rules. One result fanout to X/Y stores body once, independent consumer state X/Y.
Only currently enabled bound outbounds are eligible, preserving previous admission
semantics; disabled outputs retain/pause already accepted backlog but gain no new
references. No eligible consumers means no result is appended. Rule/routing changes
apply to new results only; existing references are immutable and drain even if rule
is edited/deleted. Adding a destination never retroactively subscribes old results.

Each consumer owns delivery ID, endpoint binding revision, known failure count,
absolute retry time and uncertain in-flight marker. FIFO key is
(outbound ID, stream/rule ID, exact Client ID). Different rules never mix in one
batch and may progress independently for the same client/outbound. Retry blocks
only this consumer lane, terminal failure continues its next item. Different
outbounds consuming the same result are independent. Per-outbound attempt1..16,
default8 and runtimeglobal16 remain shared across streams, not multiplied perrule.

Result payload remains until ALL snapshotted consumers durably finish(success or
terminalfailure). One consumer completion removes only its reference; no early
payload deletion. Reclaim/checkpoint removes fully unreferenced results. Concurrent
completion tracked per reference, not a single maximum offset; continuous cursors
may be derived but cannot skip holes. IDs/payload remain stable on retry/restart.

## Generic log API (additive, before implementation)

Keep existing public legacy NewRecord/RecordMeta/Record and old Journal methods
for v1 decoding and source compatibility; runtime production MUST use new APIs.
Generic engine calls logical key stream_id, never parses SQL/HTTP/rule JSON.

NewConsumer { outbound_id:String, revision:u64, delivery_id:String }
NewResult { stream_id:String, client_id:String, payload:Vec<u8>, consumers:Vec<NewConsumer> }
ResultIds { result_id:u64, consumer_ids:Vec<u64> }
ConsumerMeta { id:u64, result_id:u64, outbound_id:String, revision:u64,
               stream_id:String,client_id:String,delivery_id:String,
               failures:u32,not_before_ms:u64,inflight:bool,payload_len:usize }
ConsumerRecord { meta:ConsumerMeta,payload:Vec<u8> }
LegacyResult { stream_id:String,result_key:String,payload:Vec<u8>,delivery_id:String }
All public identifier/type import paths remain; lib.rs exports/modules/docs only.

Journal::append_results(Vec<NewResult>)->Result<Vec<ResultIds>,QueueError>
Journal::consumer_heads(outbound:&str,after:Option<u64>,limit:usize)->Vec<ConsumerMeta>
Journal::consumer_metas(head:u64,limit:usize,max_payload_bytes:usize)->Result<Vec<ConsumerMeta>,QueueError>
Journal::read_consumers(head:u64,limit:usize,max_payload_bytes:usize)->Result<Vec<ConsumerRecord>,QueueError>
Journal::begin_consumers(ids:&[u64])->Result<(),QueueError>
Journal::retry_consumers(ids:&[u64],not_before_ms:u64)->Result<(),QueueError>
Journal::complete_consumers(ids:&[u64],Completion)->Result<(),QueueError>
Journal::migrate_legacy<F>(&mut self,resolver:F)->Result<(),QueueError>
  where F: FnMut(&Record)->Result<LegacyResult,QueueError>
Consumer methods use consumer IDs and same-stream/client prefixes; shared bodies
are indexed separately. pending_for(outbound) counts unfinished consumers including
legacy references. Binding protection includes both legacy and shared references.
QueueStats.pending_records/recovered_records continue DELIVERY counts; add
pending_results for unique live result bodies(including one per legacy entry until
migration). pending_bytes counts unique live stored bodies, not fanout copies.
Existing per-outbound durable_queued continues delivery-reference count. Distinguish
these units in docs. Do not add payload/credential read endpoints.

Maximum per result32 distinct outbound references; IDs revision valid/current;
delivery_id nonempty<=256bytes,stream_id nonempty<=64bytes,client<=65535bytes.
64results/8MiBbody per append group,<=2048consumerrefs; mutationgroups<=64consumerIDs.
max_records applies to total unfinished consumer refs (not only body count).
Validate entire group/capacity before writes; fanout atomic, never partialsuccess.
Bound metadata decode arrays, ID/string lengths, channels, plans and body windows.
Account actual consumer metadata and shared bodies in live-byte quota. Reuse shared
client/stream identity for persistent indexes where possible; no historical key leak.
No payload-wide scans/clones per mutation. Cached status and ready-head indexes
remain bounded. Preserve noidle-poll/no read-notify-feedback and capacity-wake fixes.

## Wire materialization and runtime

Stored rule result contains event_id,rule_id,topic,client_id,qos,retain,timestamp_ms,
data, but NO per-outbound delivery_id. Each consumer stores the old stable scheme
(event_id:rule_id:outbound_id) once in its metadata. Runtime materializes the current
WireEvent envelope with that delivery ID at send time, preserving public Webhook
JSON shape, reason/retry behavior and Sink trait/API. Common runtime codec is
adapter-independent; no HTTP logic in Journal. Generic opaque Sink tests use an
explicit identity codec, never silent parse-error fallback in production.

Use bounded RawValue/borrowed result decode or equivalent to avoid full JSON Value
heap expansion. Exact encoded-body length planning includes escaped delivery ID,
separators and envelope. Reserve both stored-read and materialized-send buffers
BEFORE allocation/IO, keep total memory and metadata limits. Parse/materialization
failure of an accepted rule result fails closed with references retained. New
append Admission reserves body once plus bounded per-consumer metadata; count
precommit rejection/drop per consumer and existing memory counter units accurately.
Actor groups AppendResults/Begin/compatibleComplete, responds only after sync.
Backoff and terminal commits stay consumer-specific; old generation fully joined
before new claims, and memory-capacity waits must not spin on their own releases.

## On-disk version and v1 upgrade

New frames write version2; parser reads existing header/bodyCRC-protected version1
and newversion2 with explicit bounded variants. CURRENT manifest staysatomic.
Unsupported futureversions failclosed; oldbinary mustrejectversion2 (no downgrade).
Do not silently abandon or reinterpret old pending deliveries.

Runtime startup calls bind_all and migrate_legacy BEFORE claiming/enqueueing new
work. Resolver in mqtt-egress validates old WireEvent, extracts rule_id asstream,
event_id asresult_key, removes perconsumer delivery_id from storedbody and returns
it separately. Client ID must match oldmetadata, preserve all publicevent fields,
data and delivery IDs; preserve knownfailures/deadlines/inflight and targetrevision.
Nonparseable/ambiguous/mismatched oldrecords failclosed, oldfilesunchanged.

Migration groups by(stream_id,result_key,client_id), coalesces ONLY exact equal
transformed bodies, unions stillpending distinctoutboundrefs; no references added
for destinations that completed beforeupgrade. Conflicting body/duplicateconsumer
identity failsclosed rather than guessing. Preservelegacy consumerIDs, allocate new
bodyIDs beyondoldhighwater; monotonicnewIDs. Resultrefs/payload/checksum migration
streams bounded records and holds metadata only, no all-payload preload. Compare
candidatebody equality exactly, not CRC alone. Atomic newgeneration build/sync/
CURRENT publication/oldGC; crashes choose complete old or new state. Support mixed
legacy+shared stores conservatively without changing existing sharedrecords.
If no legacy pendingrecords, migration is no-op, not fullrewrite everystartup.
Opaque log layer migration callback keeps engine independent of ruleJSON. Legacy
APIs are compatibility only and must not bypass bodysharing in production.

## Ownership/dependency plan

Logagent owns crates/mqtt-egress-log/** in its independentworktree/branch. Runtime
agent owns crates/mqtt-egress/** (serde_json raw_value feature allowed), no Sink
adapter/corechanges expected. Quality owns tests/**,benches/**,.github/workflows/**.
Coordinator owns docs/workspace/Cargo.lock and optionalsmall Dashboardstattyping.
Freeze -> parallel additive logmodel/migration, runtimeagainstAPI, independentRED
multi-rule tests/legacyfixtures -> integrate -> actualReleaseacceptance -> review.
API changes require coordinator agreement BEFORE affected implementations change.

## Acceptance

Unit: onebody/two refs, completingX retains Ybody/retry, independentknownfailure/
deadline/inflight, stream-isolated heads/batching, fullfanout atomicreject, bodybyte
accounting, refcountGC/checkpoint/reopen/corruption/faults, v1atomicsparsemigration,
legacyID/states/bindingpreservation and conflictingbodyfailclosed.

Real MQTT->HTTP: two different SELECT results samepublication/client, sharedX plus
A-onlyY/B-onlyZ; verify resultdata correct perrule, unique bodymarker once ondisk,
pending_results vsconsumercounts; hold/retry A/X while B/X and A/Y progress; same
streamClientFIFO, crossrule/crossclient concurrency, no mixedrulebatches; onefailed
consumerdoesn'tdropanother'sbody; restart/SIGKILL andexactstableIDs; delete/editrule
whilepending retains frozenoutputs; pendingtargetbinding409 preserved. v1fixtures
with duplicateperoutbound body, partialcompletedfanout andfailure/deadline recover
once withno lost/extra consumers; malformedlegacyfailclosedfilespreserved. Adaptold
crossrule serialization test (superseded), keep all caps/pool/legacywire assertions.
Update standalone benchmark to sharedresult/consumerAPI (singleconsumer compatible
metrics); smallsmoke only, priorperformance numbers remain historical, no newclaims.
Run fmt,strictclippy,workspace/frontendtests,Release and independentblackbox.

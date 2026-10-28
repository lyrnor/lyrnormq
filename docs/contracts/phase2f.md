# Phase2f frozen Message Expiry / Will Delay contract
Normative basis OASIS MQTT5 OS sections3.1.3.2,3.3.2.3.3,4.3.3 and4.4.
Extend the existing Clock, SessionEngine, canonical QoS FSM/outbox, retained store
and transactional worker. Keep every lib.rs docs/mod/reexports only. No new codec,
storage backend, auth or competing broker runtime. Full MQTT5 conformance not claimed.

## Scheduling and exclusive file ownership
First create a compilable schema5 foundation in ONE isolated Session worktree.
That specialist temporarily owns mqtt-session/**, updates strict records/migrations,
new deadline fields/sidecars and existing constructors/tests, preserving current
accepted feature behavior (MessageExpiry/nonzeroDelay still rejected at this stage).
Commit tested foundation before feature writers start. Quality can independently
write tests in its own worktree during this prerequisite; Will specialist initially
performs read-only normative review. After integration create separate feature worktrees:
- Message specialist owns mqtt-session/src/durable.rs, durable/{mqtt5.rs,expiry.rs,
  retention.rs,authorization.rs,temporal.rs,records.rs,records/**,message_expiry_tests.rs}
  and its Cargo.toml/lib.rs if required. No lifecycle.rs edits.
- Will specialist owns durable/lifecycle.rs, durable/will_delay_tests.rs ONLY. Test
  module declaration lives in lifecycle.rs. No shared record/core/timer edits.
- Quality owns tests/**/.github/workflows/**. Coordinator owns docs/root lock.
- Transport specialist later owns mqtt-transport/** when real model commits exist.
Any required cross-file edit goes through coordinator and designated owner, never
parallel writes in shared checkout. Feature worktrees both start from real foundation;
cherry-pick dependency commits for tests, no substitute stubs. Commit only owned files.

## Frozen schema5 foundation / internal names
SQLite opaque container stays1. Business JSON becomes5. Preserve old owners,
expiry deadlines, QoS stages/order/IDs and properties via explicit1/2/3/4 decoders.
Every new nullable field is REQUIRED using a required-nullable deserializer (never
Serde Option missing->None). Unknown/malformed/unsupported legacy records fail closed
before timer pruning. Schema4 must still require its session expires_at field.

Current queued Message and RetainedRecord add expires_at:Option<u64> (Unix millis).
Incoming entries become(id,canonical311_bytes,Properties,Option<u64>) quadruples.
OutgoingRecord adds expires_at:Option<u64>,started:bool. Comp requires started=true,
empty properties and no deadline. WillRecord adds delay_interval:u32,due_at:Option<u64>;
keep pending bool. Active is pending=false,due_at=None. Scheduled is pending=true,
due_at=Some; capacity-blocked due intents retain original deadline. WillData properties
retain relative MessageExpiryInterval but strip WillDelayInterval into delay_interval.
No absolute Message deadline in WillData before publication. Legacy4 has no supported
MessageExpiry or nonzeroWillDelay; reject such old metadata rather than reinterpret it.
Legacy outgoing is conservatively started=true (it may have been transmitted), all
legacy message deadlines None; old Will delay0, active dueNone, pending dueSome(0).

Runtime Session adds incoming_expiry:BTreeMap<NonZeroU16,u64>,
outgoing_expiry:BTreeMap<NonZeroU16,u64>,outgoing_started:BTreeSet<NonZeroU16>;
engine retained_expiry:BTreeMap<String,u64>. Only timed entries need map slots.
Retire sidecars with IDs/messages/retained removal, no orphan maps or quota bypass.
Deadline/property consistency validated: normal queued/incoming/retained timed records
require MessageExpiryInterval iff deadline Some; outgoing payload same, Comp none.
Legacy migration does not invent TTL. Metadata and timestamps count in state limits.

Bootstrap supplies temporal helpers as pub(super) in durable/temporal.rs:
expiry_deadline(properties:&Properties,now:u64)->Option<u64> (saturating now+seconds*1000),
is_expired(deadline:Option<u64>,now:u64)->bool (Some<=now),
remaining_properties(properties:&Properties,deadline:Option<u64>,now:u64)->Properties
(replace MessageExpiry value with ceil(remaining_millis/1000), capped at original).
Use Clock throughout; absence infinite, MessageExpiry UINT_MAX is finite, zero immediate.
Remaining interval never grows on repeated poll/restart or backward clock movement.

## Message Expiry semantics
Accept valid MessageExpiryInterval on MQTT5 PUBLISH and Will properties, keep existing
codec duplicate/context checks. Compute deadline ON FIRST accepted PUBLISH receipt;
inbound QoS2 duplicate ID never replaces payload/properties/deadline. Pending QoS2
still completes PUBREC/PUBREL/PUBCOMP after expiry, but does not route expired payload.
Keep pending original until PUBREL/session end (do not free its ID on TTL alone).

Queued/offline and retained copies share the original absolute deadline. Replay via
new subscriptions inherits it, never restarts lifetime. Before onward start, expired
copies are removed atomically, freeing queue/retained capacity and unstarted IDs.
A retained expired replacement (includingTTL0) removes the prior retained value;
empty retained publication still deletes it. Publication application point is receipt
forQoS0/1 andPUBREL forQoS2 as existing contract; expiry never revives an older value.
Expired nonretained payload causes no fanout. Continue source ACK sequence normally.

Outgoing packets to5 carry remaining MessageExpiry, repeated UserProperty order and
other metadata unchanged. To311 strip properties only on wire but enforce internal
TTL before first delivery; do not extend deadlines during version bridging/restart.
Once delivery has STARTED, retainQoS1/2 state through ACK/retry even afterTTL (especially
MQTT-4.3.3-7/13). Retries may carry interval0; never resend PUBLISH afterPUBREC.
ACL revocation policy remains separate and may retire forbidden data as before.

## First-write boundary and public Session APIs
The model must not treat an entire slow writer queue as already-started delivery.
Add in durable module:
Delivery {pub frame:Frame,pub expires_at:Option<u64>,pub ticket:Option<u64>} Clone/Debug.
DeliveryPollOutcome {pub delivery:Option<Delivery>,pub changed:bool} Debug.
poll_delivery_with_changes(&mut self,id:&str)->Result<DeliveryPollOutcome,EngineError>:
reserve ordinary outbox/replay+quota using shared existing code, but new QoS outgoing
is started=false. Runtime per-ID ticket counters/maps are bounded by pending IDs;
not persisted. QoS0 has no ticket; its Delivery carries frame/deadline outside model.
begin_delivery(&mut self,id:&str,delivery:Delivery)->Result<FramePollOutcome,EngineError>:
caller checks connection generation first. Check ticket matches current reservation;
stale retired/reused ID returns no frame. If not started and expired, retire exchange,
ID/order/replay/properties/expiry/ticket/quota together and return changed=true.
Otherwise compute remaining properties at this instant, validate size/ACL, persist
started=true before returning first QoS1/2 frame. Started retries are not TTL-pruned.
QoS0 drops if expired before write, no durable handshake state. All mutations remain
proposal/snapshot/commit/install before transmission. The unavoidable crash gap
between committed start-intent and socket write is conservatively treated as possibly
sent, preservingQoS; document this boundary rather than claiming atomic SQL+TCP.

Existing packet/Frame polling APIs remain wrappers for trusted immediate-send callers:
perform shared prepare+begin synchronously so existing direct callers retain prior
semantics. Production network must use two-step Delivery API and invoke begin only
immediately before actual write. No duplicate QoS FSM. Recompute property remaining
at begin, not merely at queue insertion. Poll/Begin(None) changes force commit+notify.

Message specialist owns expire_messages()->Result<bool,EngineError>. Existing
expire_sessions() becomes shared expire_sessions+expire_messages operation and
keeps bool changed; preserve validation-before-prune and no online Session expiry.
next_expiry_deadline() returns earliest actionable session/queued/retained/unstarted
outgoing/delayedWill deadline. Exclude started QoS and incoming pending deadlines so
expired protocol state cannot busy-loop the timer. Keep no-deadline Tick cheap.
For a pending Will whose due_at is already past but publication is capacity blocked,
do not repeatedly wake only for that old due time: ordinary progress retries it;
other future timers still wake. Will specialist exposes next_will_deadline()->Option<u64>
for future scheduled intents; Message timer helper incorporates it. retry_wills always
checks due independently when invoked by progress/startup. Deterministic Clock tests.

## Will Delay lifecycle — Will specialist
Accept nonzero WillDelayInterval only for MQTT5; default0 preserves311 behavior.
Clock + persisted due_at schedule unexpected EOF/error/timeout/shutdown and DISCONNECT04
at min(closeNow+delay,offline Session expiry deadline). Expiry0 means immediate,
UINT_MAX Session expiry unbounded. DISCONNECT04 respects delay (not forced immediate).
A normal DISCONNECT clears the active Will as before, preserving an older already
capacity-deferred immediate intent if a no-new-Will connection had resumed to drain.

Same owner, CleanStart=false, SAME surviving session resumed before due cancels its
scheduled delayed Will, atomically before replacement Will registration. Active takeover
with old positive Session expiry and positiveDelay also cancels oldWill. Delay0 sends
oldWill on close; CleanStart=true or oldSessionExpiry0 ends oldsession and forces
oldWill immediately regardlessDelay (OASIS3.1.3.2 note). At/after due, reconnect does
not cancel intent; expiry/end wins. Preserve ownership/admission-before-takeover guards
and stale generation cleanup. Failed CONNECT cannot cancel/publish/replace old intent.

CleanStart=true ends original session before new creation; existing delayed intent
becomes due now. If capacity defers it, existing one-intent-per-ID admission safeguards
remain (never overwrite). No-new-Will resume may drain an older already-due pending
self-Will as before. Explicitly distinguish cancellable waiting intent from due/
capacity-deferred intent. Do not cancel on same ClientId when original session expired.

recover_wills(): active-at-crash records get scheduled from restart-now conservatively,
capped by restored session deadline (or immediate for missing/expiry0 source session).
Already scheduled due_at remains unchanged across downtime/restarts. Overdue scheduled
Wills publish before ready when capacity permits. Commit restored schedule once;
never reset known deadline each boot. Valid legacy delay0 Wills still recover promptly.
Will MessageExpiry starts at successful publication attempt, not CONNECT/close/due time;
failed fanout proposal must not start/consume its TTL. Remove intent and install full
retained/outbox effects atomically only on success, or preserve scheduled intent.
Use temporal::expiry_deadline when constructing publication Message, remove WillDelay
property before forwarding, preserve other metadata. Recheck current ACL before firing.

## Transport integration — later specialist
Remove only now-supported MessageExpiry/nonzeroWillDelay admission refusals. Carry
Delivery in production writer output, reserve only via shared Poll API, immediately
before first complete-frame write request BeginDelivery through bounded worker with
client+generation+ticket. No locks across await, no store IO on async tasks. Controls
stay unblocked; terminal priority and complete-frame bounds unchanged. ACK-write
inbound credits remain correct for discarded outbound data. Timer helper includes all
new deadlines; changed empty polls/begins/expiry/schedules commit and notify. Startup
restore->expire->reconcile->recoverWills->snapshot commit before application acceptance.
No silent fallback on SQL failure. Test a real blocked writer queue whose pending TTL
expires before write; never send expired first payload, but preserve started handshake.

## Quality / review gates
Independent wire + Paho in both modes; only persistence-specific skips. QoS0/1/2,
TTL0/UINT_MAX, offline and retained expiry/replacement/delete/capacity, decreased
remaining values, incoming duplicateQoS2 no reset and expired PUBREL completes,
started QoS1/2 survive expiration/reconnect, delayed writer suppression, mixed versions,
Will delay/no premature publish, reconnect cancellation/replacement, normal close,
DISCONNECT04, CleanStart true/false, active takeover, expiry0/shorter/longer/infinite,
messageTTL starts after delay, capacity-blocked retry, known schedule restart/overdue,
active crash conservative restart, auth denial cannot cancel intent and schema4->5.
Strict missing/new nullable fields/metadata inconsistency/legacy corruption tests.
Preserve all old311/auth/QoS/expiry/recovery suites; update only previously unsupported
feature expectations and strict migration fixtures. Independent read-only review,
fmt/clippy/test/rustdoc, actual results/commits/docs and lib.rs audit before acceptance.

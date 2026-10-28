# Phase2e frozen MQTT5 foundation contract
Normative basis: OASIS MQTT5 OS (2019), especially3.1/3.2 properties,3.3/4.3
QoS,3.14 DISCONNECT,4.9 connection flow control. This is an explicitly bounded
MQTT5 subset, not full MQTT5 conformance. Reuse existing protocol Packet, QoS FSMs,
router, retained store, principal ownership, snapshot transactions and outboxes.
Keep every lib.rs documentation/module declarations/reexports only.

## Versioned wire API — protocol owner
Existing Packet/Will/encode/decode (MQTT311) stay source compatible. Add Packet::Auth
unit variant; legacy codec rejects AUTH. Add ProtocolError::ProtocolViolation(String)
for syntactically framed but semantically invalid protocol input (duplicate singleton
property, zero ReceiveMaximum etc). PerMQTT5 section2.2.2.2, an unknown property
identifier or identifier invalid for this packet type is MalformedPacket0x81, as
are framing/UTF8/truncation errors; do NOT classify wrong property context as0x82.
Add ProtocolError::Mqtt5(ReasonCode) when a semantic error has a specified reason,
notably zero TopicAlias ->0x94, so transport never parses error strings.

Public root reexports from dedicated modules:
- ProtocolVersion enum V311,V5 (Copy/Clone/Debug/Eq).
- Property typed enum covering ALL MQTT5 property identifiers and correct wire
  types: PayloadFormatIndicator(u8),MessageExpiryInterval(u32),ContentType(String),
  ResponseTopic(String),CorrelationData(Vec<u8>),SubscriptionIdentifier(u32),
  SessionExpiryInterval(u32),AssignedClientIdentifier(String),ServerKeepAlive(u16),
  AuthenticationMethod(String),AuthenticationData(Vec<u8>),
  RequestProblemInformation(u8),WillDelayInterval(u32),RequestResponseInformation(u8),
  ResponseInformation(String),ServerReference(String),ReasonString(String),
  ReceiveMaximum(u16),TopicAliasMaximum(u16),TopicAlias(u16),MaximumQos(u8),
  RetainAvailable(u8),UserProperty(String,String),MaximumPacketSize(u32),
  WildcardSubscriptionAvailable(u8),SubscriptionIdentifierAvailable(u8),
  SharedSubscriptionAvailable(u8).
- Properties = Vec<Property>; Property implements Clone/Debug/PartialEq/Eq (AuthenticationData Debug redacted) and serde
  Serialize/Deserialize for versioned durable records. Order and repeated UserProperty
  preserved. All scalar singletons except permitted repeat SubscriptionIdentifier
  (only PUBLISH); SUBSCRIBE subscription ID singleton. Unknown IDs, invalid context,
  duplicate forbidden properties/zero ReceiveMaximum/zero MaxPacketSize/zero TopicAlias,
  bool>1, MaximumQos>1, invalid/noncanonical variable integers and UTF8 rejected.
  ResponseTopic must nonempty valid Topic Name. Invalid UTF8/NUL/noncharacters are
Malformed81; well-formed UTF8 with an empty/wildcard ResponseTopic is Protocol82. No unbounded allocations/recursion.
- ReasonCode opaque typed byte, TryFrom<u8> validates standard code set, value()->u8,
  associated named constants SUCCESS,NO_SUBSCRIPTION_EXISTED,DISCONNECT_WITH_WILL_MESSAGE,
  MALFORMED_PACKET,PROTOCOL_ERROR,IMPLEMENTATION_SPECIFIC_ERROR,
  UNSUPPORTED_PROTOCOL_VERSION,CLIENT_IDENTIFIER_NOT_VALID,BAD_USER_NAME_OR_PASSWORD,NOT_AUTHORIZED,
  SERVER_UNAVAILABLE,SESSION_TAKEN_OVER,KEEP_ALIVE_TIMEOUT,BAD_AUTHENTICATION_METHOD,
  PACKET_IDENTIFIER_NOT_FOUND,RECEIVE_MAXIMUM_EXCEEDED,TOPIC_ALIAS_INVALID,
  PACKET_TOO_LARGE,QUOTA_EXCEEDED,SHARED_SUBSCRIPTIONS_NOT_SUPPORTED,
  SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED (other standard constants welcome).
  Validate allowed reason set per packet (including CONNACK/SUBACK codes) and flags.
- Frame {pub version:ProtocolVersion,pub packet:Packet,pub properties:Properties,
  pub will_properties:Properties,pub subscription_options:Vec<u8>,
  pub reason_code:ReasonCode,pub reason_codes:Vec<ReasonCode>} Clone/Debug/Eq.
  Frame::v311(Packet),Frame::v5(Packet) initialize empty metadata/SUCCESS.
  packet.clean_session represents Clean Start in V5 only; version determines semantics.
  ConnAck code remains Packet.code; SubAck reasons remain Packet.codes. Frame.reason_code
  only ACK/DISCONNECT/AUTH; reason_codes only V5 UNSUBACK (one per filter).
  subscription_options empty means default packet.filters QoS; otherwise exact filter
  count and matching QoS bits required; validate NoLocal/RAP/RH reserved/value bits.
- decode_frame(input:&[u8],max:usize,version:Option<ProtocolVersion>)
  ->Result<Option<(Frame,usize)>,ProtocolError>: CONNECT determines version4/5, later
  frames use connection context; second CONNECT can be parsed but runtime rejects.
  Before CONNECT non-CONNECT may decode as legacy then runtime closes. Explicit
  version mismatch CONNECT is protocol violation. Unsupported protocol levels still
  UnsupportedVersion. encode_frame(&Frame)->Result<Vec<u8>,ProtocolError>.

Support all base control packets through normalized Packet, including short/omitted
reason+property forms for ACK/DISCONNECT/AUTH and V5 SUBACK/UNSUBACK. PUBLISH property
length always parsed at correct position. Version5 allows password without username
(syntactically; local authentication can reject it). V311 restrictions unchanged.
V311 Frame metadata must empty; cross-version forwarding deliberately strips wire
metadata through the model, not by permissive encoder. Fixed flags/IDs/remaining
length/UTF8/string+binary/property lengths and limits checked before allocations.
Shared filter syntax may decode; do not apply runtime feature policy in codec.
Also export peek_connect_version(input:&[u8])->Option<ProtocolVersion>, a bounded
partial CONNECT-header inspection reusing protocol parsing helpers, so transport
can frame a V5 failure CONNACK when later properties fail before Frame exists. It
is not acceptance/validation of the full packet; no transport-side MQTT parsing.
Test official boundary/context matrices, fragments/concatenation, malicious lengths,
all property types/reasons, v311 regression and password redaction.

V5 codec accepts empty ClientId syntax; runtime explicitly refuses with CONNACK0x85
before authentication (assigned Client IDs not implemented). V311 behavior unchanged.

## Supported runtime Properties / explicit exclusions
Implemented CONNECT SessionExpiryInterval,ReceiveMaximum; informational UserProperty,
RequestProblemInformation/RequestResponseInformation,TopicAliasMaximum can be accepted
(the server does not use client aliases; sends no alias). MaximumPacketSize is NOT
negotiated in this increment: reject supplied CONNECT value with CONNACK0x83 rather
than ignore a client bound. AuthenticationMethod/Data reject with0x8C (enhanced auth
not implemented). AUTH after accepted basic CONNECT rejects protocol error.
A MaximumPacketSize refusal itself must fit the supplied size (minimum V5
CONNACK is5 bytes); if it cannot, close without an oversized response.
CONNACK advertises ReceiveMaximum=min(max_inflight,65535),MaximumPacketSize=config limit,
TopicAliasMaximum=0,SharedSubscriptionAvailable=0,SubscriptionIdentifierAvailable=0;
normal retained/wildcard availability supported. Respect RequestProblemInformation
by omitting diagnostic ReasonString/UserProperty (none required in replies).
RequestResponseInformation may be accepted without optional ResponseInformation.

PUBLISH/Will forwardable metadata: UserProperty (duplicates/order),ContentType,
PayloadFormatIndicator,ResponseTopic,CorrelationData. Preserve through retained
replace/replay, inbound QoS2, outgoing retries, offline queues, restart and version
bridging (strip only when writing V311). TopicAlias input rejected0x94. Subscription
Identifier client->server PUBLISH rejected0x82; SUBSCRIBE with an Identifier causes
DISCONNECT0xA1 because SubscriptionIdentifierAvailable=0 was advertised.
MessageExpiryInterval is not implemented: reject its presence with0x83, do not ignore.
Nonzero WillDelayInterval rejectedCONNACK0x83; zero accepted/removed before publication.
Nondefault subscription NoLocal/RAP/RH unsupported ->SUBACK0x83 per filter (no silent
acceptance); shared filters cause DISCONNECT0x9E because SharedSubscriptionAvailable=0
was advertised. Reject these fatal unsupported SUBSCRIBEs before any filter install. Reserved option bits6/7 are MalformedPacket0x81 perMQTT-3.8.3-5;
QoS3,RetainHandling3 and NoLocal-on-shared are ProtocolError0x82.
All exclusions documented/tested; these are not claims of full MQTT5 compliance.

## Session API — session owner
Engine keeps existing constructors/methods as V311 wrappers. Uses Protocol Frame API.
New clock API in durable module: Clock:Debug+Send+Sync {fn now_millis(&self)->u64;},
SystemClock, new_with_clock(config,Arc<dyn AccessControl>,Arc<dyn Clock>),
restore_with_clock(config,bytes,Arc<dyn AccessControl>,Arc<dyn Clock>).
Existing new/restore/new_with_access/restore_with_access wrap SystemClock.

ConnectionOptions {pub version:ProtocolVersion,pub clean_start:bool,
 pub session_expiry_interval:u32,pub receive_maximum:u16,pub will_properties:Properties}.
connect_with_options(&mut self,id:&str,will:Option<Will>,principal:Principal,
 options:ConnectionOptions)->Result<ConnectResult,EngineError>.
V311 wrappers map clean=true->expiry0,clean=false->expiryUINT_MAX. V5 Clean Start
resets old state independently of expiry; default expiry0, UINT_MAX infinite.
CleanStart=false can resume old state then set new expiry0 for this connection.
Ownership/ACL/Will validation BEFORE takeover/reset; expired state may be removed
but rejected proposals must never be installed. New error EngineError::Mqtt5(ReasonCode).

handle_frame(id,Frame)->Result<Vec<Frame>,EngineError> reuses common legacy handlers,
not a second QoS/router implementation. poll_frame_with_changes(id)
->Result<FramePollOutcome,EngineError> {pub frame:Option<Frame>,pub changed:bool}.
Old packet handlers/polls wrap these or shared internals with V311 semantics.
disconnect_with_options(id,graceful:bool,expiry_override:Option<u32>)->Result<(),...>:
DISCONNECT can change expiry, but CONNECT expiry0 ->positive override is protocol
error0x82. Reason0x04 fires Will despite DISCONNECT; other valid reasons graceful.
expire_sessions()->Result<bool,...>; next_expiry_deadline()->Option<u64> for cheap
worker timer guard. Finite intervals are milliseconds on Clock; offline deadline
persisted and respected across restart; never expire online sessions. Already offline
known deadlines include downtime. Active-at-crash finite sessions acquire deadline
restart-now+interval conservatively (crash instant unknown; MQTT5 permits clock
suspension while stopped); commit that deadline before ready. No extending known
offline deadlines on each reboot. Expiry removes subscription/queues/IDs, retains
global retained messages; pending Will publication remains independent and retries.

ReceiveMaximum is connection-local, NOT persisted. Track IDs of QoS1/2 PUBLISH
actually selected for this connection, separate from total durable pending count.
New PUBLISH/replayed AwaitAck/AwaitReceipt consume quota; PUBACK/PUBCOMP or negative
PUBREC release; positive PUBREC does NOT release. Reconnect resets quota even if
old pending count exceeds new limit. Replayed AwaitComp sends PUBREL without consuming
new quota; controls must bypass blocked PUBLISH in replay order. Preserve relative
PUBLISH send order. May suspend QoS0 with queued PUBLISH; never control frames.
Negative PUBACK/PUBREC/PUBCOMP>=0x80 terminate matching outbound stage without retry,
release ID+metadata+quota; wrong stage remains error. Unknown PUBREC ->PUBREL0x92;
unknown PUBREL ->PUBCOMP0x92; no duplicate business delivery. Forbidden v5 PUBLISH
may close with DISCONNECT0x87; reuse current rollback/no success ACK behavior.
SUBACK returns proper per-filter V5 denial codes; UNSUBACK includes0/0x11 per filter.

Keep canonical stored wire PUBLISH/QoS FSM as existing V311 representation; maintain
properties alongside incoming/outgoing IDs and Message records, never overwrite
original metadata on duplicate incoming QoS2. Metadata retired atomically with IDs
and ACL pruning. Retained uses existing router store/matcher plus model metadata
sidecar if needed; no router ownership changes required. Restore validates all
property contexts and rejects unsupported persisted metadata. Cross-version resume
keeps IDs/stages/order; V311 output strips metadata only on wire.

Business JSON schema4, SQLite opaque container stays1. Explicitly migrate1/2/3 to
current records with infinite legacy persistent expiry, no deadlines and empty
properties. Current record Session fields add session_expiry_interval/expires_at;
finite None means active-at-snapshot (startup schedules above), infinite None;
expiry0 must not occur in persistent snapshot. Incoming/outgoing records add explicit
properties per ID; queued/retained/Will records add properties (required for v4).
All old fields/owners/limits/strict migration retained. Corrupt or quota-incompatible
records fail startup even if their expiry would have elapsed; validate before prune.
Runtime version, send quota and sent-ID sets are NOT persisted. Snapshot/state-byte
limits include metadata. No raw credentials/hashes added. JSON layout implementation
must be described in docs once committed; own records/types may change within this
semantic frozen contract, public APIs above must not drift without amendment.

## Transport — next wave owner
Use decode_frame/encode_frame with negotiated connection version. Existing writer
legacy test wrappers may wrap Frame; all production output version-aware. Carry
ConnectionOptions/Frame/disconnect override through bounded worker requests, while
retaining V311 helper variants/defaults as needed for existing unit constructors.
Startup attach policy->validate restore->expire->reconcile->recoverWills->commit.
Timer sends bounded Tick requests (~100ms), cheap no-op before clone when no deadline
due. Commit/notify when expiry/reconcile/poll/Will changes; generation checks precede
stale cleanup. Active-at-crash deadline committed once on restore.
V311 memory persistent policy unchanged; V5 positive expiry allowed in volatile mode
across live-process disconnects (process durability only with SQLite). New unsupported
CONNECT properties refused before model mutation/old-client cancellation.
Refusal4/5 map to V5 0x86/0x87; valid unsupported properties0x83/0x8C as above; no
pipelined rejected input. Accepted V5 errors send bounded DISCONNECT with reason
0x81 malformed,0x82 protocol,0x93 receive limit,0x95 too large,0x87 denied,0x8D timeout;
takeover old V5 socket sends0x8E when possible. EOF/shutdown still unexpected Will.
Do not use a blanket error-type flush heuristic; only actually queued terminal frames
are flushed under bounded deadlines. A connected terminal frame uses a bounded
priority signal: finish at most the current complete frame (or close on its write
timeout), discard queued data, then write one terminal frame. Do not drain an entire
slow-client queue before DISCONNECT; do not interrupt a partially written frame and
continue writing a new frame on that stream. Bound total wait to at most two frame
write_timeout intervals, then abort/close. Existing writer/backpressure safety preserved.

Advertised inbound ReceiveMaximum enforced through QoS1/2 outstanding IDs until
corresponding PUBACK/PUBCOMP/negativePUBREC is actually written. Repeated QoS2 PUBLISH
same ID does not consume twice. If writer callbacks used, bind completion tickets
to exchange generation so old ACK cannot release a reused ID. All credit structures
bounded by max_inflight and controlled without locks across await. Inbound pending
QoS2 survives session reconnect but connection receive quota resets. Controls keep
progress under outbound quota. New commands/queues bounded; no async storage IO.

## Bridge wire-size admission clarification
max_packet_size bounds every complete outbound frame as well as input. V5 adds a
Property Length byte and can cross a Remaining Length boundary even with no metadata.
Never accept/emit a too-large frame or silently drop previously durable QoS data.
For CleanStart=false resume into a version whose encoded queued or payload-bearing
outgoing replay exceeds the bound, refuse CONNECT with0x83 BEFORE takeover/reset,
preserve old session/IDs and allow311 reconnect to drain. Check after owner/new-Will
ACL so other principals still get0x87. CleanStart=true may explicitly discard the
old session as normal. Expired old sessions do not block admission.
For all recipients preflight the current runtime versioned PUBLISH size before adding outbox
or retained replay; overflow is Capacity (V5 data event closes0x97), no source ACK or
partial route/subscription install. Offline peers last negotiated as5 must also pass this preflight, preventing a
volatile5 session from accumulating data it cannot drain under its known wire limit.
Restored records have canonical311 runtime defaults; their legacy canonical data
can be stored, with later version-resume admission above. TCP311 drain is available
only with SQLite under the existing memory policy; explicit CleanStart reset remains
available in both modes. Keep a final poll guard before emitting/allocating
an oversized delivery; errors discard proposals. Do not reject valid legacy snapshots
merely because a future5 envelope might be larger; restore/311 replay remain valid.
Test exact64-byte311QoS1->65-byte5 plus127/128 Remaining Length transitions, failed
5 resume preservation/311 drain, live mixed-version publish and retained replay.

## Independent Quality / integration gates
Protocol + Session + Quality launch in separate worktrees after this freeze; Session
uses frozen protocol APIs until actual dependency commit then tests. Transport wave
after integrated dependencies, Quality tests raw wire and real Paho2.1 MQTT311/5.
Cover mixed publisher/subscriber versions QoS0/1/2, metadata/retained/Will/recovery,
all CleanStart/expiry combinations (0,finite,infinite,DISCONNECT override), expired
subscription/outbox removal, process restart before/after deadlines, legacy schema3
migration, new ReceiveMaximum1 including prior pending>newlimit, quota held through
PUBREC/PUBCOMP, negative ACK release, control/replay progress and zero/malformed props.
Unknown/duplicate/wrong-context props and unsupported semantics must fail explicitly.
Adapt old MQTT5-rejection regression to still-unsupported protocol level; preserve
all old V311 auth/ACL/retained/Will/persistence tests and owner boundaries. lib.rs thin,
fmt/clippy/test/rustdoc, real TCP/Paho and independent review before acceptance.

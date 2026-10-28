# Phase2h frozen subscription options and Subscription Identifier contract

Basis: OASIS MQTT5 OS3.8.2.1.2,3.8.3.1,3.8.4,3.3.2.3.8 and4.7.
https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
Reuse codec, indexed trie, QoS/outbox, peer size/ReceiveMaximum, Clock/expiry,
retained/Will/ACL and serialized SQLite proposals. lib.rs stays docs/mod/reexports.

## Isolation and dependency DAG
Session owns mqtt-session/** in agent/p2h-session worktree. Router owns
mqtt-router/** in agent/p2h-router. Quality owns tests/** and workflows in
agent/p2h-quality. Coordinator owns docs/root lock/integration. Start from this
freeze, no writes to other ownership or shared checkout. Router and Quality run
independently; Session codes against frozen Router API then cherry-picks its real
commit before final tests. Released Router slot enables Transport/Protocol review
and Transport capability change. No second broker or storage backend.

## Router public API
Add Router::matching_subscriptions(&self,topic:&str)->Vec<(u64,String,u8)>.
Each tuple is(client,exact matching filter,granted qos). Return each subscription
once, preserve distinct overlapping filters for the same client; deterministic
client/filter order desirable. Reuse iterative trie matching, not a global scan of
all filters. Keep matching_qos/subscribe_qos/cleanup stable and max-QoS deduplicated.
No subscription IDs/options in router: session owns protocol semantics/ACL/state.

## Persistent subscription model / schema6
Business JSON6; SQLite opaque container remains1. Session subscriptions become
full records {filter:String,qos:u8,no_local:bool,retain_as_published:bool,
retain_handling:u8,identifier:Option<u32>}. identifier MUST be present nullable,
not missing-default None. Validate filter uniqueness, qos0..2, RH0..2, identifier
1..268435455; booleans typed; unknown fields reject. Legacy1..5 explicitly migrate
old(filter,qos) to NLfalse,RAPfalse,RH0,idNone, preserving owners/deadlines/floor/
QoS stages/IDs/properties/Will timers. Strict version5 DTO rejects missing current5
fields; never reinterpret smuggled option/identifier metadata in older snapshots.

Queued Message adds REQUIRED retained_replay:bool; outgoing record adds same bool
with matching runtime per-ID sidecar. It distinguishes retained subscription replay
from live RAP-preserved retain. Replay implies retain=true; Released Comp carries
false and no payload properties/deadline. Legacy queued/outgoing retained flags
came only from replay, so derive it from old retain/canonical PUBLISH retain bit;
Comp=false. Original incoming QoS2 tuple unchanged; publisher ID is the containing
session key and remains available at PUBREL. No persistent publisher field needed.
All new fields participate in bounded state validation and atomic cleanup/migration.

## Subscribe semantics
Accept valid V5 options and singleton SubscriptionIdentifier. Identifier applies
to EACH accepted filter in that SUBSCRIBE; replacement stores new ID or absence
(clears old ID). Exact-filter replacement consumes no extra slot. Denied entries
leave prior record unchanged. Unsubscribe removes entire subscription record.
CleanStart/session end removes them; persistent resume/restart preserves all fields.
V311 subscription creation/replacement uses defaults/no identifier for that filter;
other preexisting subscriptions survive under existing resume rules.

RH0 replays matching retained values for new AND existing filters; RH1 only if
filter did not exist before that particular sequential entry; RH2 never replays.
Process multiple filters as sequential subscriptions with one SUBACK; repeated
filter within one packet observes preceding accepted replacement. Denied filters
have no replay or update. Retained replay always RETAIN1 regardless RAP and follows
RH even when NL is set (subscription-triggered replay, also established Mosquitto
retain__process behavior); no historical publisher provenance is invented.
Replay QoS=min(stored,grant), and only the triggering entry's identifier is added.
Other overlapping subscriptions must not leak identifiers into that replay copy.

## Live matching and recipient snapshot
Preserve original publisher RETAIN until recipient selection (retained updates
remain separate). Pass publishing ClientID through existing publish/route entry
points for normalQoS0/1, pendingQoS2 PUBREL and Will (WillRecord.client_id).
For each matching subscription, apply ACL and NL before QoS/IDs/RAP aggregation:
NL blocks that match when publisherClientID == targetClientID, including self Will.
Another eligible non-NL matching subscription can still deliver. Aggregate ONE
live copy per recipient at min(sourceQoS,max eligible subscription QoS). For mixed
RAP matches the broker chooses preserve retain if ANY eligible match has RAP1;
otherwise clear it. This explicit overlap policy keeps existing dedup behavior.
Only eligible subscription IDs are appended, one per matching subscription with
an ID. Preserve repeated identical IDs when multiple distinct filters share one;
order deterministic but not semantically significant. Existing UserProperty order
and other application properties remain unchanged. Broker-generated IDs never
enter global retained/Will source metadata or incomingQoS2 client metadata.

Snapshot recipient properties/RAP choice at fanout into queued/outgoing state;
retransmission preserves them through disconnect/restart/version bridging. Replacing
or unsubscribing a filter does not rewrite already accepted deliveries. Strip IDs
only on V311 wire; preserve metadata internally for later V5 replay. V311 live output
must RETAIN0 even if prior V5 RAP choice was1; retained subscription replay stays1.
Use retained_replay marker through prepare/Begin/retry to distinguish these cases.
Never PUBLISH after PUBREC; preserve existing ID/window/TTL obligations.

## Size, trust and protocol boundaries
Internal recipient properties allow repeated valid IDs; client-origin PUBLISH and
Will carrying SubscriptionIdentifier remain protocol errors (0x82 for PUBLISH).
Separate trusted delivery metadata validation from source application validation;
restored queued/outgoing may carry IDs, restored incoming/retained/Will may not.
Account full encoded recipient envelope including all ID varints BEFORE queue
capacity. Reuse Phase2g peer-specific discard/pruning (including max_queued1,
retained replay), global server resource/version-growth policy and transaction
rollback; a tiny peer cannot poison other recipients or global retained state.
IDs/options are NOT reset by transport connection-level quota/cap negotiation.

Transport changes SubscriptionIdentifierAvailable from0 to1 (or omits default1),
while SharedSubscriptionAvailable remains0 and shared filters stay unsupported9E.
Keep the explicit1 advertisement to preserve existing CONNACK encoded sizes and
Phase2g tiny-cap admission boundaries. Protocol reserved bits/RH3/QoS3 and duplicate/
zero identifier checks already exist: reuse and verify, no duplicate decoder.

## Acceptance gates
Real unit/wire/Paho memory+SQLite: NL live eachQoS including originalQoS2 after
restart/Will/same-ID takeover and mixed eligible filters; RAP0/1 live retained,
non-retained and deletion; retained replay always1; RH0/1/2 new/replacement/duplicate
filter entries/denial; identifier bounds/duplicates/replacement-clear/unsubscribe,
overlap highesteligibleQoS/repeated IDs and metadata order; no IDs from denied/NL
matches; recovery/schema1..5 migration/strict6 corruption; input-ID rejection;
small cap and varint envelope expansion/maxqueue1, QoS1/2 replay and PUBREL-only,
V5->311->V5 metadata+live/replay RETAIN handling. Run workspace fmt/Clippy/tests/
rustdoc, old suites and actual clients. Report commits, real counts and unsupported
scope; no full conformance/performance claim.

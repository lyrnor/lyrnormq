# Phase7b — reliable forwarding and persistent session failover

Frozen 2026-11-03, supersedes phase7a refusal of persistent sessions only when
explicit durable cluster mode is enabled. Standalone and phase7a default behavior
remain unchanged. Retained/Will/shared cluster support remains outside this gate.
No Controller stores message bodies, session snapshots, inboxes or outboxes.

## Placement, authority and safety

Config adds optional durable:{replicas:[{id,address},...]}: exactly three distinct
fixed Broker data replicas, including this node. Controller membership remains
three independent metadata nodes. Broker config.data_dir contains a bounded
SQLite WAL/FULL data journal, separate from Controller Raft. Local RowStore is a
compatibility mirror, never authoritative over replicated cluster records.
All replica file IO is on bounded blocking workers; no lock crosses await.

Each client has a Raft-owned monotonically increasing ownership epoch, owner node,
incarnation, connection generation and ordinary filter interest. Durable directory
records/filters survive Disconnect and NodeExpire. Offline sessions remain routable.
Only the current epoch with a quorum-renewed node lease can mutate a session.
Every transition, including offline delivery, timers, ACKs, cleanup and pending
finish, checks ownership. Prior local disk state is not automatically reactivated.
Identity ownership and MQTT expiry continue to use existing SessionEngine semantics.

Ownership acquisition: commit epoch/owner in Raft and fence via existing revision
barriers; issue Fence(new_epoch) to data replicas. A successful Fence transaction
raises min_epoch and atomically returns its highest complete record, preserving
old content. Collect two such responses, select highest (epoch,physical_revision),
then write the recovered record in the new epoch to two replicas before activation
or CONNACK. Do not fence two then read a different arbitrary pair. Replica writes
reject older epochs and conflicting identical versions; repeated identical writes
are idempotent. A stale or ambiguous writer must stop and reacquire, never continue
from an old local snapshot. Candidate records of unacknowledged operations may
survive; an uncertain operation is not promised to have rolled back.

RF=3, W=2: successful mutations survive loss of one data node/disk. Fewer than two
reachable data replicas must fail closed, including new durable ACK/CONNACK. No
availability or durability promise for loss of two copies, Byzantine peers, or
unbounded topology changes. Fixed replica membership cannot silently be replaced
with a new set of empty disks. A missing replica response is not an empty record.

## Atomic record and forwarding

Cluster session record contains opaque existing SessionEngine snapshot (or a
durable tombstone), source outbox intents, and recipient delivery receipts. One
replicated record mutation atomically changes these fields. Snapshot durable schema
does not change. Internal record physical revision is distinct from transport's
session revision: outbox completion must not invalidate an otherwise current
transport session revision. Per-record ownership/serialization prevents competing
physical writes; bounded actor queues are preferred over holding locks across IO.

Source: compute existing session proposal; atomically commit resulting snapshot
plus all resulting publications as stable outbox intents to W2 BEFORE local/remote
fanout or successful MQTT ACK. Publication identity includes source record, epoch,
session revision and publication index. Snapshot-only then forward, or forward then
snapshot, is forbidden. Clean Start/expiry/deletion must not discard accepted
forwarding responsibility. Generic HTTP/system publications use a separate internal
record namespace and the same journal; they cannot collide with MQTT Client IDs.

Freeze recipient CLIENT identities at admission from ordinary directory interest;
do not rediscover recipients on retries or address solely by a stale node. Deliver
each recipient through its current owner; local and remote recipients use the same
durable mechanism, preserving No Local, QoS, expiry and one source rules observation.
All recipients confirmed or expired => persist source intent terminal. Pending
intents remain on failure and resume after owner death, without requiring a source
client reconnect. Background recovery uses CAS against the expected abandoned
epoch, so it cannot steal an active new owner's session.

Recipient: propose delivery through existing actor/QoS/outbox. Atomically W2 commit
recipient snapshot AND stable delivery receipt before confirming acceptance. A
duplicate receipt does not install a newly computed proposal or deliver again.
Partial multi-recipient delivery retries per recipient. Do not use a volatile LRU
or separate post-delivery receipt transaction as the correctness boundary. Receipt
retention is bounded and may fail admission when full; it cannot evict a receipt
while a replay can still reference it. Any later GC requires durable source-terminal
evidence and an explicit protocol, not a timeout guess.

QoS1/2 acknowledge replicated responsibility, not immediate subscriber receipt.
QoS0 socket delivery remains MQTT best effort, even if its forwarding intent was
durably accepted; do not claim end-to-end QoS0 exactly once. Preserve MQTT QoS2
incoming and outgoing state, packet IDs, DUP, PUBREL-only recovery and Receive
Maximum. Replay must not call source rules or reflect internal ingress again.

## Frozen neutral API additions

mqtt-cluster-core adds redacted models SessionLease{client_id,epoch,revision,
snapshot:Option<Vec<u8>>}, DeliveryId{source,epoch,revision,index:u32},
SessionMutation{client_id,epoch,expected_revision,snapshot,publications,receipt},
SessionCommit{revision,duplicate}. Existing imports remain. ClusterBridge adds
durable()->bool (defaultfalse), acquire_session(client_id,generation)->SessionLease,
commit_session(SessionMutation)->SessionCommit; new async methods default to
Unsupported for legacy bridges/mocks. acquire_session is authenticated transport's
pre-CONNACK boundary. LocalBroker adds activate_session(SessionLease) and
deliver_to(client_id,DeliveryId,ClusterMessage); defaults Unsupported. Runtime owns
automatic abandoned-owner recovery and data replica RPC; transport owns actor
installation/restoration and exact receipt/session transaction coupling.

LocalBroker.deliver_to serializes per recipient. Duplicate SessionCommit leaves
current engine intact. Unknown commit result invalidates that actor; reconnect or
activate_session must restore authority before more mutations. Existing local
storage is not used to infer a failed remote operation rolled back.

Coordinated API clarification before implementation: SessionMutation additionally
contains filters:Vec<String>. Runtime never parses opaque MQTT snapshots; it commits
the corresponding directory filters under epoch authority and the visibility
barrier before returning successful subscription mutation. Generation0 denotes
offline recovery only, never an MQTT connection. Existing forward(message) is the
durable host/system enqueue boundary when durable mode is enabled; transport must
skip the old local fanout path there, then observe the source once after acceptance.

## Resources and acceptance

Default full encoded replica record <=8MiB / RPC <=16MiB; opaque snapshot <=2MiB.
Bound journal disk/logical bytes, records, per-record intents and receipts, mutation
queues, replication tasks and retry scheduling. Full capacity is failure, never a
memory fallback. No new unsafe, unwrap/expect in production, blocking async IO or
business code in lib.rs. No performance claim based on these correctness tests.

Coordinator owns contracts/core/CLI/workspace/docs; runtime owns mqtt-cluster only;
transport owns mqtt-transport only; quality owns tests only. Independent worktrees
and tested commits, sequential integration. Contract changes first coordinated here.

Actual acceptance uses three Controllers + three independent Broker disks. Kill
owner permanently and resume on another node: persistent subscriptions, offline
QoS1/2, AwaitAck/AwaitRec same ID+DUP, AwaitComp PUBREL only, incoming PUBREC recovery,
lost PUBCOMP no second onward delivery, Clean Start/expiry/username ownership,
stale returning owner, quorum loss fail closed, durable source backlog after death,
recipient crash/retry receipt atomicity, and all-process restart. Use real ACKs as
commit barriers; no test-only production failure switches or shared failed-node
files. Keep original phase7a and standalone regressions, fmt, strict Clippy, tests.
Report actual gates and any remaining limitations; do not silently downgrade QoS2
or call local restart cross-node failover.

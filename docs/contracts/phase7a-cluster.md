# Phase7a — embedded Controller/Broker cluster implementation contract

User-approved architecture supersedes the old partition-directory proposal: two
roles, 3controller OpenRaft quorum, Broker readonly local route replica, snapshot+
continuous committed metadata stream, direct Broker forwarding, no external etcd/
ZooKeeper. One rmqtt binary. No production-ready grpc0.9claim: it is preview with no
documented server module; use official grpc/grpc-rust stable tonic0.14.x behind the
adapter and fixed OpenRaft0.9.25 (validate storage conformance), Protobuf envelopes.

Initial executable gate implements actual3controller election/durable recovery,
Broker registration/leases, global client ownership, aggregated ordinary routes,
watch snapshot/incremental recovery and live cross-node QoS0/1/2. It MUST explicitly
reject unsupported cluster retained/shared/Will/persistent-session relocation before
success, while standalone retains every existing feature. These are followup gates,
not silently emulated; do not claim completed full cluster MQTT compatibility.
Durable non-retain Will on Phase7f replicas is supplied by
[phase7w-cluster-will.md](phase7w-cluster-will.md); this gate still refuses
Will-Retain, shared subscriptions, retained PUBLISH, and any Will on legacy
non-durable cluster.
No cross-node session replication or controller message-body storage. Source local
session durability is unchanged; this gate does not claim replicated/transactional
cross-node forwarding outbox. Connection/metadata failure must not report success
or silently treat directory unavailable as no subscribers.

Neutral API crates/mqtt-cluster-core (coordinator-owned) frozen in api.rs/model.rs.
Transport owner only mqtt-transport; cluster owner crates/mqtt-cluster; quality tests.
Root owns bin/CLI, workspace/lock/docs and core interfaces. Independent branches/WTs.
lib.rs documentation/modules/reexports only; no unsafe/productionunwrap/expect.

BrokerConfig.cluster:Option<Arc<dyn ClusterBridge>>. Bridge open after restoration,
before ready, supplies LocalBroker callbacks. Claim after authentication before
successful CONNACK/session takeover; global owner write must fence old local
connection before applied-revision ACK. Failure no unauthorized takeover. Release
is exact node incarnation+connectiongeneration, stale close never releases new owner.
Lease is Broker-level, conservative monotonic request-start deadline refreshed only
by quorum-confirmed controller reply. ready false after lost lease => fence local
connections and reject new mutation/forward, not indefinite stale-cache operation.
New leader must not expire prior grants early (full lease protection period or
explicit monotonic protocol). Raft state machine decisions carry leader time, not
per-replica wall clocks/randomness. Initial nodes fixed configured membership.

Controller metadata commands RegisterNode,ClaimClient,ReleaseClient,RouteDelta,
Heartbeat/appliedrevision; no clientpayload/credentials in metadata. Atomic durable
vote/log/committed state; fsync-before-logflushed, snapshot atomicinstall and verified
recovery. Never use memstore and claim durability. Bound metadata counts/bytes,
mutation concurrency, event history and subscriber queues; a history gap gets a
snapshot, not silently skipped updates. Followers cannot serve stale authoritative
writes. Broker seed client follows/retries bounded leader redirects; auth/clusternode
identity gate is mandatory, no public unauthenticated consensus RPC endpoints.

Committed revisions streamed snapshot then ordered deltas. Route events can be
batched; Broker applies complete updates then ACKs watermark. SUBACK waits local
session commit + committed route + every currently eligible publishing Broker has
applied revision. A slow/isolated Broker is excluded only after it cannot legally
serve (fenced/grant expired), not immediately on one missed heartbeat. Initial
metadata wire may send bounded full snapshots per revision (explicitly report this
as baseline, NOT optimized incremental realtime sync); true delta gate remains if
not implemented. No controller query per publication. Routes aggregate first/last
local filter interest; directory keys preserve MQTT dollar wildcard semantics.

Transport internal forwarding must suppress local observer/rules repetition and
reflection loops, retain original MQTT publisher for NoLocal (HTTP/system origin
None), carry MQTT5 properties/expiry, execute recipient durable QoS path. Source
rules observer runs once, data RPC caller gets bounded success/failure/unknown.
Same source connection ordered; stable internal ID for retries, bounded duplicate
receipt cache; no exactlyonce/crashcomplete network promise without durable receipt
and forwarding responsibility. Control and data/snapshot streams have separate
bounded queues. RPC includes cluster id + private node token, finite body/time limits;
public endpoints require configured TLS/mTLS. Plaintext HTTP is limited to
loopback and RFC1918 private addresses. No credentials/payload logs.

Root proposed config via --cluster-config PATH (strict startup JSON, role controller
or broker). Controllers: cluster_id,node_id,listen,data_dir,token_file,controllers
[{id,address}],bootstrap bool(defaultfalse),optional TLS paths. Brokers additionally
advertise,controlleraddresses,lease interval; original --listen is MQTT, adminHTTP
unchanged. Root/cluster owner coordinate Config exact shape before coding rootCLI.
Controller role starts no MQTT listener/admin auth DB/rules; Broker starts localdata
RPC, attaches runtime before MQTTready, drains on shutdown. Reuse existing token file
visibleASCII32..256 readlimits (separate secret from admin token recommended).

Required actual acceptance:3controller+2Broker processes, both MQTT versions,
crossnodeQoS0/1/2 and HTTP, routeSUBACKbarrier, global takeover/stalecleanup, leader
kill+majoritylossfailclosed, controllerrestart state/log/snapshot, stalewatchrebuild,
internaltoken/sizelimit, unsupportedfeature refusals, standalone837+ regressions.
Record exact passed gates/knownremaining; no benchmark/productionHA claim invented.

Approved config/API: mqtt_cluster::Config derives strict Deserialize with role:Role
(controller|broker),cluster_id:String,node_id:u64,listen:SocketAddr,data_dir:PathBuf,
token_file:PathBuf,controllers:Vec<Controller{id,address:String}>,bootstrap:bool=false,
advertise:Option<String>,lease_ms:u64=3000,tls:Option<TlsConfig{cert_file,key_file,
ca_file,domain_name}>. Controller metadata/log/vote/snapshot storage may use SQLite
WAL/FULL with bounded blocking ownership (never MQTTpayload). Public run_controller,
BrokerRuntime::new,serve_broker APIs agreed; async status(&Config)->ClusterStatus
{leader:Option<u64>,term,last_applied,nodes,clients,routes} supports root cluster-status
CLI. Config and keys/data directories cannot become Dashboard assets.

Coordinated fencing amendment: ClusterBridge::subscriptions takes explicit
(client_id,generation,filters). Route update commands validate exact node incarnation
and connectiongeneration; never look up latest generation on behalf of a delayed
old request. Same-node Claim is idempotent for samegeneration and rejects older
generation. Watch must terminate/reconnect on leadership change even if no metadata
revision arrives. Expiry requires monotonic grant observation protection, not just
wall-clock comparison (including clock jumps during stable leadership).

Mixed-controller lease amendment: each node incarnation durably retains
max_lease_ms, the maximum lease ever granted to it. The leader supplies this value
on Register/Heartbeat; state-machine application never decreases it for the same
incarnation. Expiry waits that duration using monotonic observation of committed
heartbeat tokens, including after leadership changes. A shorter new grant must not
erase a longer outstanding grant whose replacement reply may have been lost.
Absent legacy duration fields default conservatively to the maximum allowed
30000ms. Configurations with different lease_ms must remain safe, not merely be
documented as unsupported.

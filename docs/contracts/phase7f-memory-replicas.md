# Phase7f — volatile typed data replicas

User explicitly replaces cluster session/message/outbox/receipt SQLite with RAM.
This supersedes Phase7b/7c/7e disk durability for data replicas ONLY. Controllers
retain SQLite/Raft metadata; standalone opt-in session SQLite and rules/access
configuration SQLite are unchanged. Cluster replicated mode must reject optional
local --storage-path mirror (coordinator CLI) rather than secretly storing bodies.
No RocksDB dependency/implementation yet; provide an internal backend trait.

## Frozen storage API / ownership

Storage owner owns crates/mqtt-cluster/src/replica.rs and src/replica/** only.
Replace SQL data backend (not SQLite open_in_memory) with typed bounded in-memory
maps; facade retains existing Journal/Record/Request/Reply/repair types and methods
and TEST_PROCESS_START_LOCK used by control-store tests. Move types into model.rs,
store facade/trait into store.rs and implementation into memory.rs. lib.rs remains
modules/reexports. Journal::open signature, execute_at, voting_ready, disk_id,
status, freeze, export_page, begin_import, append_page, finish_import, resume retain
signatures. Journal delegates to object-safe async ReplicaStore trait suitable for
future RocksDB, retaining transaction-level atomic Fence+read, record replacement,
staging install and generation checks. No body/receipt/session writes to filesystem.
Normal record read/write uses typed objects, no SQL/table scans/blocking IO workers.
Canonical JSON may be prepared outside the short map lock for wire-size validation,
exact same-version conflict comparison and repair manifest. Cache record size for
O(1) total quota accounting; charge key bytes as well, bounded records100k, perrecord
8MiB, active canonical record budget256MiB inclkeys, repair stage finite768MiB.
Retain page64/12MiB, exact token/sequence/digest idempotence, fence-only tombstones,
highest complete record/min_epoch semantics and no locks acrossawait. Brief FS
work at open for directory ownership lock/identity-only manifest is allowed, not
message/session persistence. Refuse existing data.sqlite (do not delete/import).
Every new memory-store process/open has a NEW random32hex replica incarnation ID
(the existing disk_id API field is retained as an incarnation identity). Never
restore this ID from disk, otherwise an erased replica could vote as caught up.
Data/repair staging is lost on process death; tests must assert this rather than
claim persistent replay. Old SQL-only tests are replaced with memory semantics,
not ignored/mocked into success. No per-message metadata file writes.

## Frozen runtime contract / ownership

Runtime owner owns mqtt-cluster except replica.rs/replica/** and rootCargo/docs.
Add DataMode::Memory(fixed sortedgroup), preserving old Durable variant strictly for
historical Raft replay/mismatch refusal. Register adds #[serde(default)] memory:bool
(false when replaying historical commands); new runtime sets true with replicated
group. Controller refuses Memory/Durable mixtures and old metadata in-place upgrade.
Existing JSON durable.replicas remains the replication-group config for now; its
historical name is not a durability guarantee. No data bodies in Controller Raft.
Status exposes data_storage=memory/legacy_sqlite/none, with serde default for old
consumers. All replica IDs anchored in Raft; changed boot IDs cannot auto-register
as empty voters, even with same node ID/path. Old data.sqlite is refused.

A replacement normal Broker process runs the existing private repair RPC while
its MQTT service waits for catch-up. With an old anchor, it waits bounded600s for
operator-driven cluster-repair on this live process; only after Controller commits
its new boot ID+manifest may it register, reconcile terminal resume and accept
MQTT/data votes. Peer server is concurrently polled by existing CLI supervision.
No stopping a learner then starting Broker to reuse RAM: pure cluster-repair-learner
is refused for memory, with instructions to start the normal waiting Broker. Use
existing maintenance-window donor freeze/two-donor union/Controller proof commit.
No automatic repair initiations or rolling membership change. Restarted target
mid-repair gets a different ID; old repair cannot commit, Abort/restart repair is
required if two donors remain live. Donor RAM loss does not fabricate proofs.

W2 now means two live memory replicas accepted state, NOT fsync/power-loss durability.
One surviving majority can recover a lost owner; restarted blank replicas require
catch-up. Whole-group restart loses data and must fail closed against retained
control anchors; fresh initialization uses NEW separate control/data dirs/clusterID,
not deletion of old user data. No seamless all-node reboot recovery claim.

## Quality and coordinator

Quality owns tests/cluster_memory* and .github/workflows/** in independent WT;
adds real-process bothMQTTversions QoS0/1/2 and offline/owner-failover cases, normal
startup wait+explicit repair promotion, no data.sqlite/body files, stale/blank
nonvoting, and whole-group RAM loss fail-closed. Prior disk-crash/restart assertions
must be explicitly superseded/replaced in CI, not falsely reported as passing.
Retain control Raft conformance and unaffected tests. Frozen changes discussed with
coordinator first. Each writer isolated worktree/branch, commit tests/hash/limits.
Coordinator owns bin/rmqtt and mqtt-cluster-core docs, integration, optional CLI
mirror rejection, CI/build review, local zigbuild and server reproduction. Preserve
old remote data untouched; deploy RAM mode into fresh Controller/data dirs.
Acceptance: build/fmt/strictclippy/workspace tests; actual60connections100/s128B
QoS0/1/2 reproduction, raw counts/ACK/p99 and no dataSQLite generation. Report memory
semantics and remaining throughput/correctness limits honestly.

Startup integration amendment: coordinator adds ClusterBridge::startup_timeout()
with finite5s default; memory runtime overrides625s to enclose its600s repair wait.
Transport uses this ONLY for open_cluster; forwarding/commit deadlines stay5s.
Runtime owner is explicitly additionally assigned mqtt-transport/src/durable.rs,
src/durable/cluster.rs and focused startup test file for this integration. Select
shutdown against pending cluster start before client admission; do not change
normal active-service draining. On shutdown cancel pending start and close peer
service through existing CLI supervision. No indefinite startup/stop wait.

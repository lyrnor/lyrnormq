# Phase7c — replica catch-up and empty-disk replacement

Frozen 2026-11-03. Extends phase7b's fixed three-data-replica group; no external
coordinator, topology expansion, online change-stream copy or new MQTT feature.
This first repair is an explicit maintenance window: durable operations fail
closed while copying. Controllers keep leases/watch running. Test on the user's
47.84.224.186 Linux amd64 server using locally zigbuilt binaries; never compile
on that server. Tests use isolated loopback processes/directories, not public MQTT.

## Authority and maintenance state

Controller metadata adds data_generation(default0), excluded replica IDs(default
empty), active repair and last terminal repair token/report(defaultNone). An active
repair contains immutable ID, target, old/new disk UUID, two donor IDs/disk UUIDs,
generation and optional sealed manifest. Bodies/entries never enter Raft.

Begin requires fixed durable mode, target's old lease/node already expired,
unchanged old disk anchor and two original donor identities. The replacement
learner's identity comes from authenticated peer RPC. Begin CASes expected state,
increments data_generation monotonically and excludes target. Only one repair is
active. Retrying the same repair/target identity resumes; another identity cannot
silently replace it. A normal empty target remains rejected by registration.

Separate control_ready from normal MQTT/data-voter ready. Active repair disables
durable ACK/CONNACK and ordinary replica voting globally, while heartbeat, Watch,
Controller administration and repair RPC remain usable. Metadata applied revision
alone is not a data-write drain. Both donors must durably freeze their Journals
and report the exact token/disk identity before export/commit. Freeze is serialized
with writes in SQLite; queued ordinary writes recheck frozen state in transaction.

Every ordinary Replica request also carries data_generation. Journal.execute_at
checks that generation in the SAME transaction as Read/Fence/Write. Frozen state
blocks these requests. After thaw, old-generation requests remain rejected forever.
Durable record actors compare observed generation on EVERY operation and discard/
poison cached authority on a change before rebuilding at a higher owner epoch.
Canceling sockets or clearing a volatile flag is not this fence.

## Snapshot and installation

Export stable key-ordered pages from each frozen donor, including key, min_epoch
and optional complete Record. Fence-only/tombstone entries, source outbox IDs/order,
receipts, logical session generation and MQTT snapshots must survive. Merge the
union by key; min_epoch=max, record=highest(epoch,physical_revision). Conflicting
canonical records at identical version are a hard error. Do not pick a convenient
single donor or assume both have identical unacknowledged candidates.

Canonical manifest SHA256 hashes each key-ordered serde_json RepairEntry followed
by newline; bytes=sum those canonical bytes including newline, records=entry count.
The coordinator uses bounded merge iterators, not a full in-memory database.
Pages contain at most64 entries and12MiB encoded bytes, allowing an8MiB record and
maximum key while remaining below16MiB RPC. All IO is bounded blocking-pool work.

Seal the expected merged manifest in Raft before importing. Replacement imports
to separate staging tables, persisting token, manifest, page sequence/cursor and
batch digests. Exact repeated batches are idempotent; wrong sequence/content/token
cannot advance state. Newer repair may reset abandoned staging only after Controller
authorization. Target always remains non-voting during copy and after restart.

Finish import recomputes count/bytes/digest from staging and atomically replaces
records/min_epoch with the complete staged set. Incomplete/incorrect data cannot
be exposed. Installed data remains frozen until Controller authorizes commit.
Commit must verify the two original donor frozen identities/tokens and target
installed UUID/manifest, recheck CAS, then change target disk anchor, remove its
exclusion, persist terminal repair state and clear active repair. An installed
target alone is not a voter. A pre-commit crash cannot implicitly complete repair.

After commit, verified matching Journals durably resume the new generation. Nodes
restart/self-heal using Controller's terminal token and anchored identity, so a
coordinator crash after Raft commit does not leave unsafe voting or require manual
SQLite edits. Normal target startup is separate from the learner process. Old disk
identity, old repair tokens and pre-maintenance RPCs stay rejected.

## Abort and interruption

An explicit pre-commit Abort clears active repair but KEEPS target excluded and
the old disk anchor, and preserves advanced data_generation. Only the two original
donors may resume; no new/old target can contribute W2 until a successful repair.
This restores safe two-donor operation after capacity failure/abandoned target.
Resume is idempotent; a lower-generation or different same-generation token cannot
release a later freeze. A donor not yet frozen when Abort happened can advance its
generation only after terminal-Abort authorization and matching disk identity.
Completed repairs cannot be aborted. Retry/new repair never wipes original donors.

Interrupted driver/target/controller resumes the persisted repair, safely restarting
merge/transmission or continuing verified page sequence. A bad manifest, lost donor,
capacity exhaustion or transport error keeps the target non-voting. Never infer
absence from timeout. No full data-history copy under concurrent writes is claimed.

## Frozen implementation split/API

Runtime owner: mqtt-cluster except src/replica.rs. Storage owner: src/replica.rs
only. Quality: tests/** and .github/workflows/**. Coordinator: CLI, Cargo/lock/docs.
Independent Worktrees/branches; commit tested owned paths. lib.rs remains only
module declarations/docs/reexports. No snapshot/QoS schema change or unsafe.

Storage DTOs (pub(crate), serde): RepairToken{id:String,generation:u64},
RepairManifest{records:u64,bytes:u64,digest:String}, RepairEntry{key:String,
min_epoch:u64,record:Option<Record>}, RepairPage{entries,next:Option<String>,done:bool},
RepairStoreStatus{disk_id,generation,frozen:Option<RepairToken>,next_page:u64,
manifest:Option<RepairManifest>,installed:bool}. Token/manifest derive Eq.
Async Journal methods use owned DTOs: status(),execute_at(generation,Request),
freeze(token),export_page(token,after,limit),begin_import(token,manifest),
append_page(token,sequence,entries),finish_import(token),
resume(token,allow_unfrozen_advance). All return io::Result of Reply/Page/Status.
Runtime validates Controller authorization before calling repair storage methods.
Runtime exports serve_repair_learner(Config,watchReceiver), repair(&Config,target:u64)
and abort_repair(&Config), with Serialize RepairReport. CLI subcommands:
cluster-repair-learner --cluster-config PATH;
cluster-repair --cluster-config PATH --target-node ID;
cluster-repair-abort --cluster-config PATH. Learner starts no MQTT/admin/auth DB.

Repair RPC has finite separately bounded60s deadlines; ordinary data client timeout
stays2s. Add existing sha2 dependency for manifests. Existing256MiB logical record
quota remains; staging has a separate bounded copy budget and finite SQLite page
cap(up to768MiB for records+staging+metadata, WAL overhead additional). Full union
may exceed capacity: fail safely and allow Abort, never truncate to fit.

## Acceptance

Real3Controller+3Broker tests: catch-up of session/QoS/outbox/watermark/fence-only
records; unready target plus one donor cannot form W2; after repair new target plus
either original donor restores confirmed data. Kill learner/coordinator/controller
mid-repair and after installation/commit; retry/restart preserves safety. Wrong
tokens/digests/pages rejected, old generation cannot write after thaw, old disk
cannot rejoin, Abort resumes only original donors, whole-fleet crash recovery.
Use real protocol ACKs/data barriers and deterministic Journal fault tests; no
production failure hooks or arbitrary sleeps as proof. Keep phase7a/7b/standalone
regressions, fmt, strict Clippy/tests. Upload local musl binary+source test scripts,
verify SHA256 on both ends and report actual Linux results/environment. No physical
power-loss, multi-machine availability, online repair or performance claim.

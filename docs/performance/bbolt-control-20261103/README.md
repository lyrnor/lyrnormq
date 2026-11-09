# bbolt Controller integration and server rebenchmark — 2026-11-03

Controller state replacement complete; requested scale acceptance still fails.
This report distinguishes backend correctness from offered connection capacity.

## Source and storage

Broker production source92ecb8c, mqtt-cluster has no rusqlite/libsqlite dependency.
Rules/access configuration elsewhere in the application retains its existing stores.
Business checkpoint maps use bbolt buckets with32-byte hashed physical keys; values
retain full logical keys up to65538bytes, contextual checksums and row data. State
checkpoint data and applied position/membership commit atomically. RAM application
and independent durable Raft WAL unchanged. No per-command bbolt checkpoint commit.
Current periodic checkpoint compares/upserts changed rows and deletes stale rows;
it still scans the frozen State and serializes wire snapshots at checkpoint time.
The16MiB snapshot/RPC bound and16711680 metadata budget remain explicit limits.

Initial bbolt v0.1.0 exposed a confirmed native multi-page write defect: its cached
append hint could insert into a non-rightmost leaf without checking upper bounds.
Native-only repeated sorted writes reproduced missing keys at512 records; a real
2100-subscription snapshot gate rejected the inconsistent aggregate checksum.
Original failing immutable d00b5250… and log
`/private/tmp/bbolt-control-quality-wal.log` retained; no affected build was deployed.
The fix keeps the hint only for root leaves, normal lookup elsewhere, with native
and randomized/get/scan/delete/reopen/Go interoperability regressions. Released:
[bbolt v0.1.1](https://github.com/biaogd/bbolt-rs/releases/tag/v0.1.1), exact commit
3e33a39f4e3ac32123ddc8bfaa7638277a3e7a23. Upstream fix branch
codex/fix-multipage-state; upstream main and old tag were not rewritten.
Upstream release CLI suite279passed/18existingignored, release regression3passed.
No performance improvement is inferred from those tests.

## Verification

- Fixed workspace997passed,1external PostgreSQL ignored; fmt and strict all-target
  workspaceClippy passed. Local logs `/private/tmp/bbolt-control-fixed-workspace-test.log`
  and `/private/tmp/bbolt-control-fixed-clippy.log`.
- Final native immutable SHA256
  34f5ac05b90cd6c08ff96a65e32f210901159c8e8221765a08e2b95471b59df0.
  Real-process gates39/39 first pass, -f and ResourceWarning-as-error:
  WAL6 (78.775s), bbolt/offline-migration6 (43.090s), RAM recovery17 (72.552s),
  event forwarding10 (39.693s). Logs `/private/tmp/bbolt-control-fixed-quality-*.log`.
- Migration used actual Phase7j binary2a9b81a4…, Python offline export under the
  controller lock, and two new-binary crash/restarts with MQTT3.1.1/5 QoS2 routing.
  Source SQLite/WAL/SHM bytes unchanged. Controller never opens SQLite at runtime.
- Exporter10unit gates passed, including active-lock rejection, unsupported source,
  identity/bounds/no-overwrite and required-store fencing.
- Linux artifact built LOCALLY using cargo zigbuild --release --locked --offline
  --target x86_64-unknown-linux-musl. ELF64 x86-64 static/stripped, SHA256
  6ca87ef337f5b68cccf5c25523f60d2ce6081242a100fcdac5ce93ad93bbde89.
  Zig linker warns deprecated optimization1 ignored; build succeeds.
- Same-host Linux release process gates7/7 passed35.901s (WAL6 + combined-role
  double-version/all-QoS). Server log acceptance/linux-gates.log below.
- SIGKILL and injected commit failures are not physical power-failure certification.
  Data replicas remain RAM: whole-data-group loss cannot recover message bodies
  from Controller metadata.

## Reproducible server setup

Broker47.84.224.186/private172.16.240.180; load47.84.241.200/private172.16.240.181.
Each VM8logicalCPUs/~15GiB RAM. Broker Linux7.0.0-38-generic, Python3.14.4, ext4/vda.
Three cohosted Controller+data processes,4Tokio workers each,12workers total.
All3WAL/bbolt files share one physical disk; this is not a multi-machine HA benchmark.
MQTT private ports18831/18832/18833, clients round-robin evenly across endpoints.

New isolated deployment `/opt/lyrnormq-bbolt-control/20261103`; load tools
`/opt/lyrnormq-bbolt-control-load/20261103`. Old deployments/data retained.
Server env and acceptance logs saved there. Fresh root per case; BOTH max_connections
and max_sessions50000/node, state budget256MiB, metadata16711680B, physical records
101024, FD250000, handshake1024/auth-task128, auth-rate0. Limits preflight executed
before processes started. Connection tool uses max2048 handshakes/15s timeout,
10000 absolute-rate arrivals/cohort,10s hold then PING on all live connections.
Rust connection tool SHA390a8be9a239dc6984186c4f4bc770ea00c6cd52626b1dac53d22654c318fdf4;
unchanged message tool f955aa4d0843c48f0a689c5ea8ade782910709ae145a23afe77fdf494e19d7ef.

## Observed first-cohort failure

1000 intended connects/s, target10000. All10000 attempted;0 scheduler-skipped.
281 successful CONNACKs,186 alive after hold/PING.8403 failed CONNACK0x88 and1316EOF.
95 initially accepted sockets later yielded DISCONNECT:62 reason0x83,33 reason0x8b.
Those packets may have queued during ramp/hold; not all failures can be attributed
to the PING phase.0x8b is also an internal unavailable mapping, not proof processes
were actually shutting down. Final3Controllers all term1/leader1/applied5227;
metadata owners2162 is not the number of successful MQTT connections.

Successful-CONNACK latency281 samples: mean1113.530ms, p991896.240ms, max2160.786ms.
Ramp+drain10.652s;281/10.652=26.379/s is this failed ramp's success ratio over time,
NOT a measured maximum sustainable connection rate. Schedule lag mean1.442ms,
p992.082ms, max12.093ms. Load CPU averaged0.091core and peakRSS3.14MiB.

Broker totals over active client window21.009s (20sample rows/19full intervals):
CPU mean3.345cores, highest interval4.975cores out of8; RSS peak446.90MiB;
FD peak884; server ESTABLISHED peak772 includes pending/unaccepted handshakes.
CPU interval readings are sampled, not instantaneous peaks. Device-wide26sample/
25.629s disk busy average50.68%, highest interval73.73%, writes5.53MiB/s,
840.9writeops/s, reported write await0.695ms and flush530.9/s /0.046ms average.
Device statistics cannot identify per-WAL fsync latency or establish a disk limit.

Stopped immediately after this failed cohort: no20000..100000 plateaus, no2000/
4000cases, no10000TCP/10000messages/s QoS0/1/2 message case. Therefore no target-load
message TPS, message mean/p99 or loss result exists. Requested message case remains
5000pairs/128B/aggregate10000publish/s with unchanged warmup/measure/drain settings.
All test Broker/load processes stopped; source port range restored10240..65535→
original32768..60999. Files retained. Controller directory read-only audit after run:
each controller.bbolt1658880B +required67B +lock0B +raft-wal/, NO SQLite files.

## Diagnosis and remaining work

No ERROR/Raft fatal/checksum/poison/storageIO/timeout found in the239-line new node
logs. Early rejection categories are DurableWaiting/BackgroundOperations then
DurableWaitQueueFull, background Work/reacquire, followed by quorum/H2 failures.
JournalOlderVersion is RAM replica stale-version protection, not disk journal error.
Each node's wait-queue rejection counter reached at least8192, background Work and
reacquire counters at least2048. Logs print powers of two: lower bounds, not totals.

Known unchanged code limits: each data node32active +32waiting/750ms; leader
Applied32 immediate admission versus up to96active across3nodes; Control.write32
shared by business commands/heartbeats until commit completes. Background Work
errors can trigger new Acquire/epoch/quorum work, amplifying load. PINGREQ currently
also defaults to commit_cluster, consuming replicated transaction capacity. Code
paths prove these mechanisms; logs lack request-correlated timing/admission totals,
so the first saturated component and causal shares are not yet measured.
Next work: freeze and fix bounded control batching/admission, reserved heartbeats,
transient-vs-poison recovery and unnecessary PING commits; retain all-live visibility,
lease/epoch fencing and durable ACK semantics. Repeat only the first1000/s cohort
after fixes, stop upon failure. Backend replacement alone did not resolve overload.

## Evidence and reproduction

`raw/c1000-client.json`, `raw/resource-summary.json`, `raw/c1000-state.json`, and
`raw/lyrnormq-bbolt-c1000-failed.tar.gz` contain client output/server sampled resources,
commands and warning logs. No credentials or MQTT payloads are archived.
Helpers: scripts/cluster_scale_server.py, cluster_scale_client.py and
cluster_scale_limits.py. Offline upgrade: stop old Phase7j Controller, run
`python3 scripts/export_control_sqlite.py --data-dir PATH`, then start new Controller
with same identity/data directory. Pre-Phase7j SQL-Raft directories require upgrading
through Phase7j first. Source databases/export are retained as inert backup;
controller.bbolt.required prevents missing Bbolt from reimporting stale backups.

# Three dual-role nodes: failed run and diagnosis / 2026-11-03

**Correction: this run omitted --max-sessions. Default1024/node invalidates the
10k..100k capacity configuration. Do not cite this run as a valid maximum-capacity
measurement or as proof SQLite caused QoS0 setup failure.**

Broker47.84.224.186/private172.16.240.180; load47.84.241.200/private172.16.240.181.
Each8logicalCPU/~15GiBRAM, Ubuntu26.04.1x86_64. Three processes each cohost one
Controller and one RAM Broker, separate peer ports/node IDs/data directories;
4Tokio workers/process. All three run on one machine, not physical HA domains.

Unchanged Broker ELF SHA2560a20f4c91566ab92c258ab5a32bdc83b2a110de5ce3e333883e0f726c41575e2,
production sourcef3b6181. New connection-tool source46f7ecf; locally Zig-built ELF
SHA25648f09c86a9278120debd46430295156bc9310d50d105bfd37bbb0d94f6a54122.
Message tool unchangedf955aa4d0843c48f0a689c5ea8ade782910709ae145a23afe77fdf494e19d7ef.
No Rust compilation on servers. Loadgen package51tests passed including3new raw
connection/nearest-rank/wire-CONNACK+PING tests; strict workspaceClippy/fmt passed.

Fresh cluster for every case; no sample reuses prior ownership state. Per-Broker
connection cap raised20000→50000; Raft/data/admission resource limits unchanged.
Load processNOFILE250000; ephemeral source ports temporarily10240..65535 to allow
100k evenly across3destination ports (original32768..60999 permits~84k tuples).
Driver restores original source-port range in finally. No actual secrets in evidence.

## Connections

Rust/Tokio MQTT5 raw wire, clean-start, KeepAlive0. Three endpoints round robin.
Absolute intended arrivals1000/2000/4000 persecond; up to2048pending handshakes,
15s connect+CONNACK deadline. Queue-full arrivals count as not_started, not successes.
10k new arrivals then10s hold and256-concurrent PING verification before advancing
to20k..100k. Failed first plateau stops that rate; higher levels explicitly not
attempted. Exit0means driver completed, plateau_passed=false means capacityFAIL.
Success latency covers TCP+CONNACK only, not MQTT message delivery.

|Intended rate|Planned|Actually attempted|CONNACK success / alive after hold|Not started|0x88Unavailable|EOF|
|---|---|---|---|---|---|---|
|1000/s|10000|7413|94 /94|2587|7089|230|
|2000/s|10000|6184|60 /60|3816|5851|273|
|4000/s|10000|4463|86 /86|5537|4221|156|

All first10k plateaus failed under the recorded configuration.20k..100k were not
attempted. Since session limits were misconfigured, these are burst/admission
observations only, not a valid10k..100k capacity verdict.
Finite client concurrency clipped arrivals while server responses were slow; these
are intended rates, not achieved rates. Raw schedule-lag and latency/accounting
appear in per-case JSON. Successful CONNACK/s over ramp+drain6.803/7.252/18.774 are
NOT steady-state maximum connect rates, because rejections dominate these bursts.

Server diagnostics include DurableWaitQueueFull/Timeout and background reacquire;
log output uses exponential sampling, so count log lines is NOT total rejection
count. Client errors and source evidence establish server admission failure, not
which internal component is the primary service-time bottleneck. No claim of disk
hardware saturation or solely SQLite causality without profiling.

## Messages

MQTT5;5000publishers+5000subscribers=10000TCP,128B total payload, cross-node pairs,
10000aggregate publish/s (and expected10000unique delivery/s, not summed as20000TPS),
inflight1/pair, setup concurrency8, warmup3s, offering60s, drain30s, whole-case bound420s.
Publisher pairi uses nodei%3; subscriber uses next node. Each QoS starts fresh.
Latency uses one loadgen monotonic epoch and exact nearest-rank percentile;
application QoS2 delivery recorded atPUBREL. Setup/failed-warmup cannot produce
valid target-load mean/p99 and must be reported unavailable, not zero.

## Stop and direct cause

User stopped further load after failure. QoS1/2 never started. All load/Broker
processes verified exited; ephemeral port range restored32768..60999. Preserve the
four failed runs at /opt/rmqtt-cluster-scale/20261103; do not restart as a capacity
acceptance result.

QoS0 setup ran371.158s, exit1 peer closed connection, no messages measured. At288s,
management API online sessions861/861/860, messages_in/out=0. Last pre-exit TCP
counts1025/1026/1025 include a few unadmitted handshakes. Actual command lines set
max_connections50000 but omitted max_sessions; default1024/node is independently
bounded by existing actor capacity regression.3079 persisted durable owners are
not3079 admitted sessions. This configuration error is the direct explanation for
setup termination at about3072 sessions. Future commands must set max_sessions as
well as max_connections and preflight every target against all resource limits.
No new load was started after the user stop; mean/p99 for target message load N/A.

## Timing correlation

Client failure detected2026-11-03T08:56:09.003931Z. The38 logged150ms Raft timeouts
on node1 occur AFTER client exit while the driver was stopping nodes. They are
shutdown noise, not evidence of the original failure. The helper's subsequent
PID-identity refusal occurred during process exit; all processes are gone, not an
unresolved running load. No arbitrary PID was killed.

Earlier1000/2000/4000 bursts succeeded far below session cap. Logs prove
DurableWaitQueueFull/Timeout with32active+32waiting/750ms, not that enlarging that
budget fixes service capacity. Background poison/reacquire contributes follow-on
traffic; full live latency attribution has not yet been instrumented.

## Offline decomposition, no MQTT load

A locally compiled Rust diagnostic reuses unchanged production model/repair source.
It reads the stopped Controller snapshot, creates fresh isolated SQLite files, and
measures30 repetitions per stage on the same server. Original database read-only;
no production state changed. WAL/FULL retained. Size variants retain prefixes of
real durable owners and rebuild routes, not synthetic randompayloads. Nonheartbeat
CPU stage applies a no-op Expire to exercise clone/rebuild/size-check/state-encode;
this is not the exact full connect transaction. Metadata-read CPU stage measures
clone+JSONencode+JSONdecode, excluding actual network/ReadIndex/queueing. SQLite
stage preencodes changing revisions, so excludes serialization; includes filesystem
and default WAL checkpoint behavior. Stage elapsed times are not sampled live
percentages, fsync-only timings or maximumTPS.30-sample p99 equals maximum.

At3079owners/1447routes/state604765bytes:

|Isolated stage|Mean|p99|
|---|---|---|
|Nonheartbeat state clone/apply/rebuild/encode, noIO|7.671ms|10.105ms|
|SQLite full-state upsert WAL/FULL, preencoded|4.683ms|8.248ms|
|Full metadata clone/encode/decode, no network|9.421ms|12.192ms|

At1000owners, the same stages2.284/3.222/3.018ms; empty-state1KiB SQLite write
2.490ms. SQLite synchronous commits cost time, but CPU/global-size amplification
also grows with directory size and cannot be fixed by swapping database engines.

Confirmed implementation amplification:

1. Every Store::apply clones and serializes all State and upserts one blob, even
   when the entry is just a node Heartbeat. Single DB mutex also serializes Raft
   append/vote/commit/state IO with this work.
2. Each nonheartbeat Metadata::apply additionally clones the directory, rebuilds
   all routes and serializes everything to validate8MiB. Snapshot encode repeats.
3. Broker barrier waits all live nodes applied; it repeatedly fetches WHOLE metadata
   after20ms sleeps. ReadRequest::Metadata requires ensure_linearizable ReadIndex,
   clones/encodes the directory and the client decodes it. This is synchronous
   control-plane polling, separate from removed100ms message-forwarding polling.

Whole-run resource samples include setup/drain/idle: mean combinedCPU2.99/2.00/
1.72logicalcores for connection bursts,4.03cores for QoS0; peaks6.44/6.67/6.57/6.04.
QoS0 summed RSS peak1385.5MiB; RSS sums may double-count shared pages. This is not
proof disk saturation. Raw counters in server-failed-run-evidence.tar.gz.

## Priorities (not implemented by this diagnosis)

Correct benchmark session limits/preflight first. Replace full-directory barrier
reads with lightweight applied-revision events/progress; preserve required fencing
and routing visibility. Incrementally update ownership/routes/accounted bytes.
Keep Raft log/vote/commit durable; apply committed entries in RAM with bounded
periodic durable snapshots plus tested replay, instead of per-entry full-state
persistence. Split CPU work from SQLite IO lock and evaluate bounded group commit.
Do not disableFULL or remove consensus safety merely to improve numbers. A future
RocksDB/log backend alone will not remove these CPU/serialization/barrier costs.


Historical offline diagnostic now carries frozen model/repair source copied from
f3b6181 so subsequent incremental-model changes do not silently alter the baseline
CPU workload. Its full-State clone/apply/encode stage is historical, not the new
Phase7h RAM-apply hot path.

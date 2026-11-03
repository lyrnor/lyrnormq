# No-quota cluster deployment/retest — 2026-11-03

**Failed first1000/s→10000 cohort; stopped immediately.** No larger cohort/rate or
message throughput case was run. No successful-CONNACK latency samples exist.

Production source950fa70, integration documentation27f800f. Linuxmusl release built
locally with cargo zigbuild; SHA256
 e8f2b38bd16e960d711b517a2eb648307cbb5f019f6cf55871165b9117f4ec8e.
Deployment verified same SHA. No compilation on server. Pre-deployment verification
already1017Rust tests/1externalPGignored,fmt/strictClippy,14native process tests;
unchanged source was not needlessly rebuilt/retested before deployment.

## Deployment and comparable configuration

Broker47.84.224.186/private172.16.240.180; load47.84.241.200/private172.16.240.181.
Both8logicalCPU/~15GiB; same Broker Linux7.0.0-38-generic, ext4/vda shared by all
3processes. Three cohosted Controller+RAM data nodes,4Tokio workers/process(12total),
round-robin MQTT endpoints172.16.240.180:18831/18832/18833. This is same-host cluster
measurement, not multi-machine availability certification.

Fresh Broker /opt/rmqtt-quota-free/20261103 and load
/opt/rmqtt-quota-free-load/20261103. All earlier deployments/data retained. Same
max_connections/max_sessions50000/node,state256MiB,metadata16711680B,physicalrecord
101024,FD250000,handshake1024,authenticationtasks128,authrate0. Cluster transaction/
aggregatebody/Controllerwrite/watch/Applied/RPC concurrent admission quotas removed;
protocol/record/cache bounds and separate connection admission settings still exist.
No source-port sysctl change:32768..60999 before/after.

Identical Rust probe SHA256
 d0acf9899b9ea9d34fd68c8a5234b4f741b97bef816987b00b49f4ad6a28c2ae,
10000absolute scheduled arrivals at1000/s,2048pending handshake window,15s client
per-connect timeout,10s hold followed by validated PING, one requested plateau.
Scripts in this folder reproduce the commands (choose a fresh directory per run).

## Measured result

| Item | Observed |
|---|---:|
| Scheduled / attempted / not started |10000 /9857 /143|
| Successful CONNACK / alive after hold-PING |0 /0|
| CONNACK0x88 / EOF |4795 /5062|
| Ramp plus drain |10.000684s|
| Mean/p99 successful-CONNACK latency |Unavailable:0samples|
| Aggregate Broker average CPU |1.688cores of8|
| Highest sampled aggregate CPU interval |2.717cores of8|
| Aggregate Broker peak RSS |467.414MiB|
| Aggregate FD / server ESTABLISHED peak |2154 /2047|
| Load average CPU / peakRSS |0.088core /4.895MiB|
| Scheduling lag mean /p99 /max |0.703 /1.320 /9.159ms|

143arrivals were skipped when the probe's2048pending-handshake window was full;
this failed run did not deliver a complete10000attempt1000/s offered cohort.
All9857attempts are accounted:4795CONNACK errors+5062EOF.0success is a failed setup,
not0latency/0loss and not a sustainable throughput result. Hold had no surviving
connections to probe. No QoS0/1/2 messageTPS/mean/p99/loss measurement was attempted.

Resources:28server rows,20active rows/19full CPU intervals within approximately
20.008s client process window. CPU percentages are converted to single-core units;
peaks are sampled intervals, not instantaneous peaks. TCP count includes pending
and pre-CONNACK sockets, not successful MQTT clients. Wall-clock host alignment is
coarse; all client connection latency measurements use the probe's monotonic clock.

Whole-device27.643s sampling:busymean62.364%,writes8.052MiB/s,1026.273writeops/s,
reported writeawait0.768ms. Device-wide averages cannot establish per-WAL fsyncp99
or prove a storage bottleneck. No CPU saturation was observed in sampled intervals.

## Failure evidence and remaining diagnosis

Final Controllers allterm1/leader1/applied7487,metadataowners3574. Owner count does
not equal successful TCP/CONNACK count. Logs still show BackgroundWorkReply and
BackgroundReacquire >=1024 EACH per node. Node2 QuorumPeer>=512/QuorumInsufficient
>=256; QuorumNotReady8/4/8 and a few stale-version guards. These logarithmic counts
are LOWER BOUNDS, not exact totals. Initial vote warnings occur at startup, not
proof of failure during measured load. No WAL/bbolt checksum or storage-IO failure
appears in collected logs.

First background Work errors were2.009–2.010s after client process start, close to
the ordinary control RPC2s deadline. This supports investigating control mutation/
reply deadlines and resulting recovery amplification; it does not prove which
stage first stalled. RecoveryRequired reason and Acquire stage timing are still
missing. Removing quotas did not resolve the failure. Do not blame Controller
SQLite (there is no Controller SQLite runtime) or assert bbolt/disk is the cause.

Next diagnostic work: bounded per-stage/recovery-reason instrumentation and finite
local reproduction of unknown ownership outcomes; review repeated epoch recovery
and linearizable read/commit queues before any additional server pressure.

Both test Broker and load processes verified exited. Binary/data/evidence remain
on servers. raw/ includes actual commands,client accounting,resources,Controller
state,node logs in server-evidence.tar.gz and fixed-cardinality diagnostic lower
bounds. No secret token/key file or MQTT message payload is archived.

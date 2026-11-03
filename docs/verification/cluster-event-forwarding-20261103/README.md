# Phase7g — event-driven replicated forwarding

Contract: [Phase7g](../../contracts/phase7g-event-forwarding.md).
Baseline: [RAM100ms poll measurements](../cluster-memory-20261103/README.md).

No replication, quorum, recipient receipt, snapshot or wire schema change. Data
replicas remain volatile RAM. Control heartbeats/leases still use time because
they detect failure; periodic full-directory message discovery is removed.

Acceptance records, source commits and final measured results follow below after
integration. Source compilation occurs locally; no Rust compilation on servers.

## Implementation and acceptance

Integrated production a0939a8/f3b6181; isolated runtime originals5f4908d/37b53c8;
quality original972d62e/75a53f1, integratedfb6d84d/980d408. No main merge/push.
Idle-no-Work regression failed old polling. Review found16 hot keys could occupy
all16 workers forever; deterministic RED reproduced starvation of17th key, then
32-Work quantum plus immediate requeue fixed it without successful-path sleeps.

Final immutable native SHA256:
`fd92d4522320269bee125bf89163f0107c64ea5ad2ece4927d1a30e9c994a0e2`.
Final local Zig-built statically-linked x86_64 musl ELF SHA256:
`0a20f4c91566ab92c258ab5a32bdc83b2a110de5ce3e333883e0f726c41575e2`.
Linux build command: `CARGO_TARGET_DIR=target/cluster cargo zigbuild --release --locked --offline --target x86_64-unknown-linux-musl -p rmqtt`.
First invocation lacked sandbox access to Zig's cache, failed before compile; retry
with cache access completed. Native environment macOSarm64/Rust1.98.1/Zig0.16.

Final event wire10/10 passed66.998s: bothMQTTversions, FIFO/QoS0/1/2, idle publish,
failed RPC retry without new publish, committed delivery response lost without
redelivery, owner loss and Controller readiness recovery. Earlier native iteration
also passed10, but final acceptance uses the final fairness binary. Old baseline
initial readiness oracle mistakenly used Dashboard process readiness; corrected
oracle verifies actual MQTT admission. Both initial setup/oracle failures retained,
not reported as production bugs or hidden by repeated retries. No CI-host run claim.

Scheduler state is bounded100k keys, at most16 workers and one timer per pending
key. Notifications carry wakeups; keyed state carries work across races. Failure
backoff100–800ms is a per-key retry timer, not periodic whole-directory polling.
Actors schedule after quorum attempt before replying, so caller cancellation and
unknown outcomes cannot silently discard forwarding responsibility. Each key
retains FIFO; activation tokens prevent an old ACK clearing a newer Release.
Metadata heartbeat updates skip extra forwarding reconciliation; actual ownership,
node incarnation, repair and readiness transitions can reconcile relevant work.

Remaining limits: existing metadata install still clones/scans metadata and relevant
durable updates can reconcile the directory; no subscription index optimization
is claimed. Work reply body clones have finite16-worker/record bounds but are not
charged to the16MiB mutation execution semaphore. A worker panic has no additional
restart supervisor (no reachable new panic source identified). RAM does not gain
disk recovery. Retry timers and control-plane lease/heartbeat time remain required.

Final workspace915 passed/1 externalPG ignored; fmt and strict all-targetClippy
passed. Runtime unit module now covers idle, immediate FIFO drain, coalescing,
cancelled reply, unknown outcome, activation-token race, bounded overflow,
readiness/metadata triggers, retry backoff and16-hot/17th-cold fairness.

## Comparable server load

Fresh six-process cluster /opt/rmqtt-cluster-events/20261103 on47.84.224.186,
three Controllers and three RAM data nodes. Same privateMQTT endpoints
172.16.240.180:18831/18832/18833; loadhost47.84.241.200/172.16.240.181.
Old RAM deployment stopped only after confirming no active MQTT clients and
verifying new ELF hash; directories preserved. Same loadgen ELF as baseline
(f955aa4d0843c48f0a689c5ea8ade782910709ae145a23afe77fdf494e19d7ef).
Both machines8logicalCPU/~15GiB RAM, Ubuntu26.04.1x86_64. Same-host multi-process
replicas are not independent physical failure domains.

30pairs/60TCP, publisher rotates nodes, subscriber next node, MQTT5,128B,
rate100/s, inflight1, warmup3s, measurement30s, drain30s, setup concurrency1.
Per-case exact args/timing/denominators preserved in JSON. No maximumTPS claim.

Final native RAM17 passed117.719s after event10; same immutable binary hash checked
again. Includes both versions/QoS recovery, offline takeover, blank replica blocked,
maintenance repair in same process, interrupted repair and whole-group loss refusal.
No retries or edits to these existing gates. Native functional suites completed
independently of remote load tests.

|QoS|Planned / sent / unique received|Final source ACK|Offering delivery TPS|Previous p99|Event-driven p99|
|---|---|---|---|---|---|
|0|3000 / 3000 / 3000|N/A|100.000|107.115ms|5.219ms|
|1|3000 / 3000 / 3000|3000|100.000|106.881ms|5.668ms|
|2|3000 / 3000 / 3000|3000|100.000|110.069ms|8.671ms|

All cases: no missing sends/deliveries, duplicate/invalid payload or failed pairs;
QoS1PUBACK andQoS2PUBCOMP complete. Both baselines use RAM replication, same
loadgen/network/topology/settings. This is one offered100/s sample, not maximumTPS
or a sustained production latency guarantee. Exact nearest-rank send→application
delivery timing uses one loadgen process clock; no cross-host clock subtraction.

Resources:269 one-second samples spanning269.478s include startup/setup/warmup,
measurement/drain and idle. Sum of six-process CPU mean49.47%/peak148.08% of one
logical core; summed RSS peak220.75MiB (data-process sum119.47MiB). SummedRSS can
count shared pages multiple times. Whole-run CPU means are not matched active
windows: this sample does NOT establish reduced CPU use versus baseline39.37%.
Data dirs again contain onlydata.lock/memory.identity. All endpoints observed20
connections each; final Controllers applied3598/revision546 at separate sample
instants, not synchronization-lag certification. Six-process fleet remains running.

Primary remaining work: higher offered-rate/longer-load profiling and metadata/
serialization optimization. No unrequested new capacity sweep was performed.

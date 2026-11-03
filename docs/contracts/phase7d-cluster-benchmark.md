# Phase7d topology comparison and benchmark contract

Compare three combined Controller+Broker processes against three Controller-only
plus three Broker-only processes on the same Linux host, driven from a separate
host on private addresses. This is process topology comparison, not three or six
physical machines. Both use identical fixed RF3/W2 durable semantics, payload,
connections, window, protocol and load durations. No durability reduction for speed.

Combined CLI: --cluster-config BROKER_JSON --cluster-controller-config CONTROLLER_JSON.
Existing role configs and journals remain unchanged. Validate both before startup:
roles Broker/Controller, same cluster ID/controllers/token/TLS/lease policy,
different node IDs/listen endpoints and non-overlapping canonical data directories.
Controller bootstrap remains first-controller only. Same runtime hosts both services;
either service failure terminates the process, shutdown drains both. No new role in
Raft metadata, repair, storage or wire protocol. Cluster commands/export reject the
additional option. Preserve default single-role behavior and credential redaction.

Loadgen additive --addrs comma-separated numeric IP:PORT list, max32, distinct;
--addr remains old single endpoint and cannot combine with --addrs. Empty list
uses address as before. New RunConfig addresses Vec<SocketAddr>, serde default.
Pair i publisher endpoint=i%N, subscriber=(i+1)%N; N>=2 ensures every pair crosses
nodes; pairs multiple of N ensures uniform publisher/subscriber connections. Report
must retain endpoint list/mapping. Preserve one process monotonic clock, absolute
rate scheduling, payload validation, ACK semantics, exact percentiles, bounded
samples and transparent unsent/undelivered/unconfirmed counters. No hidden retries,
rate reduction, coordinated-omission correction or doubled ingress+egress TPS.

Run all QoS0/1/2, MQTT5 main load and MQTT3.1.1 interoperability. Run same measured
workload on both topologies; fixed-rate and bounded closed-loop pressure separate.
Report offered/attempted/unique/completed TPS, source confirmation, duplicates,
payload errors, p50/p95/p99, actual client lag and server/client CPU/RSS. Queue/drain
loss means observed missing messages after finite drain, not proof of permanent loss.
Control synchronization probe measures SUBACK-to-cross-node-delivery observation
latency and separately samples Raft/status; do not label it pure Raft network latency.
Independently check raw samples and per-endpoint balance. Preserve failure reports.
No performance claim from single-host fault tests or suite wall-clock durations.

Coordinator owns bin/rmqtt, docs, workspace integration/deployment. Loadgen owner
owns bin/rmqtt-loadgen only. Quality owner owns tests/cluster_topology* and
scripts/cluster_benchmark* only. Writers use isolated branches/worktrees, commit
with tests and limitations. No concurrent shared writes or production log changes.

Benchmark settings clarification before formal comparison: combined processes use4
Tokio workers each; separated Controller/Broker processes use2 each, total12 per
topology on the same8vCPU host. Both set max-auth-tasks128, handshakes1024 and no
admission-rate throttling. Initial default-auth-limit4 preflight failures remain
separate from final runs. CPU/RSS windows include connection/warmup/drain/report
unless explicitly aligned; sampled apply/revision skew is an index difference,
not time-based Raft latency or business operations throughput.

Setup clarification: additive --setup-concurrency1..1024 (default32, serde default)
bounds concurrently active pair setup before the global warmup barrier. No silent
connect retries. Final comparison uses1 to separate steady message pressure from
burst connection admission; 30-pair burst preflight is retained as a distinct failure.
Report config includes setup concurrency. Does not change messages or ACK accounting.

Quality also owns the small .github/workflows/ci.yml addition invoking dual-role
blackbox and controller-probe unit tests. Local execution does not imply hosted
Actions execution or performance CI acceptance.

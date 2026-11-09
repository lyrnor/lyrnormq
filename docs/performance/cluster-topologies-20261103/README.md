# Cluster topology comparison — 2026-11-03

Historical pre-fix comparison. Subsequent deployment and admission fixes are in
[Phase7e evidence](../../verification/cluster-backpressure-20261103/README.md).

**Performance gate FAILED for both topologies.** All six 60-connection main cases
per topology failed during warmup; there is no measured cohort or valid capacity
TPS/p99 from those runs. The smaller baseline passed without observed missing,
duplicate or client-detected corrupt messages. No maximum-TPS claim follows.

## Measured comparison

Small closed loop: 6 pairs / 12 TCP connections, 4 per data node, 128-byte payload,
inflight1 per pair, 10 seconds offering after 3 seconds warmup. TPS below counts
unique deliveries inside that 10-second window; full cohort counts include drain.

| QoS | 3 combined processes TPS | Delivery p99 ms | 6 split processes TPS | Delivery p99 ms | Cohort deliveries (combined/split) |
|---|---:|---:|---:|---:|---:|
| 0 | 49.2 | 215.388 | 49.2 | 166.897 | 498/498 |
| 1 | 49.4 | 144.567 | 50.2 | 140.627 | 500/508 |
| 2 | 39.1 | 193.477 | 40.4 | 193.164 | 397/410 |

Fixed-rate baseline: 30 messages/second for 30 seconds, same 12 connections.
Each topology/QoS delivered all 900 attempted messages, with zero duplicates and
zero missing source confirmations for QoS1/2.

| QoS | Combined p99 ms | Split p99 ms |
|---|---:|---:|
| 0 | 133.582 | 138.096 |
| 1 | 139.131 | 136.997 |
| 2 | 151.812 | 166.213 |

MQTT3.1.1 interoperability passed all three QoS levels in each topology: each
case delivered all 150 measured messages. MQTT5 is used for main/baseline loads.
All successful measured cohorts were independently reconstructed from raw samples.

## Resource observations

These are whole monitored phase resources, including setup, warmup, drain, idle
and visibility probes; not strictly offering-window utilization. CPU is in logical
cores (1 core=100% process CPU), and RSS is a sum across processes.

| Phase | Mean CPU cores | Peak CPU cores | Peak aggregate RSS MiB |
|---|---:|---:|---:|
| combined | 0.597 | 1.325 | 142.9 |
| separate | 0.521 | 1.103 | 195.3 |
| combined-high | 0.607 | 3.013 | 257.4 |
| separate-high | 0.552 | 2.666 | 427.3 |

Independent TCP samples observed baseline 4/4/4 active connections in both
topologies; the high-load cases reached 20/20/20 briefly (5 samples combined,
4 split), which is not sustained load acceptance. Per-process resource data is
in [summary.json](summary.json) and compressed raw resource records.

## Control and route observations

Thirty-second Controller probes used 10ms polling. Before/after status remained
leader1/term1 in both runs. Both followers matched every observed advance, with
zero censored advances or read errors. These probes overlap different portions
of connection/setup traffic, so their percentile differences do not establish a
controlled Raft performance improvement.

| Topology | Observed apply advances | Follower2/3 observed p99 ms | Metadata advances | Metadata follower2/3 p99 ms |
|---|---:|---:|---:|---:|
| combined-high | 455 | 10.249/1.443 | 115 | 10.281/2.996 |
| separate-high | 409 | 0.780/0.882 | 85 | 0.169/0.309 |

Sub-10ms values contain sequential read/sampling skew and do not prove sub-10ms
replication. Zero delay can mean the follower was already observed ahead. Raw
samples/matches are retained in each phase control-catchup.json.gz.
Final [snapshots](server/replication-final.json) show all three Controller metadata
revisions matching, with all three data nodes reporting that same applied revision
in both topologies. Sampled apply indices may differ by one due to sequential reads.

| Topology | Visibility trials | Subscribe→SUBACK p99 ms | SUBACK→first remote delivery p99 ms |
|---|---:|---:|---:|
| combined | 30/30 | 272.712 | 89.711 |
| separate | 30/30 | 281.248 | 50.050 |

The subscribe confirmation includes durable state and the existing barrier for
Broker metadata application; first delivery adds the durable forwarding scheduler.
Neither column isolates pure metadata transfer or Raft latency.

## Deployment and validation

The split-role test deployment remains running under
`/opt/lyrnormq-cluster-benchmark/20261103/separate/`; private MQTT endpoints are
`172.16.240.180:18831`, `:18832`, `:18833`. It is not a systemd/autostart deployment.
Load processes have exited. Peer/admin token files remain private on the server;
they are excluded from this report, as are runtime SQLite/queue files.

Local full workspace tests: **893 passed, one live-PostgreSQL environment ignore**.
Strict all-target workspace Clippy and fmt pass. Independent topology/probe tests
passed 9 cases; CI invokes those cases but no hosted Actions run is claimed.
See [Rust log](workspace-tests.log), [Clippy](clippy.log), [CI commands](ci-local.log),
[combined raw audit](independent-combined-verify.log) and
[split raw audit](independent-final-verify.log).

The higher-load gate remains open. Untyped data-path errors need separate admission,
quorum, revision and readiness diagnostics before fixes. Scheduling, sync/serialization
and quota costs need profiling while preserving W2 and atomic outbox/receipts.

Results are collected from actual processes and private-network MQTT traffic.
The comparison uses one8vCPU broker host47.84.224.186/private172.16.240.180 and
one8vCPU load host47.84.241.200/private172.16.240.181, both Ubuntu26.04.1LTS.
This compares3 combined-role processes against6 split-role processes, not physical
3-machine versus6-machine clusters. All three data journals share the same ext4
/dev/vda3 block device; physical storage throughput and failure domains are shared. All internal peer RPC stays on loopback;
external MQTT binds only the broker private address, ports18831/18832/18833.

Both topologies run fixed RF3/W2 durable mode with WAL/FULL journals. Controller
Raft contains metadata, not MQTT message bodies. Combined processes use4 Tokio
workers each; pure roles use2 each, giving12 total workers in either topology on
the same8vCPU hardware. Blocking-pool threads are additional and monitored. Both
use max-auth-tasks128, max-handshakes1024 and disabled auth-rate throttling. Rules
and access configuration databases are empty/private, with default anonymous access.

The static Linux amd64 musl Broker and loadgen are built locally with cargo
zigbuild, never on either server. Broker SHA256:
`8a63ca6216d1a57266ad3f19c20b62727843c3d2b5012721705f515114822f2c`.
Final bounded-setup loadgen SHA256:
`f955aa4d0843c48f0a689c5ea8ade782910709ae145a23afe77fdf494e19d7ef`.
Production Broker source d7e16ed; final loadgen source b650e39. Later changes to
probe tests/docs do not alter those binary sources. Remote checksums are verified.

## Workloads and accounting

128-byte total payload includes the48-byte instrumentation header. Pairi publisher
connects to endpointi%3 and its subscriber to(i+1)%3, so every message crosses data
nodes. One loadgen process owns both sockets and one monotonic clock. Final setup
concurrency1, no connect retries; warmup3s and drain30s. Counter/latency raw samples
are retained, independently checked with analyze.py, and failures are preserved.

- Main admission/message gate:30pairs/60TCP connections (20/node), fixed100/s,
  inflight1,30s offering; separately closed loop inflight4,30s offering. AllQoS.
- Stable baseline:6pairs/12TCP (4/node), fixed30/s for30s, inflight1. AllQoS.
- Small closed-loop baseline:6pairs, inflight1,10s. This is bounded-window throughput,
  not maximum capacity or coordinated-omission-corrected latency. AllQoS.
- MQTT3.1.1 interoperability:6pairs,30/s,5s, allQoS. Main loads use MQTT5.

Offering TPS counts unique subscriber deliveries inside the configured window;
cohort counts include finite drain and have their own longer denominator. Neither
adds ingress and egress or counts MQTT packets as application messages. P99 is
publisher operation start to unique subscriber delivery, QoS2 observed atPUBREL.
Source confirmation is PUBACK/PUBCOMP, never intermediate PUBREC. Missing or failed
runs have conditional/null latency, not fabricated zeros. Pre-warmup failures cannot
support a zero-loss assertion because no measured cohort was sent.

## Synchronization and resources

Controller probes read each private metadata.sqlite in read-only mode, recording
read order/start/end. First observed leader apply/revision advances are matched to
first follower observations>=value, with censored advances and already-ahead flags.
10ms polling and sequential SQLite reads bound interpretation; these are observed
catch-up delays, not pure Raft commit/network latency. Leases also generate log
entries. Controller leader/term must be checked externally before/after the probe.

The route probe separately measures SUBACK to first application message from a
remote data node. It repeats a QoS0 probe every200ms until visible. This metric
includes routing, storage, network and scheduling; repeated probes are not a loss
benchmark. Throughput uses the independent instrumented Rust client instead.

Server CPU/RSS sampling uses/proc with a monotonic clock. CPU100%=one logical CPU.
Aggregate RSS sums process resident sets and can double-count shared pages. Phase
resource summaries include setup, warmup, failures, drain and idle; they are not
strictly30-second offering-window CPU measurements. Actual established MQTT sockets
are counted independently by private listener port. Client resource logs are whole
process lifetimes, including report generation. No physical memory/TPS extrapolation.

## Reproduction and limits

Use server.py to create separate fresh directories/configs and start/stop either
topology. LYRNORMQ_BENCH_ROOT and LYRNORMQ_BENCH_BIN support a new server artifact root;
LYRNORMQ_BENCH_ROOT selects a new client output root. Refuse to overwrite prior output.
Start all members before load. client.py main/baseline/smoke runs workloads and
retains exit codes/reports/raw data. Stop and preserve the high-load phase, then
start fresh topology state for the baseline, identically for both topologies.
The helper's optional scale mode was skipped; no300-connection result is claimed.

Do not replay archived configs into a live deployment: they contain historical
absolute artifact paths. Generate new configs in an isolated root. Never upload
private SSH keys or include peer/admin token files in reports. Peer connections on
multiple physical machines require the existing mTLS contract; no TLS weakening
was introduced for this single-host loopback-peer experiment.

## Current reviewed findings

The combined topology main60-connection gate failed during warmup for all six
fixed/pressure cases; no measured cohort exists. Thus delivery/ACK latency is N/A,
not zero, and zero not_delivered cannot establish losslessness. Default-auth-limit4
preflight refusal and the first unpaced route-probe broken pipe remain separate
failed evidence. The final paced route probe passed30trials. ActualTCP sampling
observed60connections balanced20/20/20 briefly, not sustained gate acceptance.

Independent raw reconstruction (without importing the metrics code or analyze.py)
validated all six latency series, offering/late counters, ACK/completion counts and
TPS for the six successful baseline cases. Fixed30/s cohorts each delivered900/900;
no duplicate or client-detected invalid payload. Raw records retain timing metadata,
not message bodies, so independent reconstruction cannot re-decode payload bytes.


Static review identifies candidate data-path limits, not a measured root-cause proof:
Durable operations are capped32/node and Journal DB operations32/node;16 background
jobs share admission with normal traffic, and the Journal serializes SQLite work
through one connection. Work scheduling sleeps100ms between rounds and processes
one head intent per record/pass. WAL/FULL, complete record encoding and per-write
quota scans amplify IO. Generic MQTT0x83/0x8b errors do not identify the rejecting
branch. Before changing limits or scheduling, separately count each admission,
quorum, revision and readiness rejection and profile sync latency. Preserve W2,
receipt/outbox atomicity and generation fencing in any future optimization.


Metadata status clients counts retained ownership-directory records, not current
TCP connections (the final split status retains78 records after clients disconnect).
Use independent established-socket samples or transport management counters for
live connections. Generic owner/tombstone retirement remains a separate open gate.

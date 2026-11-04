# Phase7v — requested 2000/s,100k,300s dual-role acceptance

User pauses CPU optimization and explicitly requests exactly one aggregate2000/s
100000-connection case, held at least300s after the last successful CONNACK, with
three dual Controller/data processes on47.84.224.186 and load on47.84.241.200.
No Phase7v CPU barrier-grouping implementation, Broker code change or new build.
Reuse optimized7u binary SHAeb6f4be285dc7b8af33e820de4f8b4bed64772c9c6961a581b3423c72f4b55b4
and probe SHA556b3d560b3c79a4c77e9f06edc93052a876f6fb8a4be86eeaf163f70784b4ad.

Four probes each500/s25000,2workers,2048inflight,unique IDs/offsets and even3endpoint
selection. Shared PING=start+370s gives50s nominalramp+20sdrainmargin+300shold;
actual common hold must be independently checked>=300s, not inferred from the old
probe's hardcoded10s minimum field. Keep MQTT5 KeepAlive0 and final verifiedPING.
No business message throughput/latency/QoS claim from this connection-only case.

Reuse existing fresh-directory cluster helper/config, same limits and4Tokio workers
per Broker. Connection/session50000 per node; state256MiB/node; fixed RF3/W2 and
three Raft members. No hidden change to metadata/protocol bounds, quotas or timeouts.
Loadhost only: temporarily widen ip_local_port_range10000..65535 (one sourceIP,
three destinations; default28232x3 is insufficient). Archive original/current and
restore original in finally/postflight. Set finite250000FD/process as prior tests.

Monitor process CPU/RSS/FD eachsecond, TCP counts every5s, fresh target-only Raft
status every10s plus start/ramp/hold/end gates. Record memory availability, process
identity, failures/leader/term/applied and owners. Detect missing processes and stop
at<512MiB systemMemAvailable to protect test-host manageability, preserving FAIL/
aborted evidence. No higher load or retry after failure.

Raft success requires all3 initialized with common leader/term, actual initial
membership{1,2,3} from a checksummedWAL Membership entry applied by all3, and fresh
post-ramp/hold target-only statuses converging past a captured revision. Heartbeat
writes may make sequential applied indices differ; record target then verify each
has reached it. Cached observer status is not proof of instantaneous replication.
Preserve epochs/quorum/WAL/checkpoint semantics. Count clientattempted/success/PING,
per-node established connections throughout hold, precise duration, mean/p99/max,
resource peaks and all raw failures. Stop processes, verify source-port restoration
and archive evidence/hashes. Coordinator owns docs/harness; agents read-only review.

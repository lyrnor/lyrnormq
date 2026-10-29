# Phase3 local routing baselines

This is a bounded asyncio raw MQTT5 driver, not a saturation benchmark or production
SLA. Broker and Python clients share a loopback host. Run performance campaigns
serially after other builds/tests finish; save the immutable executable and result.
No third-party Python package is required. Python assertions must stay enabled.

```sh
cargo build --release --locked -p rmqtt
python3 benches/phase3/test_driver.py
# Smoke: small topology, two storage modes, exact counts; NOT a formal baseline.
python3 benches/phase3/run.py --binary /absolute/path/to/rmqtt \
  --artifact-source-revision COMMIT --output /tmp/rmqtt-p3-smoke.json \
  --smoke --messages 32 --warmup 8 --repetitions 1
# Formal: six cases (three workloads x two storage modes), each repeated three times.
python3 benches/phase3/run.py --binary /absolute/path/to/rmqtt \
  --artifact-source-revision COMMIT --build-profile release \
  --output /tmp/rmqtt-p3-baseline.json --messages 4096 --warmup 128 --repetitions 3 \
  --session-expiry-seconds 60 --flow-control delivery --outbound-capacity 256 --timeout 600
```

The driver works from any current directory when invoked through its file path.
The output contains every per-run result, all broker arguments (including temporary
SQLite path), requested artifact source revision, artifact SHA256, driver revision,
host/toolchain/FD details, memory/CPU sample timestamps, exact event counts,
latency distributions and median/min/max across repetitions. A final
`campaign_status: passed` is written only after all requested runs complete.
The empty `running` record is written before the first case, replacing any stale
success at the same path. An interrupted campaign retains completed run records with `running` status,
so it must not be presented as complete. OS metadata commands unavailable in a
sandbox record a reason; supply independently obtained CPU/RAM details in the
accompanying report. Linux CPU model comes from a bounded65536-character
/proc/cpuinfo read; absent/unreadable model names have an explicit unavailable
reason. Architecture is recorded separately and never labeled as a CPU model.
Do not infer the artifact source revision from the driver.

| Case | Publishers | Subscribers | Installed filters | Measured routing |
| --- | ---: | ---: | ---: | --- |
| concurrent |64|64|64|one subscriber per publication, round robin;128 TCP clients simultaneously connected|
| hot |8|32|32|all32 subscribers receive every publication on one exact topic|
| many |8|64|4096|64 exact filters per subscriber;4096 publications hit every filter once|

All cases use256-byte payloads by default, QoS1 ingress and QoS0 subscription grants,
with at most one unacknowledged PUBLISH per publisher. Formal controlled-load
measurements explicitly use `--flow-control delivery`: each publisher also waits
for all expected subscriber receipts before sending its next PUBLISH. An individual
publication Event is set only after every validated expected delivery, so no
fanout latency samples are discarded. `--flow-control ack` remains the compatibility
default and waits only for PUBACK; it can accumulate QoS0 recipient queues until
configured admission limits reject new work. These are different offered-load
policies, not a runtime optimization or comparable before/after speedup. Every run
and summary records its flow-control policy. Resource rejection fails a no-loss
measurement and preserves exact header/body/reason/expected-ID diagnostics. Topics/filters have no
wildcards; this isolates the installed-subscription count from wildcard fanout.
Subscription SUBACK and subscriber PING barriers precede warmup; warmup messages
are separate and excluded from measured distributions. The broker is fresh for
every repetition, including a new SQLite database. Clean Start is true. Session
Expiry defaults to0: this is **clean-session routing in each storage configuration**,
not a benchmark of acknowledged offline durable messages or per-message fsync.
The frozen formal profile explicitly uses `--session-expiry-seconds 60` for
persistent-session routing; report it separately and use the same value for both storage modes.
QoS0 live traffic still does not measure offline QoS1/2 durability. SQLite's existing
WAL/FULL configuration is retained; this is not a power-loss test.

Each payload encodes a unique phase/sequence plus exact padding. Every expected
subscriber checks the topic and complete payload exactly once, plus monotonic
sequence for each (topic,publisher) stream; no cross-publisher/topic order is assumed; duplicate, wrong,
missing or unexpected publications fail. All ACK identifiers/reasons are checked.
A postphase PING barrier rejects extra preceding frames. No latency sampling or
silent success on a socket write: every delivery and ACK contributes to a bounded
in-memory list. Individual latency lists are reduced after each phase; the JSON
preserves per-run exact counts and distributions rather than individual latency
values. The unit tests inject duplicates and same-publisher/topic reordering to verify
failure propagation, verify stale-success invalidation, and check ps child cleanup
on cancellation/timeout.

The throughput interval starts immediately before measured receiver/producer tasks
are scheduled and ends after their expected delivery and ACK counts are complete;
final PING and process sampling are excluded. Ingress messages/s and egress fanout
deliveries/s are separate. Latency starts at `monotonic_ns` immediately before the
publisher writes and ends at receipt of the complete frame, using the same host
clock. ACK latency is separate. Percentiles use nearest rank `ceil(p*N)` and retain
all samples. This is closed-loop load: client scheduling, Python parsing, ACK gating
and loopback IO affect the result; it does not expose open-loop coordinated-omission
corrected latency or a maximum broker capacity claim.

Broker-only RSS uses `ps -o rss=,time= -p PID`: reported KiB multiplied by1024,
at idle, fully connected/subscribed setup, after warmup, periodically during measured
work and at end. The nominal default50ms interval is a wait after each sampling
command, so actual intervals are in raw timestamps. Peak means maximum observed
sample, including setup, not the allocator/kernel process high-water mark. CPU time
is cumulative `ps time` end minus premeasure, a slightly wider window than throughput;
its resolution may produce zero for short smoke runs. Client memory is excluded.

Configured bounds and timeouts appear in each result. `--outbound-capacity` defaults
to256 and accepts1..4096. For example, a smoke ACK-only hot test with capacity1
reproduced an actual DISCONNECT header0xe0/body97/reason0x97 Quota Exceeded
in memory mode; this diagnostic does not establish the cause of an earlier run
whose packet bytes were not retained. Reproduce with:

```sh
python3 benches/phase3/run.py --binary /absolute/path/to/rmqtt \
  --output /tmp/rmqtt-cap1.json --smoke --cases hot --storage memory \
  --messages 64 --warmup 8 --repetitions 1 --session-expiry-seconds 60 \
  --flow-control ack --outbound-capacity 1
```

This deliberately overloaded command may exit nonzero; do not report it as a
successful throughput campaign. Scheduling can affect which publication is rejected.

Configured bounds and timeouts appear in each result. Formal CLI runs require
at least2000 measured publications and three repetitions; max10000 publications,
maxfive repetitions, payload at most4096 bytes, default180s per case. The formal
command explicitly allows600s because persistent full-snapshot workloads can
exceed the default; this timeout is a bounded campaign budget, not a latency SLA. Reader frames
are capped8192 bytes. TaskGroup cancels siblings on failure. Socket cleanup and
broker termination are bounded. Timed-out/cancelled ps subprocesses are killed and reaped. Smoke switches to four subscribers and at most
four publishers/four filters each and is explicitly labeled.

## Phase3a failure evidence and pilots

```sh
# Full original topology, shorter campaign; explicitly NOT formal acceptance.
python3 benches/phase3/run.py --binary /absolute/path/to/rmqtt \
  --artifact-source-revision COMMIT --output /tmp/pilot.json \
  --pilot --cases hot --storage memory --messages 64 --warmup 8 \
  --repetitions 1 --session-expiry-seconds 60 --flow-control ack --timeout 60
python3 benches/phase3/test_diagnostics.py
```

`--pilot` retains128 clients /32 hot subscribers /4096 filters while allowing a
small message count/repetition count. `--smoke` still reduces topology. They are
mutually exclusive; neither is a formal result. Without either switch, the existing
minimum2000 messages and3 repetitions remain. `--broker-write-timeout-ms` defaults
to10000 exactly as the existing comparison profile; any diagnostic override is
recorded in broker argv and must not be mistaken for a runtime optimization.

Every failed or timed-out campaign now saves `campaign_status: failed` and a bounded
`failures` record, then propagates the original exception. External cancellation is
recorded as `cancelled` and re-raised. Failed records never enter `runs` or summary
throughput. Evidence includes case/storage/repetition/flow, executable SHA, broker
argv, stage (setup/warmup/measured/postphase barrier/completed), per-phase expected
and partial issued/ACK/validated-receipt counts, each publisher's latest sequence,
packet ID/state/send timestamp, available RSS samples, errors and stderr. Issued
means an attempted writer write, not server acceptance; only a validated PUBACK
increments ACK. Existing successful timing/flow/count definitions remain unchanged.

A fully received invalid ACK or publication retains actual header/body/reason and
publisher/sequence or subscriber context. Up to16 packet errors retain complete
body hex (receive frame bound8192); exceptions retain up to16 leaf messages of2048
characters. EOF before a complete `receive` result retains the exception and waiting
publisher context; no packet bytes or reason code are invented. Stderr drains
continuously into a64KiB tail with total/truncation counters. Available RSS samples
are kept on failure, including idle/setup if timeout occurs before measurement.
Cleanup errors are secondary when an original failure exists. All budgets remain
finite; graceful cleanup can add up to the separately bounded socket/process waits
after the active-case timeout. Temporary DBs/ports are fresh for each run.

`--profile` is retired (docs/contracts/phase4.md): the worker that wrote the
TSV profile report no longer exists, so the flag is refused before any launch.
The child always receives explicit `RMQTT_PROFILE=0` and no profile-path
variable, so inherited shell flags cannot silently instrument a formal run.
Shutdown closes client sockets, requests SIGINT and waits up to5s, then uses a
bounded hard kill. Stderr is read before removing the temporary directory.

`expected_deliveries` is the entire requested phase plan, not a loss counter.
`expected_receipts_for_issued` and `expected_receipts_for_acked` separately sum
recipient counts for attempted writes and observed valid ACKs. Warmup and measured
phases remain separate. At cancellation these progress totals are concurrent: a
receipt may precede its PUBACK, pending accepted work may still be queued/inflight,
and cleanup deliberately interrupts the workload. Subtracting partial receipts
from any expected value does not prove acknowledged-message loss. No failed-run
throughput or loss percentage is fabricated. Optional transport queue_wait spans
cover mailbox-capacity wait plus queue residence and are not actor CPU time.

## Phase4 connection scale

```sh
python3 benches/phase3/connections.py --binary /absolute/path/to/rmqtt \
  --output /tmp/conn10k.json --connections 10000 --rate 100 --keepalive 60 --hold 120
# CI smoke: 500 unpaced clients, 2s keepalive, 3s hold.
python3 benches/phase3/connections.py --binary target/debug/rmqtt --output /tmp/smoke.json --smoke
```

Each client opens its own MQTT5 connection (Clean Start, or Session Expiry with
`--session-expiry`), waits for a success CONNACK, then sends PINGREQ every half
keepalive until the hold ends and requires each PINGRESP within one keepalive.
`--rate 0` connects without pacing, bounded by `--concurrency` outstanding
handshakes. Any refused or timed-out CONNECT, PING timeout or early EOF fails the
run; failures are counted by kind and no rate or latency is reported for a
failed phase. `rss_bytes_per_connection` is (held RSS - idle RSS) / connections
from `ps` samples, not an allocator peak.

The script raises its own descriptor soft limit; the broker inherits it. One
loopback source address supplies about 16k ephemeral ports, so 100k clients
need `--source-addresses 7` or more (Linux routes all of 127.0.0.0/8; macOS
needs `ifconfig lo0 alias` per address and a raised `kern.maxfilesperproc`).
Cross-host campaigns use emqtt-bench from a separate load generator instead.

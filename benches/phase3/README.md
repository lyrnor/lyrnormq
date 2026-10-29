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
  --session-expiry-seconds 60 --flow-control delivery --outbound-capacity 256
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
accompanying report. Do not infer the artifact source revision from the driver.

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
maxfive repetitions, payload at most4096 bytes, default180s per case. Reader frames
are capped8192 bytes. TaskGroup cancels siblings on failure. Socket cleanup and
broker termination are bounded. Timed-out/cancelled ps subprocesses are killed and reaped. Smoke switches to four subscribers and at most
four publishers/four filters each and is explicitly labeled.

# rmqtt-loadgen

Rust/Tokio MQTT3.1.1/5 paired publish/subscribe generator. A pair is one publisher
and one subscriber on an exact topic;2500pairs means5000TCP connections. This tool
reuses the project's packet codec but has its own client state machine. Independent
raw-wire peer tests validate the client without using that codec as their oracle.

## Build and run

```sh
cargo build --release -p rmqtt-loadgen
# Fixed aggregate intended rate:5000 messages/s,5000connections,128bytes total.
target/release/rmqtt-loadgen --addr 172.30.139.129:1883 --protocol 5 --qos 0 \
  --pairs 2500 --payload-bytes 128 --rate 5000 --inflight 1 \
  --warmup-ms 2000 --duration-ms 10000 --max-messages 100000 \
  --output q0.json --raw q0.samples.jsonl
# Run QoS1 and2 separately with different output paths.
# Bounded closed-loop pressure:next credit waits for BOTH delivery and final source ACK.
target/release/rmqtt-loadgen --addr 127.0.0.1:1883 --protocol 3.1.1 --qos 2 \
  --pairs 16 --inflight 8 --rate 0 --payload-bytes 128 \
  --warmup-ms 1000 --duration-ms 5000 --output closed-q2.json
# Local Linux amd64 musl cross-build; never compile on the target server.
cargo zigbuild --release --locked --offline --target x86_64-unknown-linux-musl -p rmqtt-loadgen
```

`--help` lists all options;all times in CLI are milliseconds. Defaults:2500pairs,
QoS0,MQTT5,128bytes,closedloop,inflight1,warmup10s,offering60s,drain10s,5million
measured records,64KiB complete-frame bound. Config rejects rate*duration beyond
sample budget. Existing report/raw paths are NEVER replaced;choose a new path per
run. `--raw` is optional;all latency samples are retained in memory regardless.

For cluster load, use `--addrs 172.16.240.180:1883,172.16.240.180:1884,172.16.240.180:1885`
in place of `--addr`. Up to 32 distinct numeric endpoints are accepted. Publisher
pair `i` connects to endpoint `i % N`, subscriber to `(i + 1) % N`; with two or more
endpoints every pair traverses nodes. Use pairs divisible by N for exactly equal
publisher and subscriber counts per endpoint. Report `config.addresses`,
`endpoint_mapping` and `configured_endpoint_connections` make that assignment
explicit; configured connection counts are not observed successful connections.
An empty API `addresses` list preserves legacy `address` behavior. `--addr` and
`--addrs` cannot combine. All pairs still share one monotonic clock and setup barrier.
`--setup-concurrency N` bounds active pair setup tasks (default 32, range 1..1024).
Use a smaller value, such as 1 or 3, to avoid overwhelming bounded broker admission
or durable-operation windows during setup. It changes setup pressure only, does not
retry rejected connections and does not reduce the measured publish load. The
configured bound is retained in the report; all setup still finishes before warmup.

Only one generator process is supported per run:the publisher and subscriber use
the same monotonic epoch. KeepAlive0,clean sessions,TCP_NODELAY,no aliases,auth,TLS,
WebSocket,reconnect/retransmit,fanout/shared subscription or connection-only mode.
A successful server QoS downgrade is rejected rather than silently changing the test.
Source ACKs with nonzero MQTT5 reasons fail rather than count as successes.
Client IDs use23 ASCII alphanumeric bytes. Single source/target address tuple still
has TCP source-port limits;do not infer120000clients can fit one address pair.

## Metrics with explicit meanings

The48-byte header is INSIDE the configured payload (minimum48 bytes):magic,run ID,
pair ID,measurement flag,sequence,scheduled offset and send-start offset;remaining
bytes are deterministic padding. Exact topic,run,pair,sequence,timestamps,padding
and payload size are validated. No wall-clock timestamp,no remote clock correction.

|Field|Meaning|
|---|---|
|scheduled|Absolute deadlines in the intended window;closed loop equals attempts|
|attempted|Measured PUBLISH operation begun,including later failed writes|
|write_completed|Whole encoded frame accepted by client write_all;not wire/peer acceptance|
|puback_received|Positive source QoS1 PUBACK for its outstanding identifier|
|pubrec_received|Positive source QoS2 intermediate PUBREC,never final completion|
|pubcomp_received|Positive source QoS2 final PUBCOMP|
|delivered_unique|Validated unique subscriber application delivery;QoS2 atPUBREL|
|duplicates|Repeated subscriber delivery observations,excluded from unique TPS|
|completed|Full write AND unique delivery AND final source ACK (Q0 needs no ACK)|
|not_sent / not_delivered / not_source_confirmed|Explicit unfinished work,never hidden as0ms|

Report has TWO throughput views:
- `offering_window_tps`:events timestamped in[start,end),divided by offering duration.
- `measured_cohort_tps_through_drain`:all measured-cohort observations including late
  events,divided by actual finish-start. Cleanup DISCONNECT time is excluded.
`after_offering_window_events` exposes late deliveries/ACKs separately. Never add
publisher ingress and subscriber egress to claim doubled broker throughput.

Every latency is an exact integer-nanosecond difference;ns is the unit,not a claim
of1ns physical accuracy. `latencies` reports count,min,mean,p50,p95,p99,max:
- `schedule_to_send`:client scheduling/window lag.
- `send_to_write_completed`:encoding,write and client backpressure.
- `send_to_delivery`:publisher operation start -> subscriber application event.
- `schedule_to_delivery`:intended deadline -> delivery,including client lag.
- `send_to_source_completed`:PUBACK(Q1)/PUBCOMP(Q2);null forQ0.
- `send_to_full_completion`:all completion conditions satisfied.

Percentiles sort ALL retained observations and use rank ceil(q*N);no histogram
interpolation,no averaging of per-client percentiles,no silent subsampling. Missing
samples/counter mismatches/impossible timestamps fail validation. Incomplete work
is retained with nullable event times;latency distributions are conditional on
observed events. Missing delivery/ACKs mean those percentiles do not establish an
unconditional latency SLO. A duplicate is a warning,not a new delivered message.

## Offered load and failure interpretation

`--rate 0` is closed loop:credits are released only after both end-to-end delivery
and source completion. Its latency is NOT an open-loop/coordinated-omission-corrected
SLO. `--rate N` interleaves absolute deadlines across pairs;late tasks keep their old
deadlines rather than silently moving them to now. If the bounded window is full,
schedule lag grows. At offering end,new sends stop;unsent deadlines are counted.
This includes timer granularity at the final boundary:a400message plan may send399
and explicitly report not_sent1. That is an unsuccessful fixed-rate gate,not a
fabricated400message success. Inspect actual counts/rates and scheduling lag.

Measured samples have deterministic per-pair quotas summing to max_messages;warmup
uses the same per-pair cap separately. Quota exhaustion is an explicit failed run,
not silent truncation. All frames,windows,deadlines and sample counts are bounded;
large configured sample budgets still consume substantial RAM. Parsing/sorting/
JSONL writing happens on the synchronous main thread after stopping the async load
runtime,so report IO does not pollute measured latency. A crash/kill before report
writing is not a durable streaming capture and can lose in-memory observations.

Exit0=report.success true;exit2=valid unsuccessful measurement;exit1=configuration,
setup,instrumentation validation or output failure. Failure JSON is retained when
the requested report path can be created;existing files remain untouched. RawJSONL
keeps incomplete observations for audit. Report metadata records arguments,build
revision/profile,relevant code dirty status,OS/architecture and CPU parallelism.
External resource samples must be attached explicitly;the tool does not invent RSS
or isolate broker latency from network/generator scheduling.

## Independently verify raw p99

```python
import json
r=json.load(open('q0.json'))
s=[json.loads(line) for line in open('q0.samples.jsonl')]
v=sorted(x['delivery_ns']-x['send_started_ns'] for x in s if x['delivery_ns'] is not None)
assert len(v)==r['latencies']['send_to_delivery']['count']
assert v[(99*len(v)+99)//100-1]==r['latencies']['send_to_delivery']['p99_ns']
```

Use nanoseconds/1e6 to display milliseconds. Recompute each series from its own
valid timestamps. Empty distributions have null percentiles,not zero latency.

Frozen semantics: [contract](../../docs/contracts/rust-loadgen.md). Acceptance
includes independent raw peers,QoS2 intermediate/final timing,wrong ACKs,rejections,
corruption,duplicates,drain deadlines,sample limits,multiple pairs/windows,warmup,
CLI failure JSON and independent raw-sample percentile reconstruction. See the
project's verification report for measured real-broker checks;no standalone maximum
TPS or protocol client certification claim follows from these tests.

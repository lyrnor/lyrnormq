# Rust loadgen latency diagnosis — 2026-10-30

Same5000connections,128bytes,MQTT5,QoS0/1/2,same tool binary and broker PID/version,
same8core generator and4core broker. Compare previous closed-loop unlimited-rate
window1 short acceptance with aggregate intended5000msg/s,10s offering,1s warmup,
window1. This intentionally changes offered load/pacing;it is not a broker code
optimization or an unchanged-load before/after speedup.

Independent read-only engine and quality reviews reconfirmed original raw nearest-
rank quantiles and state-machine timing. Removing first/last500ms or late deliveries
did not remove the high tail. Tail samples spanned hundreds/thousands of pairs,
not a few bad connections. Send write p99 was only0.051–0.061ms;that does NOT rule
out waiting in broker/network or in client receive scheduling after the write.

|QoS|Original pressure TPS|Pressure delivery p99ms|Low actual TPS|Low delivery p99ms|Low intended-schedule->delivery p99ms|Not sent|
|---:|---:|---:|---:|---:|---:|---:|
|0|48790.8|270.025|4999.6|0.368|2.140|4|
|1|29162.6|389.771|4999.6|0.454|2.399|4|
|2|18991.2|462.671|4999.0|0.905|2.780|10|

Send->delivery measures operation-start through subscriber application event.
Schedule->delivery additionally includes generator scheduling/window lag. All
percentiles are exact nearest-rank over raw nanosecond observations,not interpolated
buckets. All sent measured messages wrote,uniquely delivered and completed final
protocol confirmation(Q0 noACK,Q1PUBACK,Q2PUBCOMP). Each50000-deadline plan missed
4/4/10sends at cutoff;all original reports remain success:false with explicit
not_sent. These observations do NOT pass a zero-unsent fixed-rate gate and were
not relabeled successful. No missing delivery or unfinished sent transactions.

Average measured-cohort inflight count is the sum of each attempt's
[send_started,completed] overlap with the offering interval divided by duration.
It describes tool completion credits,not an instrumented broker queue depth:
- QoS0:pressure2466.9,low1.24.
- QoS1:pressure2448.2,low1.52.
- QoS2:pressure2445.5,low3.35.

Pressure kept almost all2500publisher credits occupied;low rate kept only~1–4
messages in flight on average despite the same5000TCP connections. Observed tail
latency drops by orders of magnitude when offered load is reduced. This supports
pressure/queueing as the main investigation direction,and excludes a fixed
hundreds-of-ms floor in this setup. It does NOT identify whether the saturated
queue resides in broker scheduling,outbound IO,network or client receive tasks.
Changing to paced load also changes scheduling/traffic burst shape;these cannot be
causally separated by this contrast alone. No production broker changes made.

Low-load broker CPU/RSS samples are retained. Onlytwo5-second-period samples per
10s offering window,one full CPU interval each;do not treat them as comprehensive
peak/mean measurements. Window alignment uses generator Unix start plus monotonic
offsets and approximates runtime-init/cross-host wall offset. TPS/latency use only
one process's monotonic clock. No client CPU instrumentation was added in this
contrast. Resource sampling overhead is included only for this diagnostic.

The original Q2 per-message delivery-minus-sourcePUBCOMP gap is also retained,
computed per record before quantiles (not by subtracting unrelated p99s). It
suggests extra waiting between source confirmation and subscriber observation,
but this includes several components. Differing slow modes acrossQoS cannot by
themselves prove a common TCP delayed-ACK/Nagle timer. Current repository sets
TCP_NODELAY;no runtime wire trace or per-stage broker queue timings were collected.

Next causal experiment,if preserving original high TPS and reducing p99 is the
goal:at matched offered rate sample client wire send/receive timestamps against
application observations,plus broker enqueue/dequeue/output stage durations.
Then change only the confirmed bottleneck,with unchanged-load A/B and protocol
regressions. Lowering load in this diagnostic is not claimed as a performance fix.

Broker PID2100933 unchanged,no restarts,all test connections cleared. No binary,
configuration or kernel changes. [Structured comparisons](results.json),
[lowrate driver](lowrate-driver.py),[client raw](rmqtt-lowrate-client.tar.gz),
[broker resources](rmqtt-lowrate-broker.tar.gz). SHA256 matches fetched artifacts.
Original pressure data are in ../../verification/rust-loadgen-20261030/.

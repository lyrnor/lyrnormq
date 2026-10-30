# Rust load generator v1 — frozen interface and metric contract

Standalone bin/rmqtt-loadgen; no broker/protocol API or persistent schema changes.
lib.rs docs/modules/reexports only. Stable2024,Tokio,bounded frames/windows/samples,
no unsafe/production unwrap/expect/blocking I/O in async/locks across awaits.
Coordinator owns model/config/payload/error/main/report/workspace/docs;engine agent
owns src/engine.rs+src/engine/**;metrics agent src/metrics.rs+src/metrics/**;
quality agent bin/rmqtt-loadgen/tests/** only. Each writer independent worktree and
codex branch;commits tested changes. Dependencies root model frozen below.

MQTT3.1.1 and5 via existing mqtt-protocol codec,2500 paired exact topics=5000 TCP
clients by default. Subscriber connects/subscribes before source;all pairs setup
barrier before global warmup/measurement. TCP_NODELAY,clean sessions,KeepAlive0,
no authentication/TLS/shared/retained/aliases/reconnect/retransmit initially;unexpected
or rejected frames fail explicitly. QoS matches on both ends. Positive PUBACK=Q1
source completion;PUBREC is Q2 intermediate only,PUBREL->PUBCOMP completes source.
SubscriberQ2 delivery recorded only on PUBREL;duplicate subscriber deliveries and
current QoS2 retransmission phases are idempotent. Unexpected/stale final ACKs
without an outstanding transaction fail closed;this is not a reconnecting client.
Never reuse outstanding packet IDs. Per-pair bounded inflight and I/O/deadlines.

Payload exactly configured bytes,minimum48. Magic8,run_id u64,pair u32,flags u32,
sequence u64,scheduled_ns u64,send_started_ns u64,remaining deterministic padding.
flags bit0=measured. Offsets relative to ONE process std::time::Instant epoch.
Receiver validates run/pair/topic/sequence/timestamps/padding. Unique delivery once;
duplicates tracked,never counted as new throughput. No wall-clock/remote timestamps.
Send start is before payload/encode/write;write complete is AFTER full write_all.
Acknowledgements must correspond to current state and IDs;failure reasons not success.

engine::run(config:RunConfig)->Result<RunData,LoadError>. Root model.rs is definitive.
A pair task owns both sockets and its state;avoid globally contended per-message
aggregation. PairData samples include incomplete records at drain timeout. Counters
are exclusive meanings,not sums of protocol packets. Metrics::summarize(&RunData)
returns Report. Exact nearest-rank ceil(q*N),u64 nanosecond observations,no coarse
bucket interpolation or silent sample dropping. Series: schedule->send(client lag),
send->write completion,send->delivery,scheduled->delivery,send->source completion,
send->full completion. Source completion not applicable Q0,represented null.
Missing latency observations visible via attempts/uniques/completions and failures.

--rate0 = closed loop(window credits released after BOTH source completion and
unique delivery). No coordinated-omission correction claim. --rate>0 open-loop
absolute schedule interleaved across pairs;do not reset overdue deadlines to now or
silently skip. Window saturation waits and shows schedule lag. Scheduled messages
not emitted before end reported as not_sent;rate pressure failure not passed capacity.
End offering after duration;drain bounded outstanding protocols/deliveries. Report
offering window TPS separately from elapsed-to-drain completed throughput;late
completions tracked via timestamps,not hidden or folded into shorter denominator.

max_messages globally bounds measured samples,distributed deterministically by pair
(config.quota(pair)). Warmup bounded by same per-pair cap. Hitting quota is explicit
resource failure/truncation,never successful silent sampling. Frames capped config.
JSON includes config,mode,units,count denominators,raw counts,phase ns,failure status,
p50/p95/p99/max/count for exact sample arrays. Optional raw JSONL after load. Sort/
serialization/output outside measurement on a synchronous main thread after
stopping the Tokio runtime (or via spawn_blocking for async callers). No background resource
sampling implicit;external process RSS/CPU can be joined explicitly by run timestamps.

Acceptance: deterministic metrics oracle/tails/incomplete/late/dup/zero samples,
payload corruption checks;independent wire peer tests QoS0/1/2,PUBREC vsPUBCOMP,
rejection/disconnect/delayed delivery/timeouts,rate lag and finite limits. Real broker
smoke both versions allQoS. Fmt,strict all-target clippy,workspace tests. Deliver real
release binary and documentation,not just architecture. No saturation claim without
measured actual scheduled/sent/delivered rate and client resource evidence.

Coordinator clarification: RunData.finished_ns is the last pair drive observation
end BEFORE best-effort DISCONNECT cleanup. Cleanup never dilutes drain TPS.

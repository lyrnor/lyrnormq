# Phase7r — measured Raft processing chain

Diagnostic-only user scope: measure current slow connection control chain. Do not
change consensus, batching limits, queue capacities, admission, deadlines, ACK or
durable schemas. No OpenRaft fork. No server compilation. Keep thin lib.rs.

Coordinator owns docs/harness plus new mqtt-cluster/src/profile.rs and its lib.rs
module declaration. Control agent owns controller.rs/controller/** and rpc.rs.
Storage agent owns storage.rs/storage/**. Transport agent owns admission.rs and
minimal durable.rs timer hook only. All agents use separate worktrees/branches.

Shared internal API frozen: crate::profile::{Metric,Timer,observe,enabled,stamp_us,
Reporter}. Timer::start(Metric) records elapsed microseconds on Drop (including
failed/cancelled work, not a success-only histogram); observe(Metric,u64) records
counts/bytes or explicit elapsed times. stamp_us is monotonic process-relative,
NOT comparable between nodes. Reporter::start(node) returns a lifetime guard;
one per Controller logs cumulative snapshots every second and at Drop. Enable
only with RMQTT_RAFT_PROFILE=1. Metrics use fixed atomic count/sum/max/log2 buckets,
no per-client labels or bodies. Concurrent snapshots are approximate; prefer
quiescent sample differences. Log histogram percentile UPPER BOUNDS, not exact p99.
Fixed names are defined in profile.rs; ask coordinator before adding a metric.

Control: split validate/encode, mailbox-capacity wait, enqueue-to-dequeue, assembling,
Raft quorum+apply result and reply wake; record batch size/bytes. One trace per batch,
first4096 maximum per process, gives numeric batch ID, process-relative submit/end,
returned log index, command count and queue wait summary. Do not imply batch RTT is
network-only. AppendEntries RPC encoding/roundtrip/decoding should be separated;
first4096 traces include target, log range and monotonic endpoints. ReadIndex separate.

Storage: split semaphore/worker scheduling/mutex wait from WAL work; separate entry
encoding, raft-log append/mutation, flush submission and durable callback wait;
separate apply serialization, committed-input validation, CPU worker scheduling,
state-lock wait, state mutation and history update. Checkpoints separate. Existing
log-index contexts must remain useful for matching append/callback/apply to returned
batch log indexes. Limited numeric traces are opt-in; no unbounded buffers.

Transport: count handshake acquisitions/rejections and sampled peak in-flight,
report exact cumulative counters periodically and after shutdown; avoid per-client
logs, no permit lifetime or rejection behavior changes.

Validate accounting boundaries, errors/cancellation and existing batch/recovery
semantics. Tests/fmt/strictClippy for touched packages plus relevant real-process
recovery regression. Native tests first, local locked Zig musl release second.

Server: fresh directories on47.84.224.186, three cohosted dual-role processes,
unchanged limits. Four probes on47.84.241.200, same500/s2500each,2workers,shared
start/PING. One bounded2000/s10k diagnostic reproduction; fail stops higher/message
loads. CPU perf sampling concurrently if supported (record permission failures;
never fake stacks); finite capture<=60s, bounded frequency/data, stop all processes.
No automatic limit changes. Preserve binary hashes/raw metrics/commands, correlated
stage analysis, reproducible environment and unresolved gaps. Diagnostics may add
cost; compare to previous baseline with this caveat, not an optimization claim.

Boundary clarification: EnqueueWait measures reserve().await before mailbox ownership;
QueueResidence begins at successful permit.send and ends when the worker takes it.
ApplySlotWait is the bounded blocking-worker semaphore wait; ApplySpawnWait begins
after that permit is acquired. AppendCallback traces identify actual callback timing.

Histogram bucket0 covers0..1; bucketN covers values through2^N, except final
bucket63 is an overflow bucket throughu64::MAX. Cumulative snapshots are not
atomic across fields; fixed diagnostic runs stay far below u64 sum/count wrap.

Review clarification: ApplyMutexWait must cover prechecks and clone lock acquisition
as well as normal apply locks. CheckpointClone times whole-State cloning separately;
CheckpointWait measures run_checkpoint slot+dispatch+locks before CheckpointPersist.
CheckpointTotal is persist_checkpoint only; whole snapshot cost includes prior clone.
WAL/apply timers are nested wall times, not additive CPU. Callback stamp is observed
after notification, so concurrent Core wake can precede it by a small interval.

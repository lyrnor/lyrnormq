# Phase3 — reproducible local performance and reliability acceptance

Frozen before parallel implementation. Prior protocol/session/outbox contracts and
schema7/container1 remain. No runtime API changes are initially authorized. Confirmed
bugs require a failing regression and coordinator-approved contract addendum before
the owning agent fixes production code. lib.rs remains docs/modules/reexports only.

## P1 Workloads and correctness

A standalone bounded loopback load driver launches an explicit release executable.
Cover (a) at least128 simultaneously connected clients, (b) at least32 subscribers
on one hot topic, (c) at least4096 installed subscriptions. Use MQTT5 with QoS1 ingress
and QoS0 subscriber delivery for the comparable routing baselines; explicitly record
any additional protocol/QoS workload separately. At least2000 measured publications
per case and three repetitions per storage mode (memory, fresh SQLite) are required.
No client library install is needed for the raw-wire harness. Workloads must verify
exact payload sequence/identity/counts and expected ACKs, not just successful writes.
Every task, socket, queue and campaign has finite limits/timeouts; propagate failures.
Use setup barriers, a separate bounded warmup, and a measured interval ending only
when all expected acknowledgements AND deliveries complete. Report ingress messages/s
and egress deliveries/s separately. Never count fanout copies as ingress throughput.

## P2 Latency, memory and reproducibility

Record payload bytes, concurrency, topic/filter distributions, packet limits, QoS,
storage durability settings, build profile, executable SHA256/source revision,
OS/architecture/CPU/RAM (or unavailable with reason), Rust/Python versions, FD limit,
exact command, all broker arguments, clock/percentile definitions and repetitions.
Latency uses one host's monotonic clock: publish-send to matching subscriber receipt;
ACK latency is a separate metric. Record p50/p95/p99/max and sample counts, plus wall
measurement duration. Bounded outstanding samples must not silently drop latencies.
Record broker-only RSS at idle/setup/periodic measured samples/end; distinguish
sampled peak from true allocator/kernel peak. Report CPU time where obtainable and
sampling interval/method. Preserve each raw run and summarize median/range; no
unfounded target or before/after improvement claim. Loopback Python clients share
the host and may limit throughput; these are local baselines, not saturation or WAN
capacity. Run performance campaigns serially after competing builds/tests finish.

## P3 Backpressure and slow clients

Add finite reproducible tests with small configured limits. Verify a nonreading peer
and a ReceiveMaximum-limited QoS peer cannot grow application queues indefinitely;
healthy independent clients retain bounded liveness. Verify overload refusal/closure,
reconnect and release of connection capacity without corrupting durable work. Assert
protocol-observable outcomes and exact bounded state invariants, rather than relying
on a machine-specific socket-buffer threshold or timing-only RSS plateau. Record
actual overload policy (including global rejection if implemented), without inventing
per-subscriber fairness guarantees. Exercise memory/SQLite where applicable. Paused
Tokio time or deterministic internal barriers may supplement actual TCP tests.

## P4 Reuse, integration and scope

Reuse existing SQL commit/error/crash suite, corpus/oracles and fuzz targets. Run
actual post-integration fault tests, seeded mutation and bounded guided campaigns
when existing isolated nightly tooling is available. Run workspace fmt, strict
all-target Clippy, tests, inventory guards and thin-lib audit. Independent review
checks drivers' accounting, timing, socket setup and hidden dropped samples, as well
as transport resource boundaries. CI uses small reliability smoke cases, never
machine-specific throughput assertions. Commit reproducible scripts/results/docs.

Dependencies: contract -> three isolated writers (benches/phase3 load driver;
transport deterministic tests/resource audit; tests/phase3_reliability.py and CI).
Coordinator builds immutable artifact -> all correctness runs/review -> serial
performance campaign -> results/integration/docs. Writers do not edit shared Cargo
or root docs. Any conflict/defect is coordinated before cross-module changes.

Acceptance is a bounded Phase3 baseline, not long-duration soak, official protocol
certification, hardware power-loss safety, production SLA, TLS or cluster readiness.

## Frozen local measurement profile (before formal campaigns)

Formal baseline commands set session-expiry-seconds=60 for all clients in both
storage modes. This makes SQLite retain actual sessions/subscriptions and queued
work, rather than describing a clean-session-only routing test as durable throughput.
Ingress remains QoS1, subscriber grant QoS0: do not claim outbound QoS1/2 throughput.
Use4096 measured publications per case, three repetitions,256-byte application
payloads; preserve per-case broker arguments and exact distribution. Driver default
expiry0 may remain for explicit clean-routing comparisons; label it separately.

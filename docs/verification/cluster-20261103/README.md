# Phase7a embedded cluster verification — 2026-11-03

Contract: [phase7a](../../contracts/phase7a-cluster.md).
Operation/configuration: [running](../../cluster/running.md).

Environment: Darwin arm64, Rust/Cargo 1.98.1, protoc 33.0. Native development
binary; three independent Controller processes and two Broker processes on
allocated loopback ports, with isolated temporary files. No existing MQTT service
or external coordination system is used. No Linux, physical power-loss, network
throughput, latency SLO or production capacity claim is made.

Final executable SHA256:
`45b8db928c3405f1894c50cb83c1da3b0ae65e64721713fc6312e8c65e471a1e`.
Built from code through `1bef408` plus the CLI integration in `dabd1bc`; subsequent
changes are verification/progress documentation only. The local immutable test
copy is `/private/tmp/rmqtt-cluster-accepted`; build output is
`target/cluster/debug/rmqtt`.

## Reproduction

```sh
CARGO_TARGET_DIR=target/cluster cargo build -p rmqtt --locked --offline
CARGO_TARGET_DIR=target/cluster cargo test --workspace --locked --offline
CARGO_TARGET_DIR=target/cluster cargo clippy --workspace --all-targets --locked --offline -- -D warnings
cargo fmt --all --check
RMQTT_BIN="$PWD/target/cluster/debug/rmqtt" python3 tests/cluster_blackbox.py -v
```

Offline requires the locked dependencies already fetched. Tests that bind local
ports require a runtime environment permitting loopback listeners.

## Actual final results

| Check | Result | Evidence |
| --- | --- | --- |
| Native build | Passed | build.log |
| Full Rust workspace | 852 passed, 1 explicitly ignored live-PostgreSQL test | rust-tests.log |
| Strict all-target Clippy | Passed | clippy.log |
| Fmt / diff whitespace | Passed | commands above |
| Real three-Controller/two-Broker suite | 20 passed, 86.284 s | cluster-wire.log |
| Standalone HTTP publishing / lifecycle / HTTP access | 77 passed, 46.956 s | standalone-wire.log |

The wire-suite durations are test runtime, not throughput or latency measurements.
Both final wire runs used the same executable hash above. Before-fix fault logs
are preserved as broker-rejoin-before-fix.log and watch-before-fix.log.

## Review and defect history

Three specialists used isolated worktrees: cluster control, transport and independent
quality. The coordinator froze core APIs, integrated commits, wired startup and ran
the combined checks. No parallel writes were used as a substitute for isolation.

Actual failures preceded fixes:

- An unresponsive old leader kept a TCP Watch open: Controller progress frames,
  Broker idle/revision detection and preference for a confirmed Controller restore
  convergence without waiting for that process to resume.
- A Broker removed after its lease expired could never rejoin the same process:
  quorum-proven absence now triggers local fencing, re-registration, a fresh watch
  and confirmed grant. Another incarnation cannot be overwritten by this path.
- A shorter lease from a replacement Controller could invalidate an outstanding
  longer grant: each incarnation now persists the maximum granted duration.
- Old-generation subscription updates, canceled receipt entries and cumulative
  snapshot allocation were reviewed and fenced/bounded before final acceptance.

The first fault tests incorrectly equated Raft election with immediate Broker
readiness. They now wait at most 30 seconds for a real MQTT CONNECT+PING on each
Broker. This does not weaken the routing/SUBACK assertions or promise an unmeasured
failover SLO. Lease withdrawal may close an old connection with 0x88 or 0x8B;
the test still requires that it closes and that fresh cross-node traffic succeeds.

## Scope and remaining work

Delivered: embedded durable Raft metadata, fixed three-member Controller quorum,
Broker ownership/leases, local snapshot/delta replicas, ordinary direct forwarding,
cross-node live MQTT 3.1.1/5 QoS0/1/2, generation fencing, HTTP downlink and source-only
rules observation. Official OpenRaft storage conformance runs against the real
SQLite WAL/FULL store, not a memory test substitute.

Cluster retained/Will/shared subscriptions and persistent-session relocation remain
explicitly refused. Cross-node forwarding receipts are volatile and bounded, with
no durable exactly-once or replicated forwarding-responsibility claim. Non-loopback
links require configured mutual TLS, but the process suite uses loopback plaintext;
no new cluster TLS interoperability result is claimed. Shared credentials trust
members rather than binding every RPC to a per-node certificate identity.

Next gates: durable forwarding and session failover; retained/Will/shared cluster
semantics; real Raft compaction/snapshot-transfer fault tests; cluster mutual-TLS
wire acceptance; configuration/auth policy distribution; large-topology profiling,
indexed change history/routing and performance acceptance. Fixed byte/count limits
are documented in running.md; no million-connection scalability claim follows from
these small-topology correctness tests.

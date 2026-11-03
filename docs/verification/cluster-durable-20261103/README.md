# Phase7b durable cluster verification — 2026-11-03

[Contract](../../contracts/phase7b-durable-cluster.md) ·
[Configuration and limits](../../cluster/running.md#durable-forwarding-and-session-failover).

Environment: Darwin arm64, Rust/Cargo 1.98.1, protoc 33.0. Native development binary,
three independent Controller processes plus three Broker processes with separate
data directories. Failed owners are permanently killed in migration cases; the
replacement never mounts or copies the failed owner's files. Process SIGKILL
tests are not physical power-loss certification or cross-machine benchmarking.

Final code through `7fa2784`; executable SHA256
`5057c0c976b5c14ecbc2177bd1c48e8c17efc5344e38299cd05ae1eaa79f6d60`.
The immutable local test executable is `/private/tmp/rmqtt-durable-final`.
Subsequent changes are documentation/evidence only. Test sources include the
quality worktree's `4501db5` (integrated as `85862fd`).

## Reproduce

```sh
CARGO_TARGET_DIR=target/cluster cargo build -p rmqtt --locked --offline
CARGO_TARGET_DIR=target/cluster cargo test --workspace --locked --offline
CARGO_TARGET_DIR=target/cluster cargo clippy --workspace --all-targets --locked --offline -- -D warnings
cargo fmt --all --check
RMQTT_BIN="$PWD/target/cluster/debug/rmqtt" python3 tests/cluster_durable_blackbox.py -v
RMQTT_BIN="$PWD/target/cluster/debug/rmqtt" python3 tests/cluster_blackbox.py -v
PYTHONPATH=tests RMQTT_BIN="$PWD/target/cluster/debug/rmqtt" python3 -m unittest http_publish_blackbox system_client_events_blackbox http_access_blackbox -v
```

Offline builds require cached locked dependencies. Runtime must permit loopback
listeners. CI now installs protoc and includes both real cluster suites and the
existing HTTP/system-event suites; a remote GitHub Actions run is not claimed.

## Actual final results

| Check | Result | Evidence |
| --- | --- | --- |
| Native build | Passed | build.log |
| Rust workspace | 872 passed, 1 explicitly ignored live-PostgreSQL test | rust-tests.log |
| Strict all-target Clippy / fmt / diff | Passed | clippy.log and commands above |
| New durable cluster wire suite | 27 passed, 188.931s | durable-cluster-wire.log |
| Existing phase7a cluster wire suite | 20 passed, 87.037s | legacy-cluster-wire.log |
| Standalone publishing/lifecycle/access suites | 77 passed, 52.021s | standalone-wire.log |

All three final wire runs used the identical executable hash above. Test runtimes
are not throughput/latency measurements. Before-fix evidence includes
host-init-before-fix.log, empty-disk-before-fix.log, full-queue-receipt-before-fix.log,
filter-cache-before-fix.log and mirror-before-fix.log. Original helper failures and
the subsequent host-race failure are preserved separately; the old target-reset
wire PASS is explicitly recorded as coverage rather than a reproduced failure.

## Implemented transaction and recovery boundaries

- Source session proposal plus stable forwarding intents commit to two data
  journals before ACK/local or remote fanout. Clean Start/deletion does not erase
  accepted source obligations. Host/HTTP/system publishers use the same journal
  in a separate namespace and serialize initial ownership with append.
- Destination session snapshot and per-source receipt watermark commit together.
  Retry checks receipts before attempting a possibly full queue. Strict source
  FIFO, frozen recipients and retained intent IDs make watermarks safe across
  ownership migration. The source-stream bound does not count each message.
- Epoch-CAS ownership acquisition collects data from the same two atomic Fence
  replies, writes back the recovered record to a quorum, then activates existing
  SessionEngine state. Old local rows cannot replace replicated authority.
  Unknown outcomes invalidate actors and record writers, including background
  completion, until a higher-epoch recovery.
- Controller-anchored disk identities prevent empty replacement voters. Node
  expiry retains identity anchors and persistent subscription interest. Automatic
  takeover cannot steal another currently live owner. Source recovery/draining
  does not require the source MQTT client to reconnect.
- A separate logical-session generation survives ordinary resume but changes on
  Session Present=false. Both recipient preflight and atomic commit reject an old
  generation, including a reset between preflight and commit. Old pending messages
  cannot enter a new session that reuses the same Client ID and subscription.

SQLite WAL/FULL was reused for the replicated register because an opaque session,
outbox and receipt must change atomically with epoch/version checks and random
quorum reads. It reuses the existing dependency and blocking-IO discipline; it is
not the generic connector segmented log or Controller message storage. Whole-record
serialization, directory scans and aggregate quota scans are baseline costs, not
a demonstrated throughput optimization.

## Review evidence and corrections

Three real specialist agents used independent worktrees for runtime, transport
and independent quality. Coordinator froze interface amendments before dependent
implementation, reviewed and cherry-picked owned commits, then compiled/tested
the integrated executable. Public MQTT snapshot format and thin lib.rs structure
were retained.

Confirmed failures preceded fixes: concurrent first host publication returned503;
an empty replacement disk could register as a voter; a duplicate delivery into an
already full queue failed before receipt lookup; background acquisition could
claim an owner that had become live again; local mirror tombstones and recovered
subscription caches were not reconciled. Tests also cover releasing disowned
payload caches without replacing same-key serialization actors.

The initial MQTT5 wire helper incorrectly required PUBCOMP0 for a repeated,
completed PUBREL. Only those three completed-retry call sites now permit0x92;
initial successful PUBCOMP still requires0. This follows the official
[OASIS MQTT5 PUBCOMP reason-code table](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html#_Toc3901142).
Packet Identifier, DUP and no-second-delivery assertions were retained. This was
a test correction, not a reason to weaken protocol behavior.

The added target-reset wire case happened to pass on the old binary because its
scheduling window did not expose the flaw. It is recorded as coverage, not a
deterministic before-fix failure. Deterministic unit tests exercise the reset flag
and preflight/reset/commit race, including a destructive stale proposal that must
leave the new snapshot unchanged.

## Limits and remaining gates

This is explicit durable mode on one fixed three-Broker data group with W2. It
does not supply arbitrary replica-group reconfiguration, in-place legacy migration,
rolling-version upgrade or empty-disk repair. Preserve original data; use a new
cluster for this mode rather than deleting journals to bypass identity checks.

Retained, Will and shared subscriptions remain refused in cluster mode. QoS0
subscriber socket delivery is still best effort. HTTP caller retries after an
unknown response are separate requests and may duplicate; internal intent retries
use stable identities. Rule admission remains bounded best effort rather than an
MQTT-ACK/connector transaction.

Data journal logical bytes are capped at256MiB, with SQLite/WAL overhead additional;
record8MiB, opaque snapshot2MiB, pending intents256 per source, receipt source streams
8192 per recipient and finite record/directory counts. Byte limits may bind first.
Tombstone/abandoned-host reclamation, replica repair and membership changes remain
future work. Full capacity fails admission rather than silently dropping durable
responsibility or falling back to RAM.
Invalidated transport actors can retain bounded local session slots until
reactivation; more aggressive retirement/reclamation is a subsequent resource
efficiency task, not a reason to reuse their non-authoritative snapshots.

No Linux execution, new cluster mutual-TLS wire interoperability, network-partition
latency SLO, large-topology capacity, TPS/P99 or physical power-loss result is
claimed. Next performance gate should profile pending/recovery scheduling, indexed
recipient matching, quota accounting and write serialization using identical loads.

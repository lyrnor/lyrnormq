# Progress

## 2026-10-28 / Phase 0
- Empty directory inspected; Rust/Cargo 1.98.1 available.
- Git initialized; baseline architecture commit 11131d9.
- Workspace and version 1 module contracts frozen.
- Six owned modules assigned isolated Git branches/worktrees, scheduled in batches
  under the environment's three-child-agent concurrency limit.
- Official OASIS 3.1.1 and 5.0 specifications consulted.

## Phase 1 accepted — 2026-10-28
Delivered executable `rmqtt` plus protocol/router/transport crates and CI. Supports
clean-session MQTT 3.1.1 QoS0 TCP messaging, wildcard routing, unsubscribe, ping,
keepalive, connection takeover, bounded resources and graceful shutdown.

Actual environment: Darwin 27.0.0 arm64; rustc 1.98.1; cargo 1.98.1; Tokio 1.54.1;
Paho MQTT 2.1.0 in an isolated temporary Python virtual environment. Cargo.lock is
committed. CPU/memory inventory was unavailable under the sandbox. This is
functional acceptance, not a performance benchmark.

| Check | Actual result |
| --- | --- |
| cargo fmt --all -- --check | passed |
| cargo clippy --workspace --all-targets --offline -- -D warnings | passed |
| cargo test --workspace --offline | 30 tests passed (protocol9/router5/session3/storage5/transport8) |
| cargo build --workspace --offline | passed |
| python3 tests/blackbox.py | 14 passed, 3.458 seconds |
| Paho tests/paho_interop.py | CONNECT/SUBSCRIBE/QoS0 publish/receive/disconnect passed |

The initial sandbox run failed seven TCP tests because binding loopback sockets
was forbidden (OS PermissionDenied). The complete suite was then rerun with local
network permission and all tests passed. Black-box and Paho tests also ran with
local network permission. No CI-host execution is claimed.

| Specialist | Original isolated branch commit | Integrated commit |
| --- | --- | --- |
| Protocol | 94190e6 | bf5824a |
| Router | 617ce46 | f34309f |
| Storage | 0309a30 | 3526f1f |
| Session/QoS | 81a4a90 | 62c5a73 |
| Quality | 53fa65a + 78b1272 | ed3e131 + 3656316 |
| Network | a5116f4 | fea9586 |

## Phase 1 limitations (historical; superseded below)
- Runtime does not support MQTT5, QoS1/2, retained messages, Will, credentials,
  persistent sessions or offline queues. Unsupported input is rejected; this is
  not a complete MQTT conformance claim. Empty client identifiers are rejected.
- Session/QoS2 and memory storage are tested primitives, not integrated runtime
  persistence. Durable transaction/outbox APIs must be frozen before Phase2.
- Outbound queues are bounded and saturated clients disconnected. Slow-consumer
  stress, sustained load, fuzz campaigns, crash recovery and client matrices are
  not yet validated. No throughput/latency/memory baseline measured.
- Router uses a short synchronous global lock. No lock spans await; contention
  still needs measurements. Codec encode currently validates through decode,
  adding payload copying. Keep these as explicit performance follow-up items.
- Limits are configurable through BrokerConfig; CLI currently exposes only bind.
  Tracing/traffic metrics, authentication/ACL, TLS and WebSocket remain pending.
- Module dependencies remain protocol+router -> transport -> executable. Session
  and storage remain independent with no accidental coupling into Phase1.


## Original next phase plan (historical)
1. Freeze QoS packet/property and durable session contracts.
2. QoS 1 ACK/retry and QoS 2 explicit state machines with crash recovery tests.
3. Retained delivery, Will conditions, persistent sessions and offline queues.
4. MQTT 5 Clean Start/session expiry/reason codes/properties/receive maximum.
5. Authentication/ACL extension interfaces and telemetry.
6. Reproducible stress/fuzz/slow-consumer and crash tests before production claims.

## Phase 2a accepted — 2026-10-28

Six real specialist agents worked in independent branches/worktrees in two waves,
with at most three active children. The coordinator froze phase2a contracts first,
integrated commits on `integration/phase2a`, and did not reimplement completed
codec/router/incoming-QoS state. Remaining dependencies are tracked in tasks.md.

Delivered: clean-session MQTT3.1.1 bidirectional live QoS1/2, maximum matching
subscription grants, bounded incoming/outgoing windows and ACK ID lifetimes.
MemoryStorage gained atomic bounded batches with rollback; this remains unconnected
to runtime persistence. The executable exposes all BrokerConfig resource limits
through CLI, rejects invalid inputs and provides --help.

| Specialist | Isolated original commit | Integrated commit |
| --- | --- | --- |
| Router | 15b8eb4 | c369660 |
| Session/QoS | d2cdffc + fa5a818 (docs) | d122aca + 28017e7 (docs) |
| Protocol | e45b567 | 4ffaac5 |
| Quality | a68ac8c + 0df7570 | b94cadc + 443e20f |
| Storage | 791ddf1 | 02c2741 |
| Transport | 929dc96 | 299b329 |

Actual acceptance environment remains Darwin27.0.0 arm64, Rust/Cargo1.98.1,
Tokio1.54.1 and Paho2.1.0. Loopback tests run with local-network permission.
No remote CI-host execution, persistence validation or performance claims.

| Check | Actual result |
| --- | --- |
| cargo fmt --all -- --check | passed |
| cargo clippy --workspace --all-targets --offline -- -D warnings | passed |
| cargo test --workspace --offline | 55 passed: protocol13/router9/session7/storage10/transport14/CLI2 |
| cargo build --workspace --offline | passed |
| python3 tests/blackbox.py | 14 passed, 3.463 seconds |
| python3 tests/qos_blackbox.py | 9 passed, 2.186 seconds (includes wire3x3 matrix) |
| Paho tests/paho_interop.py | full3x3 publication/subscription QoS matrix passed |
| CLI --help and --max-inflight65536 | help successful; invalid bound exits1 |

### Review and defects
- Coordinator found continuous ready delivery queues could starve socket reads
  because of biased selection. A real TCP regression failed before the fix
  (PING not handled within the test deadline), then passed with fair selection.
  This timing is a correctness assertion, not a latency benchmark.
- Independent protocol-agent review found no material session/router defects.
  Temporary public-API model checks also passed, but are not committed CI tests
  and are not presented as a performance baseline.
- Independent quality-agent review checked ACK ordering, inflight rejection, ID
  namespaces and generation safety. No further blocker found. Existing active
  write cancellation/Keep Alive responsiveness remains a known test/architecture
  limitation bounded by write_timeout; do not claim complete protocol conformance.

### Current incomplete requirements
MQTT5, persistent sessions and reconnect retry, offline queues, durable storage and
crash recovery, Retained, Will, credentials/auth/ACL, TLS/WebSocket, telemetry,
fuzz/stress/performance baselines and production extensions remain unfinished.
Session is now integrated for live clean-session QoS; storage is still independent.
Durable exactly-once/application processing is not guaranteed. See tasks.md for
full dependency groups and explicit next acceptance gates.

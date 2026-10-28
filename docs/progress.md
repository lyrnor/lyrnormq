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

### Phase2a incomplete requirements (historical; superseded by Phase2b)
MQTT5, persistent sessions and reconnect retry, offline queues, durable storage and
crash recovery, Retained, Will, credentials/auth/ACL, TLS/WebSocket, telemetry,
fuzz/stress/performance baselines and production extensions remain unfinished.
Session is now integrated for live clean-session QoS; storage is still independent.
Durable exactly-once/application processing is not guaranteed. See tasks.md for
full dependency groups and explicit next acceptance gates.

## Phase2b accepted — 2026-10-28
Contract ae3a3cf freezes durable session records, transactional recipient outboxes,
Clean Session reset, ID/stage-preserving reconnect replay and ACK ordering. Storage,
Session and Protocol work in isolated p2b worktrees. SQLite uses an explicit bounded
versioned whole-registry snapshot for this increment; it is not a row-level storage
performance optimization. Independent network tests must use successful protocol
ACKs as durability barriers and SIGKILL/restart as process-crash validation.

### Integrated implementation and review

Five specialist roles used isolated p2b branches/worktrees: Storage and Session
implemented in parallel, Protocol enabled non-clean CONNECT, Quality independently
implemented wire/Paho/fault tests, then Transport integrated the tested APIs.
Coordinator froze the contract before implementation and owns CLI/docs/integration
on integration/phase2b. Existing Router/QoS primitives were reused.

| Specialist | Isolated original commit(s) | Integrated commit(s) |
| --- | --- | --- |
| Protocol | 7f290b6 | 89c7dd4 |
| Storage | 6c2eb38 + a67abf7 | f56da07 + 40b350b |
| Session | b516d67 + 5df3f10 + 130d22e docs | 8263470 + f62e57a + d3d9e27 |
| Quality | 344c2be + 1ccd079 + 9b9e307 + afd199c | 170eb9a + 3aee18f + d9140da + 3a247e2 |
| Transport | 7f3b8d7 + 89219a5 | a26823a + e9ea6df |

Persistent SQLite mode now commits subscriptions, incoming QoS2 and queued/inflight
outboxes before acknowledgement/transmission. It supports offline QoS1/2, ordinary
TCP reconnection, original-ID/DUP replay and PUBREL-only replay, session-present,
Clean Session reset and generation-safe takeover. The durable path uses a bounded
spawn_blocking transaction worker, fair reader/writer scheduling and bounded queues.
CLI adds storage-path/max-sessions/max-state-bytes; no SQLite path keeps the prior
clean-session-only mode. SQLite WAL/FULL is verified, with strict schema/load bounds
and permanent canonical-sidecar OS locking.

Review found and fixed concrete regressions before delivery:
- Direct DB-inode OS locking conflicted with SQLite on Darwin; contract was amended
  before integration to a permanent canonical-path OS sidecar lock.
- A dangling symlink could create different alias/real sidecars and two owners;
  an independent harness reproduced it, then a failing test preceded fail-closed fix.
- Restoring a large subscription after lowering max_packet_size lacked a complete
  SUBSCRIBE-size check; a failing quota regression preceded the correction.
- ACK before a pending local replay was queued could leave obsolete PUBLISH after
  PUBREL; a deterministic worker/wire scheduling regression failed then passed.
- Cancelling a connection drained a blocked writer queue, multiplying write timeout;
  a real tiny-buffer TCP/full-queue regression failed then passed after aborting
  writers on close. Protocol rejection flush is restricted to an actually queued
  pre-CONNECT rejection CONNACK.

Independent session review and temporary public-API models found no remaining
model blocker (including ID wrap/order); these extra temporary models are not CI
artifacts or performance benchmarks. Independent storage/worker review findings
were fixed; final acceptance runs committed, reproducible suites.

### Remaining requirements and limitations after Phase2b
MQTT5, Retained, Will, credentials/auth/ACL, TLS/WebSocket, metrics/config reload,
continuous fuzzing, broader client/fault matrices and measured performance remain
unfinished. SQLite row-level migrations/backup tooling/disk quotas are pending.
Whole-registry JSON commits/clones and routing-index rebuilds are potentially
expensive; snapshot size is not an aggregate process-memory/disk budget. Worker
transaction waits can still delay controls. Hard-link aliases/external active-DB
modification are unsupported. SIGKILL recovery and storage SQLITE_FULL rollback
are tested; physical power-loss behavior and application-side exactly-once
processing are not claimed. Current architecture/task dependencies are in
architecture.md and tasks.md.

### Final Phase2b acceptance results

Final local environment: Darwin27.0.0 arm64, rustc/cargo1.98.1, Tokio1.54.1,
rusqlite0.41.0/bundled SQLite3.54.1, serde1.0.229/serde_json1.0.154 and Paho2.1.0.
Root Cargo.lock is committed. TCP and subprocess tests ran with local-network
permission; no remote CI-host execution is claimed.

| Final command / mode | Actual result |
| --- | --- |
| cargo fmt --all -- --check | passed |
| cargo clippy --workspace --all-targets --offline -- -D warnings | passed |
| cargo test --workspace --offline | 77 passed: protocol14/router9/session15/storage16/transport20/CLI3 |
| cargo build --workspace --offline | passed |
| python tests/blackbox.py / legacy | 14 passed, 3.523s |
| python tests/qos_blackbox.py / legacy | 9 passed, 2.282s |
| python tests/paho_interop.py / legacy | 3x3 QoS matrix passed |
| tests/blackbox.py / SQLite enabled | 14 passed, 3.545s |
| tests/qos_blackbox.py / SQLite enabled | 9 passed, 2.471s |
| tests/paho_interop.py / SQLite enabled | 3x3 QoS matrix passed |
| python tests/persistence_blackbox.py | 14 passed, 3.395s |
| python tests/persistence_paho.py | persistent reconnect/offline QoS1+2/SIGKILL restart without resubscribe passed |

This is 60 TCP test-method executions across the two existing modes and the
persistence suite, plus the Paho checks. Durations are functional-suite elapsed
times, not broker latency/throughput baselines. SQLite mode for reused suites is
enabled by RMQTT_TEST_STORAGE_PATH; each suite used its own temporary DB. Paho
commands used /tmp/rmqtt-client-venv/bin/python with Paho2.1.0 installed. The CI
workflow reproduces the temporary-database split.

Storage-level tests include an actual SQLITE_FULL error induced with SQLite page
quota (not filling the host disk), prior-state rollback, SIGKILL of an uncommitted
transaction and OS-lock release. Network fault cases include ordinary immediate
TCP reconnect, SIGKILL at protocol-confirmed state barriers, original send order,
QoS2 AwaitComp PUBREL-only recovery, never-read PUBCOMP and single onward fanout,
full-outbox rollback across multiple recipients, takeover/late cleanup and corrupt
DB/snapshot startup rejection.

The final transport patch also added a regression for established-client version
errors: only an actually queued pre-CONNECT rejection CONNACK may flush; inferring
flush from UnsupportedVersion alone could drain a slow business queue. That
regression failed before the explicit flag and passed after. All commands above
were rerun on this final functional implementation.

## Module layout refactor accepted — 2026-10-28

All five crate lib.rs entry points now contain only crate docs, module declarations
and re-exports (protocol14/router11/session20/storage15/transport10 lines). Business
types/functions/impl blocks and test bodies live in responsibility-specific modules.
The convention is recorded in AGENTS.md; architecture.md lists the resulting layout.
Public root import paths, configuration, protocol behavior and durable schema1 are
preserved. No new business feature or dependency was added.

Five specialists used isolated worktrees. Integrated commits:

| Crate | Specialist commit | Integrated commit |
| --- | --- | --- |
| protocol | cf1c560 | 737a7d4 |
| session | ed38825 | b8e9a31 |
| router | e92ac44 | f7a5d37 |
| storage | 22ad82f | ad18805 |
| transport | 48083b0 | c8efdf2 |

Review confirmed SQLite durable.rs is unchanged, session durable.rs changes only
its private state import, and transport durable.rs changes only its deadline helper
path. Internal sibling access uses parent-scoped visibility; external APIs are not
expanded. Existing tests were moved, not replaced with weaker checks.

Actual final checks: fmt, strict workspace Clippy, build, all77 existing Rust tests,
and RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --offline passed.
A source audit confirmed no business definitions remain in any lib.rs. Existing
TCP checks passed in both modes (legacy14+9, SQLite14+9), persistent/crash tests14
passed, and both Paho3x3 matrices plus persistent Paho reconnect checks passed.

An additional real compatibility test created a synthetic SQLite DB using the
pre-refactor 5dc60c4 binary and killed it with persistent subscriptions, an outgoing
AwaitComp, a queued QoS1 and an incoming pending QoS2 record. The refactored binary
restored session-present, the exact PUBREL ID and both queued/pending payloads
without schema migration. This is a one-off cross-version validation, not a new
committed test fixture. Runtime capabilities and remaining feature tasks are unchanged.

## Phase2c accepted — 2026-10-28
05e78a2 froze Retained Store, Will protocol/lifecycle, schema2 migration and shared
worker contracts before parallel implementation. Protocol and Retained Store are
integrated; Session adds global retained state, ordered subscription replay,
connection-bound active/pending Wills and atomic publication through existing
outboxes. Quality implemented independent both-mode wire/Paho checks. Network
integration and unified acceptance are completed below. The pre-change binary also
created a real schema1 DB with unacknowledged outgoing and pending incoming QoS2
records for cross-version migration verification, rather than assuming compatibility.

### Final Phase2c delivery

Five real specialist agents used isolated branches/worktrees after contract05e78a2:
Protocol and RetainedStore proceeded alongside Session implementation; Quality
used a released slot; Transport started from integrated dependencies. Existing
QoS primitives/allocator/Router/outbox/SQLite were reused. No business definitions
were added to lib.rs. The old duplicate live production path was removed and its
meaningful regression tests now exercise the shared worker.

| Specialist | Original commit(s) | Integrated commit(s) |
| --- | --- | --- |
| Retained Store | a90b1d9 | 5d1aeaf |
| Protocol | ce4a5fd | c88a1df |
| Session | ff07108 + 6bf62d2 | 0d0844f + a40952d |
| Independent Quality | 5c0f547 + 2475371 | 6e33720 + b3d77aa |
| Shared Runtime | a07196e | 33ead95 |

Implemented global retained replacement/deletion, retained QoS storage, ordered
new/replacement subscription replay with correct RETAIN/QoS flags, normal live
RETAIN=0, and QoS2 retained change only on PUBREL. Will supports binary/empty
payloads and QoS0/1/2, retain updates/deletions, normal cancellation, abnormal
EOF/RST/protocol error, Keep Alive timeout and generation-safe takeover. Pending
capacity-blocked Wills are retained/retried atomically, including when Poll(None)
causes progress elsewhere. Active clean-origin Wills recover after SIGKILL with
SQLite. Schema2 required globals are strictly validated; valid schema1 migrates
explicitly with original session IDs/stages/order preserved. CLI adds max-retained.

Independent protocol-agent audits found no RetainedStore/Session blockers; temporary
model comparisons were also run but are not committed CI artifacts or performance
baselines. Quality independently reviewed commit/notification/generation ordering.
Coordinator found an additional liveness edge after the first integration passed:
a persistent source subscribed to its own Will with full inflight+outbox could
never reconnect to free capacity. Contract da0ef83 was updated first. A failing
model regression reproduced Capacity; the minimal guard now permits no-new-Will
resume while retaining the older pending intent, but still rejects overwriting it
with a new Will. Unit and independent TCP tests verify graceful/abnormal reclose,
DUP replay, queue draining, one old Will and no repeat after restart. Unrecovered
active records cannot use this bypass.

### Final actual verification
Local environment remains Darwin27.0.0 arm64, Rust/Cargo1.98.1, Tokio1.54.1,
rusqlite0.41.0/bundled SQLite3.54.1 and Paho2.1.0. TCP tests run with local-network
permission. No remote CI-host run or physical power-loss/performance claim.

| Check on final functional code | Actual result |
| --- | --- |
| cargo fmt --all -- --check | passed |
| cargo clippy --workspace --all-targets --offline -- -D warnings | passed |
| RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --offline | passed |
| cargo test --workspace --offline | 104 passed: protocol19/router14/session29/storage16/transport23/CLI3 |
| cargo build --workspace --offline | passed |
| lib.rs source audit | all5 contain only docs/modules/re-exports |
| basic wire / memory | 14 passed, 3.468s |
| QoS wire / memory | 9 passed, 2.311s |
| Retained/Will wire / memory | 14 passed, 6 SQLite-only cases explicitly skipped; 4.783s |
| basic wire / SQLite | 14 passed, 3.471s |
| QoS wire / SQLite | 9 passed, 2.388s |
| Retained/Will wire / SQLite | 20 passed, 6.565s |
| persistence_blackbox.py | 14 passed, 2.926s |
| paho_interop.py / both modes | both3x3 QoS matrices passed |
| retained_will_paho.py / both modes | update/delete/resubscribe and normal/abnormal retained Will passed |
| persistence_paho.py | persistent reconnect/offline QoS1/2/SIGKILL recovery passed |

These are94 passing TCP test-method executions plus6 explicitly inapplicable
volatile-mode skips and the separate Paho checks; elapsed suite times are not a
throughput/latency benchmark. RMQTT_TEST_STORAGE_PATH selected SQLite for reused
suites; each suite had an isolated temporary DB, as in the updated CI workflow.

A real schema1 DB was generated by the pre-change binary, containing subscriptions,
an unacknowledged outgoing QoS1 ID and pending incoming QoS2. The new executable
restored session-present, replayed the same ID/DUP, completed the old incoming
exchange, committed schema2, and successfully stored/replayed a new retained
publication. This supplements the committed schema-migration tests with a one-off
actual old-binary compatibility check; no synthetic fixture is shipped as production
data. Remaining work is updated in tasks.md; Retained and Will are no longer TODOs.

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


## Phase2d accepted — authentication, ACL and durable identity (2026-10-28)

The frozen phase2d contract768f689 plus policy-limit clarification5dfc23c preceded
implementation. Five actual Codex specialist agents used independent worktrees and
branches under .worktrees/p2d-{auth,protocol,session,transport,quality}. Auth, Protocol
and Session ran in parallel; after a slot freed Quality authored independent tests,
and Transport integrated real dependency commits. Coordinator owns CLI/policy IO,
contracts, Cargo.lock, documentation and unified integration on integration/phase2d.
No unverified work was committed to main. Existing storage, routing, retained store,
QoS state machines and transactional outboxes were reused.

| Responsibility | Original commit | Integrated commit |
| --- | --- | --- |
| Credential codec/redacted Password | ce7099e | 9cb61b5 |
| Bounded local policy/Argon2id/containment | 80abb70 | f4f8e38 |
| Identity/session/Will/outbox/schema3 | a7ca213 | 26e0c38 |
| Authenticated network transaction integration | 3bc7054 | 48331e3 |
| Independent wire/Paho/CI and schema1 regression | 6037a42 | 8e47fc8 |
| Failure-first refusal-order regression | 2e2ce14 | bc30fc5 |
| Refusal-order fix and worker regression | 7f6a759 | 1d26af4 |

The new mqtt-auth crate supplies bounded synchronous AccessControl hooks, stable
non-secret Principal labels, backward-compatible AllowAnonymous and an immutable
local StaticPolicy. JSON is capped at1MiB, users1024, names256bytes, each grant
list128; Argon2id v19 PHCs require checked m/t/p minima and maxima65536KiB/10/4
before library parameter construction. Passwords are binary, owned bytes zeroized
on drop and Debug-redacted; no password/hash/raw CONNECT is serialized. Identity
labels deliberately include static usernames. The stdin hash example uses OS salt
and normal Argon2 defaults, with no password argv, logging or newline trimming.

Policy file IO runs bounded on spawn_blocking before TCP bind. CONNECT authenticates
before cloning model state; owner/new-Will checks precede takeover/reset. Bad
credentials produce CONNACK4, denied identity/prefix/owner/Will produce5, then bounded
writer flush/close. Pipelined data after refusal is not processed. Session/Will
ownership prevents another principal taking or clearing the same Client ID. Shared
anonymous identity remains explicit default behavior; it does not distinguish people.

ACL covers exact-topic publishing (including retained deletion), safe single-grant
subscription-language containment, retained replay, live/offline delivery, outgoing
retransmission and Will registration/firing. Incoming QoS2's original topic is
checked again before PUBREL completes. Restart with revoked permissions atomically
prunes forbidden subscriptions/queues/payload-bearing inflight records and Wills,
freeing IDs and replay cursors together. PollOutcome.changed forces commit even
when no packet returns. AwaitComp may complete its already accepted payload-free
handshake. Global retained values stay, protected by current recipient permissions.

JSON schema3 requires Session/Will owner fields; SQLite opaque container remains1.
Valid schema1 and2 explicitly migrate to anonymous, preserving stages/order/IDs,
never assigning an old session to the first authenticated claimant. Restricted
authentication can make legacy anonymous sessions inaccessible; offline identity
conversion/removal remains unimplemented. Owner metadata may require extra snapshot
limit headroom. Malformed/unknown schemas and incompatible quotas still fail closed.

Independent read-only review checked both policy/model and runtime/CLI. Additional
standalone model probes passed takeover, revoked Will/outgoing state, usable freed
windows and denied original QoS2 replay without replacement. The actual Rust
coverage function was checked against34,225 filter pairs and3,904 reference topics
to depth5: none of the1,672 admitted pairs produced an observed false grant. This
is bounded independent validation, not a formal proof of all filter languages.

Review found one contract-order defect: memory-mode non-clean CONNECT checked
persistence availability before owner/Will authorization and returned bare EOF
instead of CONNACK5. Quality committed a failing two-subcase wire regression first;
Transport also reproduced the failure in a worker test before moving capability
checks after successful proposed authorization and before registration. Valid but
unsupported memory persistence still closes without installing a proposal. Old
owner PING, original Will, retained data and rejected pipeline remain unaffected.
A separate early test failure was only a QA read-order assumption about a second
already-queued PUBLISH; the test now reads both ordered messages before ACK/PING.

### Actual verification
Environment: Darwin27.0.0 arm64, rustc/cargo1.98.1, edition2024, Python3.14.7,
paho-mqtt2.1.0 in /tmp/rmqtt-client-venv. Dependencies were resolved from cache
with --offline; Argon2 is RustCrypto0.5.3 and zeroize1.9.1.

- cargo build --workspace --offline and --examples: passed.
- cargo fmt --all -- --check: passed.
- cargo clippy --workspace --all-targets --offline -- -D warnings: passed.
- cargo test --workspace --offline:141 passed: auth13, protocol23, router14,
  session40, storage16, transport29, CLI6.
- RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --offline: passed.
- All six crate lib.rs entry points audited as docs/mod/reexports only.

| Real TCP/client suite | Volatile mode | SQLite mode |
| --- | --- | --- |
| blackbox.py |14 passed|14 passed|
| qos_blackbox.py |9 passed|9 passed|
| retained_will_blackbox.py |14 passed,6 inapplicable skips|20 passed|
| auth_acl_blackbox.py |13 passed,7 inapplicable skips|20 passed|
| auth_acl_paho.py |2 passed,1 inapplicable skip|3 passed|
| paho_interop.py |3x3 QoS matrix passed|3x3 QoS matrix passed|
| retained_will_paho.py |passed|passed|
| persistence_blackbox.py |not applicable|14 passed|
| persistence_paho.py |not applicable|offline QoS1/2/reconnect/SIGKILL passed|

The full old+new matrix passed before the narrow capability-order fix; afterward
the complete final Rust checks, both-mode updated authentication/Paho suites and
the affected existing rejected-CONNECT/Will regression passed again. This accounts
for127 passing wire-method executions in the combined current matrix,
plus client checks; repeated focused runs are not counted again. Skips are explicit
mode applicability, not suppressed failures. No throughput/latency result is claimed.

A real Phase2c binary generated a schema2 DB before new executable build, with
an unacknowledged QoS1, AwaitComp, pending incomingQoS2, retained value and active
persistent-source Will. After SIGKILL the new binary migrated to3, bound all
legacy owners anonymous, replayed the same QoS1 ID/DUP and PUBREL-only AwaitComp,
completed original incomingQoS2, and replayed retained/crash-Will values. The
one-off fixture/old binary were cleaned after success. Committed tests separately
cover v1/v2 migration and refusal to transfer anonymous data to authenticated users.

### Remaining scope
MQTT5 (properties/reason codes/session expiry/Receive Maximum/AUTH/etc.), TLS,
WebSocket, remote auth, policy reload and per-identity rate limiting are still open.
Argon2 and global ACL reconciliation use the shared bounded state worker and can
delay controls under load; isolation/rate limits, batching/pruning efficiency,
row-level persistence and measured stress/fuzz/latency/memory campaigns remain.
Snapshot-byte caps do not establish process-memory/disk budgets. Physical power
loss, hard-link/external DB modifications, production readiness and full MQTT
conformance are not claimed. Detailed next dependencies are maintained in tasks.md.


## Phase2e accepted — initial MQTT5 wire/session implementation (2026-10-28)

The coordinator froze phase2e.md atf813e0b before parallel code work. Four actual
Codex specialists wrote only owned modules in independent branches/worktrees under
.worktrees/p2e-{protocol,session,transport,quality}. Protocol, Session and Quality
started together; Transport used the slot released by Protocol and integrated real
dependency commits. Protocol later independently reviewed model and production
network code. Coordinator owns contracts, dependency lock, documentation and final
integration on integration/phase2e. main was not used for unverified implementation.

| Responsibility | Original commit | Integrated commit |
| --- | --- | --- |
| Versioned protocol/Properties/ReasonCode |7c47979|b0c1492|
| MQTT5 session expiry/quota/metadata/schema4 |55a1d7f|1c7fe1d|
| Versioned worker/production network/writer |6d9f6cb|fc1df49|
| Protocol classification regressions/fixes |089ef78|74fef4e|
| Wire bounds and unavailable subscription features |4625e0a|beedc64|
| Required deadlines/offline version preflight |0b97ea0|6ea8318|
| Independent mixed-version TCP/Paho/CI |96639a0|f322be8|
| SQL-only startup observation barrier |3852331|1471287|

Contract clarifications were made before affected fixes: partial header detection,
specific typed errors, terminal-frame bounds, normative property/option classification,
unavailable subscription feature disconnects, cross-version frame-size admission
and known offline-version preflight. No contract mistake was preserved merely to
make an implementation pass. Public legacy methods/variants remain; the additive
Packet::Auth variant requires external exhaustive matches to account for it.

### Implemented scope and reuse

ProtocolVersion/Frame separate connection-version metadata from the existing Packet
and canonical MQTT311 QoS bytes. All27 standard property identifiers have typed
wire handling, ordered/repeated UserProperty support, legal context/value/cardinality
checks and bounded incremental lengths. Typed ReasonCode validates per-packet sets;
ACK/DISCONNECT/AUTH short forms are supported. Password and AuthenticationData Debug
are redacted. V5 syntactically permits password without username; local authentication
still decides whether credentials are acceptable.

One SessionEngine/router/retained store/transactional worker is reused for both
versions. Clean Start independently resets old session state. Expiry defaults0,
supports finite seconds orUINT_MAX, and can be changed on DISCONNECT except0→positive.
Finite offline deadlines use injectable millisecond Clock and persist across reboot.
Online sessions do not expire. An active-at-crash finite session conservatively gets
restart-now+interval because the failure instant is unknown; known offline deadlines
are not refreshed. Timer-only cleanup commits without client traffic.

ReceiveMaximum is connection-local and never persisted. Replay can honor a new
window smaller than durable pending count; positive PUBREC holds quota through
PUBCOMP, negative ACK releases the matching stage, and PUBREL/control bypasses
blocked PUBLISH. Inbound credit IDs/QoS/generation tickets remain until relevant ACK
write_all completes. Stale callbacks cannot free a reused ID; mixed-QoS ID collisions
are rejected. Production writer tests verify the actual implementation, not a
retired cfg(test) copy. A priority terminal signal waits for at most the current
complete frame, discards queued data, then writes DISCONNECT; total shutdown is
bounded by two frame write deadlines and never appends after an interrupted frame.

UserProperty/ContentType/PayloadFormatIndicator/ResponseTopic/CorrelationData survive
incoming QoS2 original ownership, retained update/delete/replay, Will, outgoing
retransmission, offline queues and restart. Metadata is stripped only for MQTT311
wire output and retired with its QoS payload/ID. Auth/ACL and principal ownership
continue to apply across version changes and expiry. MQTT5 positive expiry works
across disconnects in volatile mode; process durability requires SQLite. The prior
MQTT311 non-clean/no-SQLite refusal policy remains unchanged.

Business JSON is schema4; SQLite opaque container remains1. Explicit1/2/3 converters
add empty metadata/infinite persistent expiry and preserve existing owner semantics
(1/2 anonymous,3 original identity). Session records add session_expiry_interval and
required nullable expires_at; incoming tuples are(id,canonical_bytes,properties),
outgoing records add properties, and queued/retained/WillData require properties.
Runtime version/quota/sent sets are omitted. Unknown/corrupt/incompatible records
fail startup without resetting; even expired data is validated before pruning.

### Reproduced defects repaired before acceptance

- An early PUBREC before replay polling left a cursor that generated an extra
  automatic PUBREL; a failing regression preceded cursor retirement.
- Empty-properties311 messages grew beyond the configured complete-frame cap when
  wrapped for5:64→65 and Remaining Length boundary129→131 were independently
  reproduced. Unclean5 resume now refuses0x83 before takeover if queued/replay data
  cannot fit, preserving old IDs/Will and SQLite311 drain. Recipient preflight,
  retained replay and final poll prevent oversized admission; known offline5 peers
  also preflight so volatile queues cannot accumulate undrainable data.
- Shared/SubscriptionIdentifier capability flags0 require DISCONNECT9E/A1, not a
  per-filter SUBACK. The model now rejects before installing any filter.
- Reserved SUBSCRIBE bits require Malformed81, while QoS3/RH3 and semantically bad
  ResponseTopic require Protocol82; invalid lexical UTF8/NUL/noncharacters remain81.
  Wrong/unknown property contexts use81; zero TopicAlias uses typed94.
- Serde's implicit missing Option default accepted omitted expires_at as active and
  refreshed an offline deadline. Independent failure and owner regressions preceded
  a required-nullable deserializer; explicit null and legacy migration remain valid.

Independent review/probes also passed finite expiry, negative-ACK release, old
Released PUBREL bypass, metadata retirement, schema strictness,32 consecutive
actual-wire ID reuses, inbound quota/duplicate behavior, CONNACK-before-error ordering,
takeover8E/Will once and idle expiry. No new unresolved implementation defect was
reported. Initial QA read-order assumptions and TCP-listener readiness assumptions
were repaired without weakening message or commit requirements. Startup/expiry SQL
checks now poll boundedly for the committed row state without sending MQTT commands
that could mask missing timer/boot behavior.

### Actual verification

Environment: Darwin27.0.0 arm64, rustc/cargo1.98.1, edition2024, Python3.14.7,
paho-mqtt2.1.0. Dependency resolution/build used --offline. Final tested executable
SHA256:37a73df3a0b703259fa91e1f376b4d852cecf76839056d8fca8522da3b3b7e6c.

- cargo build --workspace --offline: passed.
- cargo test --workspace --offline:183 passed (auth13,protocol37,router14,session60,
  storage16,transport37,CLI6).
- cargo fmt --all -- --check: passed.
- cargo clippy --workspace --all-targets --offline -- -D warnings: passed.
- RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --offline: passed.
- All six lib.rs files audited as docs/module declarations/reexports only.

| Actual TCP/client suite | Volatile | SQLite |
| --- | --- | --- |
| mqtt5_blackbox.py |31 passed,11 SQLite-only skips|41 passed,1 volatile-only skip|
| mqtt5_paho.py |4 passed,1 SQLite-only skip|5 passed|
| blackbox.py |14 passed|14 passed|
| qos_blackbox.py |9 passed|9 passed|
| retained_will_blackbox.py |14 passed,6 inapplicable skips|20 passed|
| auth_acl_blackbox.py |13 passed,7 inapplicable skips|20 passed|
| auth_acl_paho.py |2 passed,1 inapplicable skip|3 passed|
| paho_interop.py |3x3 QoS matrix passed|3x3 QoS matrix passed|
| retained_will_paho.py |passed|passed|
| persistence_blackbox.py |not applicable|14 passed|
| persistence_paho.py |not applicable|offline QoS1/2/SIGKILL passed|

All suites ran against the integrated executable; only the final SQL-readiness
barrier adjustment changed test code afterward and its two focused cases plus full
SQLite42-method suite passed again. New MQTT5 tests account for72 passing wire
method executions plus9 Paho methods, with13 explicit mode-only skips. The reused
wire suites account for127 more passing executions (199 combined wire executions);
internal mixed-version QoS combinations and repeated focused runs are not double
counted as methods. Elapsed suite time is not a performance benchmark.

A real pre-change Phase2d executable generated schema3 authenticated sessions with
unacknowledged QoS1/AwaitComp/pending inboundQoS2, retained state and active Will.
After SIGKILL, the new executable upgraded to4, preserved user:alice ownership and
old IDs/DUP, replayed PUBREL-only AwaitComp, rejected Bob's takeover87, resumed the
old pending input over5 and replayed retained/crash-Will values over5. Legacy311
session expiry stayed infinite while the new5 connection selected60seconds. The
one-off fixture/old binary were cleaned after success.

### Remaining scope

This increment does NOT claim full MQTT5 compatibility. Codec coverage is broader
than implemented runtime semantics. Client MaximumPacketSize negotiation, assigned
Client IDs, nondefault NoLocal/RAP/RetainHandling, MessageExpiry, positive WillDelay,
topic aliases, subscription IDs/shared subscriptions and enhanced AUTH remain
explicitly refused as documented in README/phase2e.md. MQTT311 conformance edge
cases, full direction/reason matrices, more client implementations, slow-worker
response deadlines, CPU/rate isolation, row-level persistence, operational fault/fuzz
coverage and reproducible throughput/latency/memory baselines remain tasks.md work.
TLS/WebSocket/admin/cluster remain outside this increment; physical power-loss
behavior, production readiness and application-side exactly-once are not claimed.


## Phase2f — Message Expiry and Will Delay (2026-10-28)

Frozen contract: [phase2f.md](contracts/phase2f.md), initially704f81d; subsequent
receive-time, first-write, clock-floor and lifecycle clarifications were committed
before affected implementations. Architecture revision7 and tasks.md describe the
current boundaries. This increment reuses Clock, canonical QoS FSMs, trie/retained
store, SQLite WAL/FULL and the serialized session/outbox transaction pipeline.
No storage-engine or competing runtime rewrite was introduced.

### Implementation and isolation

A single isolated schema bootstrap preceded parallel Message, Will and Quality
work. Message and Will had disjoint source ownership in independent branches and
worktrees; Transport started from integrated real model commits. The coordinator
owned contracts/docs and cherry-pick integration on integration/phase2f; main was
not changed. Every lib.rs remains documentation/module declarations/reexports only.

| Specialist source commit | Integrated work |
| --- | --- |
| 72eb1ed24a303518c52d6cb73c81067183997b17 | schema5 required records and explicit legacy1..4 migration |
| a735822e72adaa2525ce497f515e6eb61301d870 | absolute message deadlines, retained/offline expiry, Delivery/Begin lifecycle |
| 1f62a2d4c3b877c6e67829f7b2de1e4a6535794f | ingress age, nonzero DISCONNECT Will handling |
| 4284f59035072df72c7f41d93c609ab224384fcb | durable logical clock floor and quiescent observations |
| 0a6feee2728152407db9516f646077bfcf276d4a | delayed Will scheduling/cancellation/session-end/recovery |
| a399fb5f7720a05bba5aeb480dc9f0ea7f58671c | CleanStart old-recipient removal and effective-time lifecycle |
| 090ca4c7060e88e45f9adcaf5aaa5ac2c800d6be | reader timestamps, transactional Begin, writer TTL refresh/timer |
| 764e1e74f99ec397c576248c709043254adf9d95 | effective-time Tick guard and rollback no-spin test |
| 206cb0a,30568bc,7c64a2d28e0b7ee5132d5e92d74e2479db90913a | independent wire/Paho/SQL/SIGKILL/strict migration tests |

MessageExpiry0 expires immediately; UINT_MAX is finite and absence unbounded.
Queued/offline/retained copies share their original absolute deadline; expired
replacement removes old retained data. QoS2 keeps its handshake until PUBREL even
when its payload expires. Before first write, stale/expired leases retire complete
unstarted exchanges and IDs; started QoS1/2 remain protocol obligations and may
replay interval0, while AwaitComp remains PUBREL-only. Receive timestamps include
coalesced-buffer/actor waiting; TTL is refreshed after storage commit before output.

Will Delay uses the earlier delay/session-end deadline. Pre-due surviving-session
resume cancels it; CleanStart, expiry0 or due time forces it. Only reason00 cancels;
04/80 and other valid nonzero reasons schedule. TTL starts at successful Will
publication after delay/capacity waiting. Known due times persist, active-at-crash
delay starts conservatively at restart capped by restored session expiry. Current
ACL and bounded pending-intent retry semantics are reused.

### Failure-first review and fixes

- Independent CleanStart/full-old-self-queue probes reproduced phantom-recipient
  rejection, including replacement-Will admission. Removing ended old session and
  subscriptions before forced Will routing fixes it while retaining real external
  capacity pressure and existing pending-intent protection.
- A separate clock rollback/restart probe reproduced remaining TTL growing6→9.
  Required clock_floor and effective-time lifecycle/timer restore committed temporal
  observations; clones do not share proposal clock mutation. Pure clock observation
  does not create repeated commits/notifications. Ingress raw age is rebased onto
  logical time, preventing premature expiry after rollback.
- Actor/buffer waiting, due-Will no-traffic scheduling, nonzero80 DISCONNECT and
  write-reservation boundaries gained reproduced regression coverage.
- Slow-reader blackbox uses a6MiB QoS0 frame, bounded120s write timeout, receive
  buffer set before TCP handshake, and a persistent reader. SQLite directly proves
  timed QoS1/2 reservations have started=false before expiry; after draining the
  full blocking frame, PINGRESP arrives without expired QoS0/1/2 payloads. Started
  frame/replay behavior has separate production-worker and wire coverage. Earlier
  too-small TCP windows/timeouts were harness failures; requirements were retained.
- Old schema fixtures explicitly remove schema5-only fields when constructing
  legacy versions. A prior imported helper failed one schema1 regression during
  integration; current strict helper and full affected SQL suites pass. Current5
  still rejects missing floor/deadline/start/delay fields rather than defaulting.

### Actual verification

Environment: Darwin27 arm64, Rust/Cargo1.98.1 stable, edition2024, Python3.14.7,
Paho MQTT2.1.0. Tests used the integrated debug executable built from production
commit b4b931c; subsequent ac960df changes only tests. SHA256:
396f7ce104f72dde3889c41dd24c123536914894829d36abf6682ea11dcbcffb.

cargo build/test --workspace --offline, cargo fmt --all -- --check, strict
all-target Clippy and RUSTDOCFLAGS=-D warnings rustdoc all pass. Rust tests:229
(auth13,protocol37,router14,session102,storage16,transport41,CLI6). Thin-lib audit,
all Python test compilation and git diff --check pass.

| New suite | Memory | SQLite |
| --- | --- | --- |
| expiry_will_blackbox.py |28 passed,10 mode-only skips (47.118s)|38 passed (58.809s)|
| expiry_will_paho.py |5 passed,1 mode-only skip (14.466s)|6 passed (16.027s)|

These are66 passing new wire-method executions and11 Paho executions; focused
reruns are not double-counted. Coverage includes offline/retained TTL/update/delete,
remaining TTL, mixed versions, QoS2 expiry/ID obligations, first-write reservations,
started replay, delayed normal/abnormal/timeout/takeover Wills, reconnect cancellation,
finite/zero/infinite Session Expiry interactions, no-traffic timers, strict legacy
migration, SIGKILL/restart and malformed state rejection.

Reused regression suites also pass against this executable (current strict fixture
helper for all legacy-data tests):

| Suite | Memory | SQLite |
| --- | --- | --- |
| blackbox.py |14 passed|14 passed|
| qos_blackbox.py |9 passed|9 passed|
| retained_will_blackbox.py |14 passed,6 skips|20 passed|
| auth_acl_blackbox.py |13 passed,7 skips|20 passed|
| mqtt5_blackbox.py |31 passed,11 skips|41 passed,1 skip|
| auth_acl_paho.py |2 passed,1 skip|3 passed|
| mqtt5_paho.py |4 passed,1 skip|5 passed|
| paho_interop.py |3x3 QoS matrix passed|3x3 QoS matrix passed|
| retained_will_paho.py |passed|passed|
| persistence_blackbox.py |not applicable|14 passed|
| persistence_paho.py |not applicable|offline QoS1/2 and SIGKILL passed|

The reused wire suites total199 passing method executions; combined with the new
suite this is265 wire executions. Mode-only skips are explicit, and Paho workflows
are reported separately. Local suite durations are reproducibility evidence, not
throughput/latency benchmarks.

A real accepted Phase2e executable generated authenticated schema4 sessions with
unacknowledged QoS1, AwaitComp, pending inboundQoS2, retained metadata and an active
Will, then was SIGKILLed. The new executable migrated to5, preserved Alice ownership,
IDs/DUP/application properties, replayed PUBREL-only AwaitComp, recovered the Will,
and completed old pending inboundQoS2 over MQTT5. New retained TTL expiry without
MQTT traffic and delayed-Will TTL then worked on that same upgraded database.
The one-off fixture/old binary were removed after success.

### Remaining work and measured limits

Client MaximumPacketSize negotiation, assigned IDs, nondefault subscription options,
topic aliases, shared/subscription IDs and enhanced AUTH remain unsupported as
listed in README. Full MQTT conformance/production readiness are not claimed.

Start intent and socket output have a conservative may-have-sent gap; no atomic
SQL/TCP guarantee exists. The clock floor bounds recovery by committed observations;
a later socket-only TTL refresh can be newer than durable time, so crash plus clock
rollback may restore a value above that last wire-only observation. Physical clock
accuracy and physical power-loss behavior are not certified. Additional Begin
transactions, full snapshots and deadline scans have unmeasured costs; no latency,
throughput or memory performance number is claimed. tasks.md tracks measurement,
row-scoped persistence, broader fault/fuzz/client campaigns, auth CPU/rate isolation
and TLS/WebSocket/admin work. Cluster remains deferred.


## Phase2g — Maximum Packet Size and Assigned Client Identifier (2026-10-28)

Frozen [contract](contracts/phase2g.md): initial2a4720a, diagnostic prefix helper
919c608, admission clarification b5a00e9. Architecture revision8/tasks.md/README now
supersede prior refusal of these two MQTT5 features. Existing protocol properties,
Clock, schema5/SQLite persistence, retained store, Will and QoS/outbox pipeline are
reused. Negotiated peer maximum is connection state; no schema migration or durable
allocator is introduced. All lib.rs files remain docs/module declarations/reexports.

### Worktree isolation, implementation and review

Session, Transport and Quality started on separate branches/worktrees after the
contract freeze. Quality captured old reason83/85 refusals as intended failures.
Its released slot enabled a Protocol specialist to reuse the canonical property
parser for safe CONNECT prefix inspection. The initial Transport agent encountered
service capacity errors before committing. That writer stopped; its existing diff
was preserved and copied to a new isolated p2g-transport-finish branch/worktree.
The continuation reused that implementation and completed verification; there was
no concurrent writing to the old tree or shared integration checkout. Coordinator
integrated tested commits on integration/phase2g; main was not changed.

| Specialist commit | Integrated commit | Work |
| --- | --- | --- |
| 637580f91d7b3567c51d3a3dfd4bf8afd1b71b94 |b56c9cc|runtime peer cap, complete-size discard/QoS retirement, ID occupancy query|
| 9c2237cd4c51aa3fe7d89f92af06b37b4951201a |3f2dc46|canonical prefix/property limit probe and boundary tests|
| f37398e179629776c3835cfe5118985185f08967 |a035f1f|entropy ID assignment, resolved-key worker/network, exact CONNACK admission, writer caps|
| a4c7dc1efa5d0aca1fbcc172b7ea9379c2de240c |f89fcfb|online preadmission discard/backlog pruning and genuine-backpressure guards|
| f3c8f7f,40a51b5,2f3c8c7,885fe19 |181ab67,fb02453,afa6bd4,f31b691|independent wire/Paho/CI and failure-first edge cases|

ConnectionOptions.maximum_packet_size and runtime session cap reset at each CONNECT
and restore. Complete encoded size includes all headers and variable-length fields.
Online guaranteed-oversize recipient copies are skipped before queue admission;
preexisting backlog is pruned before fitting traffic/retained replay. Poll/Begin
also enforce the cap and atomically retire QoS IDs/order/properties/deadlines/tickets/
credit, even old started PUBLISH replay. AwaitComp remains PUBREL-only. Offline
queues ignore old negotiated caps, while global retained records/other recipients
survive per-peer discard. Configured server resource/version-growth admission rules
and genuine fitting-message backpressure remain intact.

The worker allocates23-character alphanumeric identifiers with88bits of OS entropy,
bounded16 collision attempts against sessions AND Will records. Authentication,
Connect/Will ACLs, registry generation, cleanup, records and reply all use the
resolved key. Empty V5 ID with either CleanStart value returns assignment exactly
once in successful CONNACK. Explicit reconnect, including311 where persistent-mode
policy permits, uses normal owned-session recovery with no assignment property.
Entropy/authorization/admission/CONNACK-fit failures cannot mutate old state.

Exact successful CONNACK is preflighted before session/Will/takeover mutation;
nondefault capabilities/assignment remain present and default TopicAliasMaximum0
is omitted. At default limits the required success is17bytes,43 with assignment.
Limits below required size get a fitting five-byte reason95 refusal or EOF. Writer
guards every data/control/error/takeover/shutdown path; an oversized mandatory
control closes without truncation or partial output. The protocol-owned diagnostic
helper obeys the declared packet boundary, works before later payload completes,
and reuses valid property parsing; malformed properties establish no trusted cap.
Only cached getrandom0.2.17 was made a direct Transport dependency; Cargo.lock adds
that dependency edge without a new package/version.

### Reproduced defects and independent review

- The old broker refused valid MaximumPacketSize and empty V5 IDs; independent
  wire and real TCP tests failed before implementation. Existing codec support was
  reused instead of replacing encoding/decoding.
- A valid cap1 followed by malformed later CONNECT payload still caused the old
  error path to write CONNACK byte0x20. The shared prefix probe plus final writer
  cap fixes this; QA tests30 malformed-payload/Will subcases at multiple ceilings.
- An independently compiled public-model probe with max_queued1 reproduced a
  guaranteed-oversize, never-polled peer blocking a second source publication even
  after the healthy peer consumed the first. Owner reproduced that plus reconnect
  backlog/fitting admission and multiple oversized retained matches before fixing.
- Independent real TCP reproduced the retained variant: two large retained values
  and one fitting value with capacity1 yielded DISCONNECT instead of SUBACK. Final
  admission skips/prunes pass the same wire case in both modes; uncapped observers
  still recover both large global retained values. New unit guards retain genuine
  fitting-message backpressure and server-global311→5 size rejection.
- QA verifies original empty-ID CONNECT exactly64bytes with Will fits servercap64
  after assignment; normalized-key expansion no longer poisons synthetic validation.
- The Protocol specialist independently reviewed the full network/worker flow:
  no further defect found; final Session118 and Transport49 test runs pass. The
  original public-model queue-poison probe now returns successful PUBACK.

During final legacy Paho regression, immediate reconnect after expiry0 disconnect
once observed session_present=true. QA's deterministic SQL write-lock probe proved
the old helper returned before server commit: durable state still existed and a
duplicated socket had no remote EOF. Paho2.1 closes its own descriptor without
shutdown, so the test helper now duplicates it before disconnect, stops the Paho
reader and waits bounded remote EOF. No sleep/retry or production behavior changed;
this establishes actual server-side ordering for both memory and SQLite tests.

### Actual final verification

Environment: Darwin27 arm64, Rust/Cargo1.98.1 stable, edition2024, Python3.14.7,
Paho MQTT2.1.0. Integrated debug binary built from production commit f89fcfb SHA256:
6594cd086f7e3240ac26ace0112d7e8cf137adb8a35769cd2b3ce40d5f3e100d.

cargo build --workspace --offline; workspace test; fmt check; strict all-target
Clippy and Rustdoc all pass. Rust tests260: auth13,protocol44,router14,session118,
storage16,transport49,CLI6. Tests include deterministic bounded allocator collision/
entropy errors, resolved-key authorization and full writer/error/cap boundaries.

| New independent suite | Memory | SQLite |
| --- | --- | --- |
| negotiation_blackbox.py |23 passed,3 mode-only skips (15.675s)|26 passed (16.373s)|
| negotiation_paho.py |5 passed,1 mode-only skip (2.066s)|6 passed (2.091s)|

Totals49 executed new wire methods and11 executed Paho methods,4 explicit mode-only
skips. Nested QoS/varint matrices, concurrent-ID checks and focused reruns are not
counted as extra methods. Coverage includes exact/one-byte-over/variable-integer
width limits, property preservation, data/control/refusal/terminal bounds, peer
cap changes, queue/inflight discard and quota cleanup, retained preservation, TTL,
311↔5 bridging, both CleanStart values, unique assigned IDs, explicit resume,
owner/ACL denial, no-state on tiny CONNACK rejection, assigned Will, SIGKILL and
repeated restart with same IDs/QoS stages. SQLite confirms negotiated max is absent
from schema5 and retired oversized stages do not reappear after another restart.

Reused final regression matrix also passes on that same final production artifact:

| Suite | Memory | SQLite |
| --- | --- | --- |
| blackbox.py |14 passed|14 passed|
| qos_blackbox.py |9 passed|9 passed|
| retained_will_blackbox.py |14 passed,6 skips|20 passed|
| auth_acl_blackbox.py |13 passed,7 skips|20 passed|
| mqtt5_blackbox.py |31 passed,11 skips|41 passed,1 skip|
| expiry_will_blackbox.py |28 passed,10 skips|38 passed|
| persistence_blackbox.py |not applicable|14 passed|
| auth_acl_paho.py |2 passed,1 skip|3 passed|
| mqtt5_paho.py |4 passed,1 skip|5 passed|
| expiry_will_paho.py |5 passed,1 skip|6 passed|
| paho_interop.py |3x3 QoS matrix passed|3x3 QoS matrix passed|
| retained_will_paho.py |passed|passed|
| persistence_paho.py |not applicable|offline QoS1/2 and SIGKILL passed|

The reused wire suites total265 passing executions; with49 new methods the total
is314 wire executions. The Paho shared-helper ordering fix was the only test change
after production artifact freeze: existing MQTT5, Negotiation and Expiry Paho suites
were rerun in BOTH modes, plus the expiry0 disconnect case five times per mode, all
passing. Focused repeats are not double-counted. Thin-lib audit, Python compilation
and git diff --check also pass.

Suite durations are local reproducibility evidence, not performance baselines.
SIGKILL tests establish process recovery, not physical power-loss behavior.

### Remaining scope

Nondefault NoLocal/RAP/RetainHandling, Topic Alias, shared subscriptions/subscription
IDs and enhanced AUTH remain unimplemented as listed in README/tasks.md. Broader
MQTT311/5 direction/reason/client conformance, auth CPU/rate isolation, TLS/WebSocket,
observability and operational/fuzz/fault campaigns remain. Whole-registry snapshots,
extra first-write transactions, packet-size preflights and online backlog scans have
unmeasured CPU/memory/latency costs. Row-scoped persistence/indexing needs measured
baselines. Existing SQL/TCP start-intent ambiguity and clock-floor/wire-only time
boundary remain unchanged and documented. No full MQTT5 conformance, production
readiness or throughput improvement is claimed; cluster remains deferred.


## Phase2h — Subscription options and Subscription Identifier (2026-10-28)

Frozen [contract](contracts/phase2h.md) e789bea defines NL/RAP/RH, identifier
aggregation, indexed matching, schema6 and version bridging before parallel writes.
Architecture revision9, README and tasks.md describe current behavior. Existing
codec, trie, Clock, expiry/Will/ACL, SQLite storage and QoS/outbox transactions are
reused; no second runtime, storage engine or public Session API replacement.

### Isolated implementation and independent review

Session, Router and Quality started in separate Git worktrees/branches. Router's
completed slot enabled Transport/Protocol capability and codec checks. Session
cherry-picked the real Router dependency, then Transport independently reviewed
the completed model and reran real tests. Coordinator owned contracts/docs and
integration on integration/phase2h; main was not changed. All lib.rs files remain
crate documentation, module declarations and public reexports only.

| Specialist source | Integrated | Result |
| --- | --- | --- |
| 22fc779 |65bdb15|exact indexed subscription matches,4 new router tests|
| 90cb56a5b78a3ab925f8471aaa5c3222f93bd170 |329c6a2|identifier capability1 and codec option/varint guards|
| 6dd48c454eac10138fed72ac4368f7217a381d40 |d5611e1|full subscription records/schema6, recipient metadata and RETAIN provenance|
| 24a2de8f66267547f1f6e65fe8a854847da61165 |2b127eb|independent wire/Paho/legacy fixtures and both-mode CI|

Router::matching_subscriptions returns each indexed exact filter/client/grant once,
retaining overlapping matches without global filter scanning. Prior matching_qos
remains max-QoS deduplicated. Session applies ACL/No Local BEFORE eligible QoS,
identifier and RAP aggregation, including source QoS2 PUBREL after recovery and
Will routing by original ClientID. Live delivery remains one copy per recipient;
any eligible RAP1 match preserves the publisher retain flag. This mixed-match rule
is an explicit broker policy, not precedence mandated by OASIS or Mosquitto.

RH0 replays on new/replacement, RH1 only when that exact filter did not exist before
its sequential entry, RH2 suppresses replay. Denied entries leave existing records
unchanged. Retained subscription replay always uses RETAIN1 and its triggering
entry's identifier; No Local does not suppress that replay, matching established
Mosquitto retain__process behavior. Repeated filters within one SUBSCRIBE observe
preceding accepted entries. V311 resubscription replaces that filter with defaults.

Identifiers1..268435455 apply to all accepted filters in one SUBSCRIBE. Overlapping
eligible filters contribute one ID each, including repeated equal values; excluded
NL/ACL matches do not contribute or inflate QoS. Replacement with no ID clears it;
unsubscribe/session reset removes the record. Metadata is captured in recipient
queue/inflight state and survives retry/restart; changing subscriptions does not
rewrite accepted copies. UserProperty order stays intact. Source PUBLISH/Will,
incomingQoS2 and global retained metadata cannot contain subscription IDs.

Business schema6 adds strict full subscription records with REQUIRED nullable
identifier, plus required retained_replay flags for queued/outgoing payloads and a
runtime per-ID sidecar. It explicitly migrates1..5 with default options/no IDs,
preserving owners, clock floor/deadlines, IDs/stages/order/metadata and Wills. Legacy
queued/outgoing retain bits represented replay, so supply the provenance marker;
Comp=false with no payload metadata. Legacy metadata smuggling IDs is rejected.
V311 output strips IDs and masks live RAP retain to0 while preserving retained
replay1; canonical internal properties/retain choices remain available for laterV5.
Recipient full-wire size, negotiated cap discard before capacity, genuine queue
backpressure and server resource/version-growth checks remain enforced.

### Failure-first checks and review evidence

- New model NL+ID/RAP+ID tests initially received unavailable-identifier errors;
  RH2 initially received SUBACK83. Indexed-match missing-API compile-red and live
  capability0 assertion failed before implementation; completed owner tests pass.
- Independent actual TCP baseline failed capability availability and options/ID
  subscription acceptance. Completed wire/Paho tests preserve remaining shared,
  alias and enhanced-auth exclusions and input direction validation.
- Codec tests exercise36 valid option combinations across8 identifier varint
  boundaries, repeated identical server PUBLISH IDs and UserProperty order, while
  duplicate SUBSCRIBE IDs still fail. No duplicate property parser was introduced.
- Independent model review checked strict old5/current6 decoding, source/delivery
  trust, eligible aggregation, RH replacement/denial, negative ACK/PUBREC/expiry/ACL
  sidecar retirement, Poll AND Begin RETAIN masks, full envelope-before-capacity
  and absence of identifier contamination in globals. No new defect found;
  independent Protocol46/Session130/Transport50 test runs all pass.

### Actual verification

Darwin27 arm64; Rust/Cargo1.98.1 stable, edition2024; Python3.14.7; Paho MQTT2.1.0.
Integrated debug executable built from production d5611e1 SHA256:
2ab8a951aadd9048af08ea9008272201583b395d19dc5a3da246937a2e9b35ff.

Workspace build/test offline, fmt check, strict all-target Clippy and Rustdoc all
pass. Rust tests279: auth13,protocol46,router18,session130,storage16,transport50,
CLI6. No production unwrap/expect/unsafe or business definitions in lib.rs added.

| New independent suite | Memory | SQLite |
| --- | --- | --- |
| subscription_blackbox.py |20 passed,6 mode-only skips (3.776s)|26 passed (4.754s)|
| subscription_paho.py |5 passed,1 mode-only skip (2.929s)|6 passed (3.466s)|

Totals46 executed wire methods and11 Paho methods,7 explicit mode-only skips.
Coverage includes QoS0/1/2 NL and overlap, Will/source QoS2 after crash, RAP flags/
delete/replay, RH replacement/duplicate entries, accepted/denied identifier state,
repeated IDs, metadata order, V5↔311 recovery/provenance, peer envelope limits and
max_queued1, strict schema6 and legacy5 restore. Subcases/focused repeats are not
counted as separate methods. Test durations are not performance benchmarks.

All reused suites passed in both modes on this same integrated artifact:

| Suite | Memory | SQLite |
| --- | --- | --- |
| blackbox.py |14 passed|14 passed|
| qos_blackbox.py |9 passed|9 passed|
| retained_will_blackbox.py |14 passed,6 skips|20 passed|
| auth_acl_blackbox.py |13 passed,7 skips|20 passed|
| mqtt5_blackbox.py |31 passed,11 skips|41 passed,1 skip|
| expiry_will_blackbox.py |28 passed,10 skips|38 passed|
| negotiation_blackbox.py |23 passed,3 skips|26 passed|
| persistence_blackbox.py |not applicable|14 passed|
| auth_acl_paho.py |2 passed,1 skip|3 passed|
| mqtt5_paho.py |4 passed,1 skip|5 passed|
| expiry_will_paho.py |5 passed,1 skip|6 passed|
| negotiation_paho.py |5 passed,1 skip|6 passed|
| paho_interop.py |3x3 QoS matrix passed|3x3 QoS matrix passed|
| retained_will_paho.py |passed|passed|
| persistence_paho.py |not applicable|offline QoS1/2 and SIGKILL passed|

The reused wire suites total314 actual passing executions; with46 new methods,
this increment verifies360 wire executions. Both-mode runners completed successfully;
only tests intentionally tied to the other storage mode were skipped. Thin-lib audit,
all Python test compilation and git diff --check also pass.

A real accepted Phase2g executable generated authenticated schema5 sessions with
QoS1 retained-replay RETAIN1 and live RETAIN0, AwaitComp, retained metadata, pending
incomingQoS2 with original RETAIN1, and an active retained Will, then was SIGKILLed.
The new executable upgraded to6, preserved Alice ownership/default subscriptions,
IDs/DUP/flags/properties, replayed PUBREL-only AwaitComp and recovered the Will.
On that SAME database it completed the original pendingQoS2, applied new RAP/ID
forwarding, replaced a legacy subscription with NL+ID, suppressed its self publish,
and verified RH1 replacement did not replay old retained values. Global retained
properties had no recipient identifiers. The owned one-off fixture was removed
after success. This validates process recovery, not physical power loss.

### Remaining scope

Topic Alias, Shared Subscriptions and enhanced AUTH remain unsupported. Broader
MQTT311/5 direction/reason/client conformance, fault/fuzz/clock/crash campaigns,
metrics and auth CPU/rate isolation, TLS/WebSocket and operations remain tasks.md
work. Indexed match results/full subscription records/recipient property encoding
and whole-registry transactions have unmeasured costs; no throughput/latency/memory
baseline or full MQTT5 conformance/production-readiness claim. Existing SQL/TCP
start-intent and latest-wire-time boundaries remain documented. Cluster is deferred.


## Phase2i — Connection-local Topic Alias (2026-10-28)

Frozen [contract](contracts/phase2i.md) a9415e8 defines independent bounded map
ownership, canonical worker boundary, output write ordering and resource policy.
Architecture revision10/README/tasks.md supersede the old alias refusal. Schema6,
SessionEngine/QoS/outbox, subscription options/identifiers, Clock/expiry/Will/ACL
and the existing codec are reused. lib.rs remains docs/module declarations/reexports.

### Isolated implementation and review

Transport, Protocol and Quality wrote separate Git worktrees/branches. Coordinator
owned CLI/docs/integration. Protocol then independently reviewed the tested Transport
commit in its own tree and ran real TCP tests; no shared source writes. Integration
branch is integration/phase2i; main was not changed.

| Specialist source | Integrated | Work |
| --- | --- | --- |
| bf240c39162eb88d87228e6600d4fac7d03b1dac |a788cca|bounded input/output aliases, canonical normalization and postwrite binding|
| 787e12301489186ec339401d6654657e3c3a9862 |5632c38|6 codec syntax/size/UTF8/coalesced guards, no production parser rewrite|
| fa12bc4 |bbfc0f0|raw Wire/Paho tests, adapter disclosure, old expectations and CI|
| coordinator |d569ae3|CLI alias budget/default/disable/range tests|

BrokerConfig.topic_alias_maximum:u16 defaults32; CLI --topic-alias-maximum accepts
0..65535. The exact configured input maximum is advertised, including0; local
outbound entries are bounded by min(configured budget,CONNECT TopicAliasMaximum).
CONNACK adds3bytes: default success20/assigned46, preflight before prior session/
Will/takeover changes. Tiny peer packet maxima keep existing fitting reason95/EOF.

Input full Topic+alias binds/rebinds; empty Topic+known alias resolves. Duplicate
singleton remains82, empty-without-alias82, zero/out-of-range/unbound aliases94.
Bounded wire decode precedes canonical expansion; expanded canonical resource
exhaustion returns97 rather than false wire-size95. Alias properties are stripped
before worker/ACL/NL/routing/storage and original timestamp/flags/IDs/other metadata
are preserved. A duplicate QoS2 may update wire binding without replacing the
original stored QoS2 Topic/payload/properties/expiry; PUBREL uses that original.

Output state belongs to the writer. After committed Begin/TTL refresh/terminal
priority, prepare the exact next encoded frame; full-topic binding/reassignment is
committed only after complete write succeeds. Later references can use empty Topic.
Candidate encoding accounts for all varint widths and final peer/server caps.
Inflating bindings fall back to canonical output without installing phantom state;
known references are used only when smaller and fitting. Maps are sparse/count-
bounded, separate by direction/client, deterministic round-robin, and die with the
network connection including persistent resume/takeover/crash. No state is added
to schema6; replay establishes fresh mappings from canonical full Topic records.

Canonical resource/recipient admission is deliberately unchanged: optional alias
compression does not rescue a copy already dropped for canonical size. This policy
is documented rather than claimed as maximal compression optimization. Additional
map/string memory is outside serialized state-byte accounting and is separately
bounded by alias count/UTF8 Topic length; no performance baseline is claimed.

### Failure-first and independent evidence

- Old executable failed actual TCP capability discovery, valid aliased publication
  and outgoing compression checks. Transport's capability/handshake assertion was
  red before adding the nonzero maximum; the new20/46 boundaries pass.
- Existing codec already supported alias syntax; six focused guards cover allQoS,
  min/max values, binding/reference/rebind, zero94/duplicate82/empty-no-alias82,
  wrong contexts, Unicode, packet/varint boundaries and coalesced input.
- A valid-schema6 public-API corruption probe inserted real TopicAlias properties
  into incoming, queued, outgoing, retained and Will scopes; ALL were rejected while
  untouched baseline restored. Existing Session validators remain a canonical-
  boundary defense, not a claim that valid network aliases are unsupported.
- Independent read-only audit checked original wire bounds/canonical97/timestamps,
  ACL/NL/QoS2 retention, writer preparation/commit order, fallback/round-robin/map
  lifetimes and no durable leakage. No new defect found; independent Protocol52
  and Transport61 actual TCP/unit tests pass.

### Actual verification and interoperability boundary

Darwin27 arm64, Rust/Cargo1.98.1 stable, edition2024, Python3.14.7, Paho MQTT2.1.0.
Integrated debug binary including CLI d569ae3 SHA256:
51d141b123e2091ccdb793f3e45f4ea15fc40630dfd85c2d36e8da972cc64cc5.

Workspace offline build/test, fmt check, strict all-target Clippy and Rustdoc pass.
Rust tests297: auth13,protocol52,router18,session130,storage16,transport61,CLI7.
Thin-lib audit, Python compilation and git diff --check pass.

| New independent suite | Memory | SQLite |
| --- | --- | --- |
| alias_blackbox.py |23 passed,2 mode-only skips (1.766s)|25 passed (1.945s)|
| alias_paho.py |5 passed,1 mode-only skip (0.243s)|6 passed (0.268s)|

Totals48 executed new wire methods and11 Paho methods,3 explicit mode-only skips.
Subcases/focused repeats are not double-counted; timings are not throughput metrics.
Coverage includes sparse65535/config0, binding/ref/rebind/separate maps/clients,
canonical97, exact caps/varints/fallback/no phantom binding, Q2 duplicate/rebind/
restart originals, outgoing Q1/Q2 and PUBREL-only recovery, SessionPresent1/reset/
takeover/crash, NL/RAP/SubIDs/TTL/ACL and legacy311 bridging.

Paho2.1's native receive path does not resolve incoming TopicAlias before caching
QoS2. Its alias suite therefore explicitly uses a small validated receive subclass:
check nonzero/range/unbound, reset on CONNACK and resolve before Paho's QoS2 cache;
actual Paho sockets/ACKs/QoS and outbound behavior are unchanged. Independent raw
Wire tests validate the actual broker encoding/mapping. This is not a claim of
unmodified Paho2.1 receive-alias support; default alias maximum0 clients need none.

All reused suites pass on the same final artifact in both modes:

| Suite | Memory | SQLite |
| --- | --- | --- |
| blackbox.py |14 passed|14 passed|
| qos_blackbox.py |9 passed|9 passed|
| retained_will_blackbox.py |14 passed,6 skips|20 passed|
| auth_acl_blackbox.py |13 passed,7 skips|20 passed|
| mqtt5_blackbox.py |31 passed,11 skips|41 passed,1 skip|
| expiry_will_blackbox.py |28 passed,10 skips|38 passed|
| negotiation_blackbox.py |23 passed,3 skips|26 passed|
| subscription_blackbox.py |20 passed,6 skips|26 passed|
| persistence_blackbox.py |not applicable|14 passed|
| auth_acl_paho.py |2 passed,1 skip|3 passed|
| mqtt5_paho.py |4 passed,1 skip|5 passed|
| expiry_will_paho.py |5 passed,1 skip|6 passed|
| negotiation_paho.py |5 passed,1 skip|6 passed|
| subscription_paho.py |5 passed,1 skip|6 passed|
| paho_interop.py |3x3 QoS matrix passed|3x3 QoS matrix passed|
| retained_will_paho.py |passed|passed|
| persistence_paho.py |not applicable|offline QoS1/2 and SIGKILL passed|

Reused wire suites total360 actual passing executions; adding48 new methods gives
408 wire executions. Both-mode runners completed successfully with explicit mode-
only skips. Default peer alias maximum0 leaves old plain-wire client behavior
unchanged. No post-verification production changes.

### Remaining scope

Shared Subscriptions and enhanced AUTH remain unsupported. Broader conformance,
client/fuzz/fault/clock/crash campaigns, auth CPU/rate isolation, TLS/WebSocket,
metrics/admin/backup and measured CPU/memory/latency/throughput baselines remain
in tasks.md. Whole-registry persistence/first-write transactions, alias map scans/
encoding and memory budgets need measurements before optimization. Existing SQL/TCP
start-intent and latest-wire-clock boundaries are unchanged. Process SIGKILL is not
physical power-loss certification. No full MQTT5 conformance or production claim;
cluster remains deferred.


## Phase2j — Shared Subscriptions and Enhanced AUTH

### Contract, isolation and implementation

Frozen phase2j contract3baa63c, literal-pattern ACL clarificationcfc0cfe and QoS0
incarnation clarificationf459379 preceded implementation. Auth failure packet-context
clarificationb6b8a50 preceded the independent wire-regression fix. Separate Git
worktrees/branches owned Shared/Router, Auth, Transport and Quality; coordinator
owns CLI/docs/integration. Existing protocol properties/Reason Codes, Clock, SQLite
container1, session/outbox transactions, alias writer and policy verifier are reused.

| Owner | Source commit | Result |
| --- | --- | --- |
| Router |ea0e0fe,d621b02|Shared grammar, literal underlying Topic matcher/index|
| Auth |4a97b78|Bounded enhanced provider API, real PLAIN through Argon2/ACL|
| Session |4e74209,3b555c8|Schema7, shared scheduling/recipient ownership, lease epochs|
| Transport |d6bc10b,99f4ea5|Initial AUTH/reauth lifecycle, contextual terminal reasons, network leases|
| Coordinator |aa206be|Explicit CLI PLAIN configuration after policy load|
| Quality |b17ac73|Independent shared/AUTH/Paho tests, strict fixtures and CI|

Each full shared filter defines a separate group; ordinary delivery remains a
separate copy. Selection and cursor changes participate in the existing outbox
transaction. Tests cover aggregate capacity across copies, online preference,
offline QoS1/2, peer-size skipping, selected IDs/RAP, no retained replay, V311
creation denial and resumed memberships, wildcard/$ isolation and literal nested
$share namespaces. Last-member departure prunes unstarted work; runtime incarnation
tokens prevent a previously polled QoS0 copy from surviving same-key group recreation.

Selected QoS1/2 messages belong to their recipient session. QoS2 never reassigns.
QoS1 deliberately also waits for that recipient and does not redistribute after its
session ends: an explicit SHOULD exception, not a claim of seamless group redelivery.
Group cursors reset after restart. Schema7 requires nullable group markers on queued/
outgoing records, validates before pruning, and explicitly migrates schemas1..6.

PLAIN is opt-in via --enhanced-auth-plain, initialized after the existing policy.
Direct/challenged initial authentication and reauthentication share bounded provider
traits. No initial session/Will/takeover mutation precedes authentication, exact
final CONNACK preflight and durable commit. Reauthentication retains the same owner
and existing aliases/QoS while ordinary traffic continues. Absolute deadlines and
8-call limits apply independently of AUTH or PING activity. Debug redacts secrets;
consumed credential buffers use Password cleanup. Generic Frame/receive/write
buffers are not completely zeroized. PLAIN provides no encryption, SCRAM or SASLprep.
Without a configured credential policy, existing anonymous behavior is preserved.

### Failure-first and independent evidence

The previous Phase2i executable fails the new real-wire shared capability check
(SharedAvailable0 versus1) and does not recognize the opt-in AUTH CLI flag. Existing
Session QoS0 reincarnation test was captured red before the epoch fix and green
with the implementation. Independent review reran14 Shared and6 AUTH state tests
and checked strict migrations, per-ID ownership, group pruning, exact packet caps,
provider deadline checks, generation cleanup and registration after durable commit.

Independent raw AUTH testing found reauth bad credentials used86, which is legal in
CONNACK but illegal in DISCONNECT. This prevented terminal encoding and caused EOF.
The frozen correction uses DISCONNECT87 and distinguishes enhanced AUTH deadline87
from established Keep Alive timeout8D; regression tests precede owner fixes.

A separate coordinator probe used the actual previous executable to create schema6
SQLite state, then SIGKILL and opened it with the new binary. Real owner identity,
full subscription options/ID42, outgoing QoS2 packet ID/properties/DUP replay,
queued QoS1, global retained metadata and Will records survived schema7 migration.
The resumed MQTT5 client completed PUBREC/PUBREL/PUBCOMP and drained the next QoS1.
This is actual old-artifact recovery, in addition to synthetic strict DTO fixtures.


### Final artifact and verification

Darwin27 arm64, Rust/Cargo1.98.1 stable, edition2024, Python3.14.7, Paho MQTT2.1.0.
Final debug executable SHA256:
64f8dbd5b1b31c8fabff24d047407622facdbe87a1bfccd978c7ea48cb97fa31.
The artifact includes CLIaa206be and contextual AUTH fix99f4ea5. No performance
metrics are inferred from test durations.

Workspace build/test333 passes: Auth20, Protocol52, Router21, Session144, Storage16,
Transport72, CLI8. Fmt check, strict all-target Clippy and strict Rustdoc pass.
Independent TCP tests need local-listener permission; the initial sandboxed test
attempt was blocked by socket permission, and the actual permitted full run passed.

| New independent suite | Memory | SQLite |
| --- | --- | --- |
| shared_blackbox.py |17 passed,5 mode-only skips (5.958s)|22 passed (6.838s)|
| enhanced_auth_blackbox.py |23 passed,2 mode-only skips (1.825s)|25 passed (1.946s)|
| shared_paho.py |4 passed,1 mode-only skip (1.375s)|5 passed (1.692s)|

Totals87 new wire executions and9 unmodified-Paho executions;8 explicit mode-only
skips. Definitions/subcases and repeats are not double-counted. AUTH uses a dedicated
actual MQTT5 socket client because installed Paho2.1 lacks enhanced AUTH support.
The shared suite uses unmodified Paho; the existing alias suite retains its explicitly
documented receive-alias adapter. The final artifact independently passes both-mode
AUTHfailure87/Will and absolute-initial-deadline87 regressions.

The final immutable artifact also passes every reused suite in both modes:
17 suite executions in memory and19 with SQLite, including persistence-only suites.
These cover the existing base/QoS/retained-Will/auth-ACL/MQTT5/expiry-negotiation/
subscription/alias wire and Paho suites. Reused wire executions408 (memory175,
SQLite233) plus87 new executions total495 passing wire executions. Existing Paho
QoS matrices, metadata/expiry/recovery and persistence scripts also pass. Repeats
from the earlier candidate are excluded. Python compilation, thin-lib audit and
Git diff checks pass. CI is updated; no hosted CI run is claimed.

### Remaining scope

Further SASL mechanisms/credential normalization, auth CPU/rate isolation and TLS/
WebSocket remain open. QoS1 group-owned redistribution, broader conformance, client/
fuzz/fault/clock/filesystem campaigns, backup/admin/metrics and measured CPU/memory/
latency/throughput baselines are in tasks.md. Full snapshots and shared group view/
trie construction remain bounded but need measurement before optimization. Process
SIGKILL is not physical power-loss certification. No full MQTT5 compatibility,
production or cluster claim is made.

## Phase2k — normative clause matrix and confirmed corrections

### Source and engineering approach

Inventory pins the official OASIS MQTT5 OS HTML (2019-03-07), windows-1252, SHA256
16f24e2b021d86b3bbb2b39c8b5ee134b3d39f69320ba61d3c07a38af74e696c.
There are252 distinct literal numbered IDs, including250 literal body IDs plus two
appendix-only index spellings. The body reason-code paragraph has a damaged
3.2.2-8 label; body4.2-1 and appendix4.2.0-1 are aliases. These source anomalies are
explicitly recorded, never fabricated as independent passing requirements.

The catalogue also fingerprints102 unnumbered paragraph candidates selected by
normative keywords or error-constraint terminology. This is a reproducible heuristic,
not an exhaustive sentence/property-table extraction. The matrix preserves scope,
client-only/conditional applicability, policy exceptions, test gaps and limitations.
No feature-level claim automatically produces verified rows or a compliance percentage.

Isolated Protocol, Session and Transport worktrees audited actual code and tests;
coordinator froze K1/K2/K3 before implementation. Quality wrote independent wire
cases and a reference/inventory/staleness gate in its own worktree; independent
read-only reviewers checked normative context and submitted further evidence.
Source verification runs, exact symbols/cases and source commits are preserved in
`docs/conformance/reviews/`; source import and matrix merging are deterministic.
The checker validates references, not the truth of normative interpretation.

### Confirmed defects and failure-first corrections

| Finding | Frozen correction | Source commit |
| --- | --- | --- |
| Initial enhanced AUTH emitted forbidden pre-acceptance DISCONNECT |K1/b251263; failed CONNACK or close, established reauth unchanged|689624b|
| Multi-filter UNSUBSCRIBE reasons read stale pre-removal state |K2/23dd4f3; sequential ordinary/shared removals and atomic malformed rejection|4ac44bf|
| Public AUTH codec accepted missing AuthenticationMethod |K3/d75dd51; strict encode/decode82, Transport still checks negotiated equality|81926bb|

Logs captured the old preauth headerE0 rather than20, duplicate unsubscribe reasons
[0,0,17] rather than[0,17,17], and accepted shorthand AUTH before fixes. These are
actual counterexamples, not inferred gaps from feature names. The public codec gap
was already defended by Transport's connection-state checks; its fix strengthens
the public codec contract rather than claiming an additional network exploit.
Session followup95d9107 closes new evidence gaps with no production behavior change.
Qualitye00d3c5 integrates the independent real-client suite and offline matrix gate.
Existing schema7, outbox/SQLite transactions, clocks, aliases and thin lib.rs remain.

### Final implementation verification

Darwin27 arm64, Rust/Cargo1.98.1 stable, edition2024, Python3.14.7, Paho2.1.0.
Immutable debug binary SHA256:
73693ad51e5b2258778676e3e2b4682ff8f20e2816654ba15f19752025bd175d.

Workspace Rust tests348 pass: Auth20, Protocol57, Router21, Session151, Storage16,
Transport75, CLI8. Fmt, strict all-target Clippy, strict Rustdoc pass. Independent
review reruns Protocol57/Session151/Router21/Transport75 and alias wire23 pass with
2 explicit mode-only skips. Counts exclude repeated runs and subcases.

Independent conformance_blackbox.py18 defined cases: memory17 pass,1 mode-only skip
(1.976s); SQLite18 pass (2.172s). Existing495 actual wire executions plus35 new
executions total530; all reused Paho both-mode suites also pass on the same artifact.
The full runner executes21 suites in memory and23 with SQLite; the final two added
conformance methods were independently rerun in both modes, with no production
change. Dedicated raw clients check AUTH; shared Paho stays unmodified and the old
alias suite's explicit receive adapter is unchanged. Test durations are not latency
or throughput measurements. CI is updated; hosted CI execution is not claimed.

Actual old-binary regressions, new terminal packet/size boundaries, no preauth
Will/takeover, started QoS completion, ordered ACKs and SQLite restart pass.


### Final matrix disposition and limits

All354 inventoried entries now have first-review dispositions, with954 valid code/
test references checked offline. Literal numbered entries252:198 verified within
recorded scenarios,20 test-gap,34 not-applicable/source aliases. Supplementary
candidates102:23 verified,41 test-gap,36 not-applicable/editorial/client-only and
2 SHOULD exceptions. Total221 verified,61 test-gap,70 not-applicable,2 policy
exceptions; no inventoried row remains unreviewed. These heterogeneous entries must
not be used as a conformance percentage. No full MQTT5 conformance is established.

The final unnumbered read-only audit screened91 previously pending candidates and
reran Protocol57/Session151/Auth20/Router21/Transport75 plus five focused existing
wire methods; all pass. Those repeats are excluded from530 wire executions. Many
singleton properties use correct shared seen-ID guards but still lack exact
property-by-packet counterexamples. PFI1 origin/forwarding semantics, diagnostic
text independence and directional Reason Code/property table combinations need
further audit. Numeric tables and unmarked sentences are not exhaustively certified
by the heuristic inventory. A test-gap is not automatically a confirmed code defect.

The source importer reproduces the digest/ID/candidate inventory from the original
local official HTML; matrix generation and offline integrity checks pass. Python
compilation, thin-lib audit and git diff checks pass. Build output matches the
immutable client-tested artifact exactly. No post-verification production changes.

Next acceptance follows specific rows in docs/tasks.md: packet IDs/association,
refusal contexts, complete malformed-packet termination, RAP/order/Unicode,
property/table/direction negatives, and deliberate shared-QoS1/secure-transport
SHOULD decisions. TLS/SCRAM/performance/operations remain separate engineering
work; adding them does not replace normative acceptance. Clustering stays deferred.

## Phase2l — packet lifecycle and exact property acceptance

Frozen scopec1dd789 preceded isolated Protocol/Session/Quality writers. No new
production mismatch was confirmed; implementation behavior, public API and schema7
are unchanged. Tests add evidence rather than new protocol features.

| Owner | Source commit | Actual evidence |
| --- | --- | --- |
| Protocol |9d161cd,871a532|9 tests;42 literal singleton/property packet cells, positive exact bytes, duplicate82, values/truncation/context and repeatable-property order|
| Session |90bba16|7 tests;identifier lifecycle/recovery/response association and PUBREC/PUBREL/PUBCOMP/DISCONNECT ReasonString independence|
| Quality |9729e98|14 actual TCP methods;75 invalid fixedflag subcases, ID directions/reconnect, takeover8E, Unicode/space equality and refusal contexts|

Independent Session review reran9 property tests and checked required companion
fields. Its AUTH fixture-isolation finding was corrected: SessionExpiry wrong-context
input now also contains a valid method so only one variable is invalid. No production
behavior changed. Client allocator clause2.2.1-3 is explicitly outside broker-only
acceptance; server allocation and acknowledgment clauses have separate evidence.
Optional refusal and uncertain direction semantics were not manufactured into bugs.

### Matrix reconciliation

Exact accepted rows close44 evidence gaps and reclassify one client-only obligation.
The354-entry inventory now has265 verified-within-scope,16 test-gap,71 not-applicable
and2 SHOULD policy exceptions. Numbered252:209 verified,8 gap,35 not-applicable.
Supplementary102:56 verified,8 gap,36 not-applicable,2 exceptions. All1275 source/test
references resolve; no feature/chapter-level pass inference or compliance percentage.
Broad property/table/direction and payload policies remain open even though individual
singleton fixtures pass. Remaining exact tasks are in tasks.md; full conformance
and production readiness remain unclaimed.

### Verification environment and final artifact

Darwin27 arm64, Rust/Cargo1.98.1 stable, edition2024, Python3.14.7, Paho2.1.0.
Final debug binary SHA256:
8ad4dfa524bcaa3edc90a6625730edbb1523b7049c7cbd524a59d5f52056604a.
The digest differs from Phase2k after test-module source-position changes despite
no production-logic change. The final rebuilt artifact was retained separately;
earlier-candidate regression results are not mixed into its reported acceptance.

Rust364 pass: Auth20, Protocol66, Router21, Session158, Storage16, Transport75,
CLI8. Fmt, strict all-target Clippy, strict Rustdoc and workspace build pass after
the final fixture correction. Final build bytes match the immutable tested binary.
Matrix regeneration/reference guard, source-import digest check, Python compilation,
thin-lib audit and diff checks are part of closing verification. Test durations and
subcase loops are not reported as measured performance or extra test methods.

Final immutable-artifact full regression passes557 actual wire executions:
memory245/SQLite312,22/24 suite executions; all Paho suites exit successfully.
New14-method clause suite: memory13 pass/1 mode-only skip (2.013s),SQLite14 pass
(2.137s). Existing530 plus27 new wire executions are counted once;75 invalid-flag
subcases and repeated candidate runs do not inflate the total. CI includes the new
suite in both modes; hosted CI execution is not claimed. No post-verification Rust
changes. No new implementation bug found; remaining16 evidence/interpretation gaps
are explicit and neither SHOULD exception is waived as a MUST.

## Phase2m — remaining interpretations and property-position evidence

Scopeb0d7b3a preceded isolated Protocol, Session, Transport and Quality worktrees.
No production defect or behavior/API/schema change was confirmed. In particular,
wrong protocol names already close silently; only unsupported versions enter the
legacy refusal path. The previous conflated observation was corrected after exact
code and actual wire inspection. MQTT311 unsupported-level handling was checked
against its official3.1.2-2, preventing a speculative change from regressing it.
MQTT5's pre-negotiation unknown-version interpretation remains explicit.

| Owner | Source commit | Evidence |
| --- | --- | --- |
| Protocol |c042ef1|5 tests;independent Table2-4 oracle, UTF8 receiver policy|
| Session |01ac387|7 tests;order/recovery/RAP and PFI policy|
| Transport |d310a7b|5 actual/constructor tests;refusal/ID policy/CONNACK legality/AUTH diagnostics|
| Quality |31c3ca7|13 independent real-wire methods and CI integration|

The property-position oracle covers27 IDs across Will plus15 packet columns:
432 cells,64 allowed and368 prohibited; exact public bytes/roundtrip, typed truncation
and104 unassigned VBI identifiers supplement the table. Optional64 control-code
acceptance/66 noncharacter rejections are distinguished from mandatory invalid UTF8
and NUL rules. No endpoint-direction or entire value-space certification follows
from packet-position tests alone.

New ordered-flow evidence includes PacketID65535→1, reconnect/restore, ReceiveMax1,
interleaved QoS1/2 and PUBREL-only resume. Current mixed-RAP collapse and PFI
no-validation policies are now tested in ordinary/shared/retained/Will cases, but
normative interpretation remains unresolved. Policy cases were not marked as MUST
proof. ClientID grammar refusal is unused under the current valid-string acceptance
policy; malformed strings, resource and ACL failures have separate contexts.

Independent read-only review reran the five Protocol table tests and the numeric
CONNACK test. It checked literal oracles and required companion fields. The latter
proves all256 numeric inputs cross the real constructor/encode/decode boundary and
illegal values cannot be emitted by the V5 encoder. It does not claim that the
constructor sanitizes arbitrary input or that all branch-to-reason mappings were
exhausted. Real wire cases independently check86/87/8C/95 and close behavior.

### Matrix and remaining scope

Eight evidence gaps gain scoped verification; one becomes conditionally
not-applicable. The354 entries now comprise273 verified,7 test-gap,72 not-applicable
and2 SHOULD exceptions. Numbered252:213 verified/3 gap/36 not-applicable.
Supplementary102:60 verified/4 gap/36 not-applicable/2 exceptions. All1472 source/
test references resolve. There is no compliance percentage or full compatibility
claim; heuristic inventory and finite tests do not exhaust every normative input.
Remaining rows are unknown-version context, mixed RAP2, PFI2, broad application
matching and overall conformance. Exact next tasks are in tasks.md.

### Final actual verification

2026-10-29, Darwin27 arm64, Rust/Cargo1.98.1 stable, edition2024,
Python3.14.7/Paho2.1.0. Immutable debug executable SHA256:
b078a8d3f80b4598c52bdda87adbabbf26f757549a81b938169cae1030819a34.

Rust381 pass: Auth20,Protocol71,Router21,Session165,Storage16,Transport80,CLI8.
Fmt, strict all-target Clippy, strict Rustdoc and workspace build pass. Same final
artifact582 wire executions pass (memory257/SQLite325),23/25 suite executions;
all Paho scripts/methods exit0. New13-method suite:memory12 pass/1 explicit mode-only
skip (0.782s),SQLite13 pass (0.938s). Existing557 plus25 new executions are counted
once. Table loops/reviews/repeats are not inflated into extra test counts or
performance figures. The executable matches the immutable final-client artifact.

Source importer reproduces the pinned digest/inventory; matrix guard/regeneration,
Python compilation, thin-lib audit and diff checks pass. CI includes new both-mode
suite; hosted CI run is not claimed. Production architecture, schema7, outbox and
existing SHOULD policy exceptions remain; cluster work stays deferred.

## Phase2n — resolved RAP/PFI/version boundaries and implemented behavior

2026-10-29. Contractdb3f375 froze role/profile interpretations and public interfaces
before production changes. Real specialists wrote Session, Protocol and Transport
in separate Git worktrees/branches; released PFI slot enabled independent Quality.
Coordinator owns CLI/docs/matrix/integration. Independent PFI reviewer then audited
N1/N3 and reran Session180 and profile5 actual tests; no blocking defect was found.
Its pre-existing-queue and mirrored malformed-profile suggestions became two new
actual wire cases, both passing in memory and SQLite.

| Owner | Source commit | Result |
| --- | --- | --- |
| Session |7149a63|N1 mixed-RAP per-subscription copies;15 new tests|
| Protocol |3ec2245|PFI producer/forwarder boundary;3 exact-byte tests|
| Transport |e6235ec|ProtocolMode Both/V311/V5 and pre-worker refusal;5 actual TCP tests|
| Coordinator |c3764cf|--protocol both/3.1.1/5;CLI acceptance test|
| Quality |8d53584|20 wire methods, actual upgrade, native Paho and CI updates|

### Decisions and material behavior

Mixed ordinary RAP could not express two requested RETAIN choices in its previous
single copy. Failure-first tests captured old [(R1,IDs11+22)] versus the frozen
[(R0,ID11),(R1,ID22)] policy. N1 deliberately chooses the multiple-copy path allowed
by3.3.4-2, preserving each entry's grant/flag/identifier. This is a conservative
resolution satisfying both RAP branches, not a claim OASIS uniquely mandates this
algorithm. It changes NEW live RETAIN1 fanout to V5 with mixed eligible RAP values;
other collapsed cases, shared selection and retained replay keep their behavior.
Every ordinary/shared copy counts in atomic queue admission, with existing caps,
expiry, ReceiveMaximum and QoS2 idempotence. Source red log is retained in the local
verification artifacts. Old captured schema7 outboxes are not rewritten.

PFI's producer promise is separate from optional receiver validation. Client supplies
PUBLISH and Will payloads;3.3.2-4 forwards PFI unaltered, Will BinaryData is described
in3.1.3.4, and5.4.9 expressly anticipates Server not validating while Client does.
No validation mandate was invented from producer text, and malformed client data
was not declared valid UTF8. The two producer-only candidate rows become not-
applicable to this broker's origin role, with positive forwarding/framing evidence.
Broad U4.7.3 describes the implemented routing model and optional dynamic/ACL choices;
bounded independent evidence is appropriate, not an impossible all-input proof.

Explicit protocol profiles resolve the deployable unknown-version boundary without
regressing default dual-protocol behavior. V5 profile only accepts5 and rejects
other levels with minimalV5CONNACK84; V311 only accepts4 and replieslegacy1 otherwise.
Both remains default and preserves legacy fallback. Mode gate precedes auth,
assignment, session/Will changes and takeover; peer/local caps and malformed-prefix
handling are tested. Unknown payload grammar is never guessed. This verifies
3.1.2-2 for explicit V5 profile; Both's compatibility boundary stays documented.
Public BrokerConfig gains runtime protocol_mode; schema7/container1 stay unchanged.

### Actual verification and immutable artifact

Darwin27 arm64, Rust/Cargo1.98.1 stable, Rust2024, Python3.14.7, Paho2.1.0.
Final debug executable SHA256:
52932c01179780b9dcdc4a82595b122ddb820eb24549c5807180a42d3ac4598a.

Rust405 passed:Auth20,Protocol74,Router21,Session180,Storage16,Transport85,CLI9.
Workspace build,Fmt,strict all-target Clippy and strict Rustdoc pass. Independent
N1/N3 review found no blocking issue. Same immutable executable passes619 actual
wire executions(memory274/SQLite345),24/26 suite executions, all reused Paho cases.
New20-method suite:memory17 pass/3 SQL-only skips(4.934s),SQLite20 pass(5.557s).
Existing582 plus37 new executions are counted once; subcases and repeats are not
inflated or presented as performance results. The mixed native Paho test now checks
each separate QoS/RAP/ID. Raw test dispatch handles legal interleaved QoS2 PUBLISH
and PUBREL; that was a client fixture assumption, not a broker defect.

Actual previous executable seeded collapsed outgoing QoS2 and queued QoS1 with
IDs[11,22]/RETAIN1. SIGKILL/new startup replays the original packet ID/DUP/captured
metadata unchanged, then new live publishes split by subscription. Both-mode tests
also verify existing queue occupancy+ordinary/shared+second-recipient rollback,
profile state isolation, strictV5 malformedlevel4 refusal, aliases, exact20-byte caps,
expiry/NoLocal/ACL and profile-switch V5→311→V5 recovery. This is process-crash
verification, not physical power-loss certification.

CI includes the new suite and amended old mixed-RAP assertions in both modes.
Actual old-artifact upgrade needs /tmp/rmqtt-p2n-baseline and passed locally in SQL;
fresh CI without that executable explicitly skips that one SQL method. All other
new applicable cases are mandatory. Hosted CI was not run/claimed.

### Matrix result and remaining gate

354 entries reviewed,1690 valid source/test links. Current277 verified,1 test-gap,
74 not-applicable,2 SHOULD exceptions. Numbered252:216 verified/36 not-applicable;
supplementary102:61 verified/1 gate/38 not-applicable/2 exceptions. Six specific
boundary rows are resolved; the one remaining U7 whole-server acceptance gate is
not automatically satisfied by the other inventory rows. An independent completeness
assessment of applicable unmarked tables/directions/cross-layer MUSTs and broader
client/corpus/fault evidence is still required by project acceptance. Those evidence
campaigns are not new MQTT MUSTs. Explicit V5 profile scope and both existing SHOULD
exceptions remain visible. No full MQTT5/production claim or cluster work follows.


## Phase2o — whole-server scoped acceptance (2026-10-29)

### Frozen criteria, isolated implementation and review

Contract phase2o decomposed U7 into G1 source completeness, G2 endpoint direction,
G3 cross-layer transitions, G4 actual fault recovery, G5 real Fuzz and G6 integration.
Each writer used an independent worktree/branch. Coordinator owns docs/integration.
Protocol source commits ded821f/427cac0 integrated as c95c5a1/d0852aa; Transport
66983cf/485677c as5a1d3d9/6003ebb; Quality20a2238/0e62821/ca6eef3 as
75e11c3/9b01beb/aa8d594. Contract4cd40fd preceded implementation. Source review
ba4c5d2 reconciles independent ledgers. Dependency copies were not reintegrated.

No confirmed new production MUST defect was found. Test-only modules exercise all
256 CONNECT flag bytes and reason-byte tables, exact ACK Reason String duplication
and repeatable User Property ordering, server-only packet types in four receive
states, requested diagnostic isolation, legal server0x00 PUBACK/PUBREC output and
stale-generation QoS2 leases/ReceiveMaximum. PUBACK/PUBREC0x10 is an optional server
sender code; no false receiver-rejection rule was imposed on external client data.
Existing runtime APIs, session/outbox/schema7/container1 are unchanged. lib.rs stays thin.

Official DOCX SHA256 a567582f279f53b300956282b760db95f66e5096d3ca8412321cf1e8403ac58b
supplements the pinned HTML. Reproducible extraction finds285 keyword/error
paragraphs+58 body tables=343 source entries, all explicitly reviewed,1436 code/test
links. Five table dependencies were reconciled against27 endpoint property roles.
The semantic review includes constraints in paragraphs that also contain numbered
MUSTs; source indexing alone is not proof of conformance. Offline CI refuses a missing
decision/review, and six isolated-copy negative tests verify gate anti-bypass behavior.

### Actual final verification

Darwin27 arm64, Rust/Cargo1.98.1 stable, Rust2024, Python3.14.7, Paho2.1.0.
Final immutable debug executable SHA256:
e98f5c599a804b08e04c6d08f590b4eb304cf90adc292f66babbe4edf1ddc23d.

Final integrated Rust413 passed:Auth20,Protocol78,Router21,Session180,Storage16,
Transport89,CLI9. Workspace Fmt,strict all-target Clippy,strict Rustdoc pass. Both
standalone Fuzz workspaces pass format; stable harness3 tests and strict Clippy pass.
An independent final format check caught a codec wrapper formatting omission; its
format-only fix and standalone-workspace CI check are integrated in aa8d594.

The same executable passed629 unique raw-wire unittest executions(memory275/
SQLite354). Full regression first had628, then the new SQLite busy-error method
was added and the entire fault suite rerun against the unchanged artifact; union629
excludes repeats. All Paho2.1 suites passed:67 separate unittest methods plus script
assertions. Existing previous-artifact upgrade passed locally where its saved binary
exists; fresh CI explicitly skips that one optional-artifact case. Hosted CI not run.

The fault suite passed9 SQL cases(5.361s); memory1 passed/8 SQL-only skips. It uses
real BEGIN IMMEDIATE contention, actual SQLite busy_timeout error returns, unchanged
committed snapshots and socket observation/positive ACK barriers, SIGKILL and restart.
It covers precommit SUBACK/PUBREC/PUBCOMP/PUBREL rejection/replay, committed PUBREL-only
resume and acknowledged queued/retained/outbox/Packet ID preservation. Independent
review reran the original8 SQL cases successfully. No fallback-to-memory ACK occurred.
These are process recovery results, not hardware power-loss or exact-instruction failpoints.

Immediate ID reuse with server ReceiveMaximum1 passed4000 QoS1+2000 QoS2 events
across8 clients,without PING barriers(4.887s); an independent4-client40000 QoS1-cycle
probe also passed. These finite scheduling probes found no reproducible ACK callback
race and are excluded from unittest and throughput counts.

### Reproducible Fuzz evidence and scoped closure

Stable seeded mutation:20000 runs,seed332593189941,1146707 input bytes,no crash.
41 checked-in hand seeds are reconstructible; initial canonical manifest SHA256
2cad8ce831e93378b627fbb1615dea8dc58329f2d1266831c75d3be337f457bd.
Actual cargo-fuzz0.13.2/nightly-2026-10-29 was installed only in temporary tooling
directories; the global default and production toolchain remain stable. Rustc nightly
commit date2026-10-28 differs from the pinned distribution date2026-10-29.

Two independent fresh corpus directories containing only the41 seeds ran concurrently.
Codec:2128810 executions/31s,cov1958,features4441,RSS224MB,seed20261029.
Legacy+V5 session:2160/31s,cov5260,features29694,RSS175MB,seed20261030.
Both exit0/no crash; each max_total_time30,max_len4096,timeout2,rss_limit_mb512,
ASAN quarantine64MB. Cold results supersede warm corpus counters for reproducibility.
Scheduling affects guided execution counts; these counters are not benchmarks or
protocol coverage percentages. See fuzz/README.md for runnable commands and oracles.

Codec validates bounds, consumption, deterministic decode and semantic round trips.
Session validates bounded legacy/V5 traces, proposal discard, controlled-time expiry,
QoS/reconnect and snapshot fixed points; a seed test proves actual mixed-RAP QoS2
deliveries start. Pure model targets do not implement network aliases, provider AUTH,
KeepAlive scheduling or arbitrary actor interleavings; separate wire tests cover those.

G1..G6 passed with no unresolved confirmed defect. Matrix354 reviewed rows now:
278 verified,74 not-applicable,2 SHOULD policy exceptions,0 test-gap; numbered252
remain216 verified/36 NA,supplementary102 become62 verified/38 NA/2 exceptions.
The343 source entries are a separate completeness ledger, not extra distinct MUSTs.
This closes the frozen finite project gate, not official certification, universal
MQTT5 correctness or production readiness. Explicit V5 TCP scope, default Both
unknown-version qualification and both SHOULD exceptions remain. Next:longer
independent-client/fuzz/fault campaigns, measured resource/latency baselines and
production hardening; TLS/WebSocket require new scoped audits.

Independent final read-only closure review found no blocker and reconciled343/1436
source evidence and354/1719 matrix evidence. Historical routing notes were clarified
to distinguish their Phase2n open gate from Phase2o scoped closure.


## Phase3 — local performance/reliability baseline (2026-10-29)

Frozen phase3 contract preceded three isolated writers:load codex/p3-load,
transport codex/p3-transport,and quality codex/p3-reliability. Runtime APIs,
schema7/container1 and lib.rs structure remain unchanged. No confirmed production
protocol defect was found. Scope is a local baseline, not full performance hardening.

Source/integration commits:transport9c59707->b702874;quality218612a->4aceb90,
895e4d8->8544246,629a693->6b9e02b;load41242a6->3f44fb6,
bc7360c->b1e871f,f1e6d3c->9b3771b. Contract e2f8562; positive-session profile
a2be508; completion-feedback/overload separation6ebc96e. Every writer used its
assigned worktree/branch and ownership; no shared-root Cargo changes.

Integration passed Rust416(Auth20/Protocol78/Router21/Session180/Storage16/
Transport92/CLI9),workspace Fmt/strict Clippy/strict Rustdoc,driver14 tests,
selected release-wire135 executions with21 explicit skips (7 new Phase3 TCP
executions included). Repeated checks are not double-counted. Real SQL fault9 and
volatile control1 pass. Reused Fuzz:3 oracle tests,20000 stable mutations with seed
332593189941 and1146707 bytes;actual cold guided codec2289643 and session2317
executions,each31s,zero crash. Each uses41 committed seeds,max_total_time30,
max_len4096,timeout2,RSS512MB,ASAN quarantine64MB,seeds20261029/20261030.
Isolated nightly-2026-10-29/cargo-fuzz0.13.2 leaves default/production stable.
Source/matrix343/1436 and354/1719 guards plus6 anti-bypass tests pass. Hosted CI
was configured,not run. Existing protocol matrix remains278 verified/74 NA/2 SHOULD
exceptions; performance measurements are not additional normative MUST rows.

Deterministic pressure traces cover full output queues,uncompleted credits,worker
barriers,termination deadlines,ReceiveMaximum1,global atomic rejection,healthy
controls,takeover/old-generation ACK rejection,DUP recovery and renewed admission.
Real TCP complements them with nonreading peers,capacity release and full-window
SQL SIGKILL recovery. No OS socket-buffer saturation or hard RSS bound is claimed.

Independent review corrected measurement/fixture defects before final runs:EOF is
not a permit-release barrier;startup must replace stale passed output with running;
ps processes must be killed/reaped on cancellation/timeout;ordered streams need
per(topic,publisher) checks;ACK failures need actual response bytes. Four earlier
driver regressions failed before correction. New completion-feedback tests confirm
all expected receipts plus valid PUBACK gate the next publication and propagate
consumer/ACK failures. Server resource/protocol behavior was not changed.

### Actual measurement and negative evidence

All18 completion-gated runs passed:three workloads x memory/SQLite x3 repetitions,
4096 publications each,128 separate warmup,256-byte payloads,SessionExpiry60,
QoS1 ingress/QoS0 grants,at most one not-fully-completed publication per publisher.
128-client concurrency,32-subscriber hot topic and4096 installed filters are covered.
Totals73728 measured publications/ACKs and835584 validated receipts. Artifact SHA256
2682740440920644a6d424fc790eed62c381a9a4999522547343b710dd040eee.
Darwin27 arm64,AppleM4/Mac16,12,10 logical CPUs,16GiB,Rust/Cargo1.98.1 stable,
Python3.14.7. Campaigns ran serially after competing project tests/builds finished.

memory/concurrent:median ingress215.46/s,egress215.46/s,p99 delivery751.69ms,sampled peak RSS47.08MiB.
memory/hot:median ingress17.21/s,egress550.56/s,p99 delivery912.17ms,sampled peak RSS23.94MiB.
memory/many:median ingress9.87/s,egress9.87/s,p99 delivery2164.01ms,sampled peak RSS31.70MiB.
sqlite/concurrent:median ingress217.01/s,egress217.01/s,p99 delivery757.78ms,sampled peak RSS42.33MiB.
sqlite/hot:median ingress14.25/s,egress455.91/s,p99 delivery1245.37ms,sampled peak RSS26.03MiB.
sqlite/many:median ingress9.81/s,egress9.81/s,p99 delivery2220.36ms,sampled peak RSS33.47MiB.

See docs/performance/phase3-2026-10-29.md for all per-run ranges,p50/p95/p99/max,
ACK latency,CPU time,RSS stages,commands and compressed raw samples. These are
closed-loop same-host baselines,not saturation/WAN capacity or a production SLA.
Peak RSS is sampled;CPU window is wider than throughput;thermal/background activity
is not controlled. No hardware power-loss or long-duration-soak claim follows.

ACK-only offered-load attempts remain negative evidence:initial hot run had an
unclassified non-PUBACK reply;its original bytes were not captured. Same-configuration
replay hit600s without reproducing that response. Independent cap1 smoke produced
legal e0/97 quota rejection;it does not explain the initial unknown reply. Both failed
attempts are preserved and excluded from passed throughput. Completion feedback
changes offered load,not runtime speed. ACK-only hot stress has no successful full
throughput figure here and its original response remains an investigation item.

Static review candidates are whole-engine clones,repeated live/persistent JSON,
per-publication Router rebuilds,broadcast-induced empty polls and full-BLOB commits.
Next:profile actual actor counts/CPU/commit bytes first,then freeze optimizations
that preserve authorization,admission,ACK,expiry and recovery. Current low throughput
exposes scalability work;this baseline does not conclude high-performance readiness.

Postmeasurement metadata-only followup: Linux CPU model no longer uses architecture
as its label; two failing fixture subcases were reproduced before the fix. Darwin
sysctl and measured workload/timing logic are unchanged. Source a8a2b53 is integrated
as 3f77761; final14 driver tests pass. Formal examples specify600s.

Independent final accounting review recomputed all18 run counts,throughput/latency/
RSS/CPU tables and archive hashes without rerunning loads;no blocker found. Failed
ACK-only evidence remains separate and explicitly unresolved. Final source guards,
six anti-bypass cases and seven thin-lib audits pass.

## Rust load generator — 2026-10-30

Added `bin/rmqtt-loadgen` on `codex/rust-loadgen`; frozen standalone contract
`docs/contracts/rust-loadgen.md`. Real isolated engine/metrics/quality subagents
implemented paired MQTT3.1.1/5 QoS0/1/2, bounded closed-loop/absolute-rate load,
monotonic in-payload identity/timing, complete/incomplete record accounting, exact
nearest-rank nanosecond percentiles and JSON/optional rawJSONL. No broker modules
or durable schemas changed. `lib.rs` remains declarations/reexports only.

43 tool tests (26unit/13 independent wire/4CLI),whole workspace511passed,fmt and
strict all-target Clippy passed. Six actual local broker dual-version/QoS profiles
had all raw percentile series independently recomputed;three fixed-rate checks
verified explicit unsent handling (one deadline miss was correctly unsuccessful).
Linux amd64 musl release cross-built locally,code revisionf44b657;artifact hash
and logs in `docs/verification/rust-loadgen-20261030/manifest.json`.

Live validation on47.117.6.20 is pending:SSH host keys differ from existing trusted
known_hosts entries;identity confirmation requested. Do not disable strict host
checking or count this remote validation as passed. Tool usage and metric definitions
are in `bin/rmqtt-loadgen/README.md`. Future:connect-only profiles,TLS/auth and
multi-host synchronized-clock support;these are not advertised as implemented.

### Rust load generator new-server gate resolved

User confirmed47.117.6.20 is a new instance;new ED25519 fingerprint pinned with
strict checking,global old host records unchanged. New generator is8cores/16GB,
private172.30.139.132. Linux static release deployed`/opt/rmqtt-loadgen/rmqtt-loadgen`.
Three actual5000connection QoS0/1/2 correctness runs passed(1s warmup/5s offering,
closed-loop window1);all six raw percentile series independently recomputed,delivery
p99 rechecked locally. Counts fully reconcile;brokerPID2100933 unchanged,no restarts,
test connections cleared. Full reports/raw/hash/limitations in
`docs/verification/rust-loadgen-20261030/`. Not a maximum-throughput comparison with
old4core emqtt generator or unlimited offered load.

### Loadgen tail-latency diagnosis — 2026-10-30

Read-only independent audits confirm270/390/463ms pressure p99 in raw samples;
not cutoff/drain artifacts or a few pairs. Same5000connections,unchanged binaries,
fixed intended5000msg/s10s contrast measured send->deliveryp99=0.368/0.454/0.905ms;
including scheduling lag=2.140/2.399/2.780ms. All sent messages fully completed;
4/4/10deadline sends were missed and reports remain unsuccessful,not falsely passed.
Tool in-flight measured work fell from~2450credits to~1–4 despite5000TCPclients.
This establishes strong load sensitivity,not a precise broker/network/client queue
location or a code optimization. No production code/config changes. Evidence and
remaining causal experiments:docs/performance/loadgen-latency-diagnosis-20261030/.

## Phase4c single-node TLS — 2026-10-30

Cluster work deferred by user. Frozen `docs/contracts/phase4c-tls.md` before two
independent implementation/quality worktrees. Added optional server-authenticated
Rustls TLS1.2/1.3 listener, bounded startup identity loading and CLI flags. MQTT
protocol/session/outbox code paths and durable schema are reused. Plain TCP keeps
owned halves and static dispatch. Handshake permits, timeout/shutdown, write flush,
normal TLS close_notify and abnormal EOF/Will semantics are covered.

Workspace521 Rust tests, fmt and strict all-target Clippy pass. Independent stdlib
ssl/OpenSSL blackbox12 methods pass, including all TLS/version/QoS combinations,
strict CA/hostname rejection,40KiB cross-record payload, packet-size rejection,
handshake/permit recovery, clean/abrupt/close_notify Will and dual-version SQLite
SIGKILL recovery. Test CA initially failed Python's strict KeyUsage requirement;
fixture fixed without weakening certificate verification. Runtime/CLI and transport
changes independently reviewed, no blocking findings. Final evidence and plaintext
regression results are recorded in `docs/verification/tls-20261030/`.

Plaintext18 suites × memory/SQLite final36 executions passed (728 unittest method
executions,68 expected skips). One initial RAP queue-capacity assertion failed;
independent old/new20-run comparisons reproduced it on both versions. The test
now pins capacity using an offline persistent recipient, preserving all assertions
and runtime semantics; old/new × both stores ×10 repeats passed40/40, followed by
full RAP suite reruns. Original failure and comparisons are archived, not erased.

No server deployment or TLS throughput test performed. mTLS, WebSocket, certificate
reload, shared dual listeners and rate-limited authentication remain unimplemented.
The prior MQTT conformance ledger stays explicitly scoped to its TCP assessment;
new TLS functional tests are not a full protocol/security certification.

## Phase4d WS/WSS — 2026-10-30

Frozen bounded HTTP/WS framing and lifecycle contract, then implemented transport
and independent raw/Paho quality in separate worktrees/branches. Reused TLS,
incremental MQTT reader, session actors, outbox and durable schema. TCP retains
owned halves. Added strict mqtt subprotocol/path, bounded headers/messages/writes,
binary fragmentation/control handling, and normal WS+TLS closing state.

Verified540 workspace Rust tests,18 raw WS/WSS methods, Paho2.1.0 twelve
WS/WSS×MQTT311/5×QoS profiles (no skips),40 delayed TLS-notify connections,
12 existing TLS methods and36 original plaintext suite executions across both
storage modes. fmt/strict Clippy and source/negative-evidence guards passed.
The final closing patch additionally reran6 affected plaintext MQTT5/QoS/Will
suite executions, both storage modes, rather than relabeling earlier binaries.
Q1 AwaitAck, Q2 AwaitRec/AwaitComp and offline queues resume with matching IDs/DUP
across WS↔WSS process kills. New raw faults, Will and control/KeepAlive tests passed.

Confirmed/fixed pipeline bytes rejected as HTTP junk, fragmented text not rejected
until final fragment, and normal WSS closing reset from unread peer notifications.
Nonminimal frame lengths are rejected as an explicit strict-input policy, not an
invented receiver MUST. Initial test-oracle issues (error-close echo after peer
reset and pre-session timeout closure) were corrected with assertions retained;
strict normal1000 and TLS unwrap were never relaxed. Original failures retained in
`docs/verification/websocket-20261030/` with final results and scope limitations.

Matrix now281 verified/71N/A/2SHOULD exceptions within inventoried scenarios;
three newly applicable WS server rows updated, client-only mqtt offer stillN/A.
No deployment or WS performance claim. mTLS, reload, multiple shared listeners,
Origin ACL, proxy identity and longer soak/Fuzz remain separate work.

## Phase4e authentication admission + mTLS — 2026-10-30

User narrowed scope to authentication rate/handshake protection and mTLS only.
Frozen contract, three isolated writer worktrees and independent review delivered
global fixed token bucket, handshake128/auth-job4 defaults, rate1000/burst1000,
nonblocking cleanup and mandatory Rustls client certificates for TLS/WSS. Credential
rate0 disables only token admission. No certificate-name user mapping or ACL bypass.

Review found a real late-CONNECT-commit cancellation window. Controlled provider,
network deadline and SQLite regressions preceded fixes; bounded tracked handoff
retains reply/slot until identity adoption or same-generation cleanup. Old takeover
is not falsely rolled back; stale cleanup cannot disconnect a newer owner.

Final556 Rust tests,12 new independent methods (24 positive/16 negative mTLS
profiles), existing TLS12/WS18/Paho12 profiles and36 plaintext dual-store suites
passed. fmt, strict Clippy and source/negative metadata guards passed. SQLite late
commit and mTLS TLS↔WSS outbox recovery passed. Evidence and saved red transcripts:
`docs/verification/admission-mtls-20261030/`.

No metrics/health/reload/multi-listener, deployment, long soak or performance
acceptance performed. Running blocking hooks remain non-preemptible; limits bound
their count, not arbitrary custom hook lifetime. Durable schema unchanged.

## Phase4f Dashboard — 2026-10-30

Frozen read-only management API, DTO/error/query/resource contracts before three
independent writer worktrees (`codex/dashboard-telemetry`, `codex/dashboard-react`,
`codex/dashboard-quality`). Integrated transport/session metadata projections,
payload-free retained traversal, process-lifetime MQTT wire counters, bounded HTTP
management and CLI, React real-data UI and independent blackbox/CI checks. The
coordinator owns HTTP/CLI/docs; approved read-only metadata visitors narrowly extend
telemetry ownership into router files. No durable schema or ACK/outbox changes.

Management is opt-in, numeric loopback only, with independent file-loaded bearer
token. Static build assets are startup-loaded with finite file/byte caps; configured
sensitive-file aliases and copied token assets are rejected. API exposes no payload,
credential hash, key/certificate or secret path. Read views are eventually consistent;
restored negotiation is unknown until reconnect rather than fabricated from defaults.

User requested Tailwind CSS + shadcn/ui instead of extensive handcrafted CSS. The
React migration uses official generated controls and Lucide icons, preserves the
light design and real API behavior, and replaces the old2191-line stylesheet with
61 lines of theme/base/native-backdrop CSS. Details deliberately use native dialog
to preserve strict CSP without library style injection; no fake resource metrics.

Verified complete Rust workspace571 tests including doctests, strict Clippy and
fmt; independent live HTTP/MQTT14/14 including dual-version QoS0/1/2, retained,
clean/persistent disconnect, SQLite crash/reconnect/outbox, resource saturation,
redaction and sensitive static-file aliases. Matrix/source guards and6 negative
acceptance-guard tests also pass. Original red evidence and fixture-policy source
comparison are retained; MQTT311 persistent cases use existing SQLite requirement.
See `docs/verification/dashboard-20261030/` for logs and final UI validation.

Remaining: CPU/RSS/latency sampling, persistent event history, control-plane mutations,
user/ACL editing, rule execution, certificate reload, monitoring performance/large
instance acceptance, and server deployment. This is a real read-only console, not
an assertion that all future administration or monitoring functions are complete.

Final UI acceptance:30 React tests plus production build/typecheck pass; browser
verified actual Broker login, live sampling, search/details, empty filters, mobile
navigation/modal and logout. Found and fixed active-topic pagination dereferencing
null while a newly selected page loads; a deferred dual-direction regression and
original browser/test red evidence are retained. Desktop1440/phone390 have no page
overflow and final fixed-bundle browser console is clean. Existing TLS12/WS18/
admission-mTLS12 methods also pass with no skips. CI jobs added; hosted CI and remote
deployment have not been run. Local verification broker has been stopped.

## Embedded Dashboard — 2026-10-30

React dist now embedded in rmqtt at Rust compile time with include_dir. No external
frontend directory or Node runtime needed at deployment. Optional dashboard-dir is
an explicit full-bundle override, retaining resource, symlink and secret containment
checks; invalid override is fatal. Runtime never implicitly reads cwd dist. Build.rs
requires a valid frontend build, checks file/count/byte bounds and tracks additions,
changes/removals; it does not invoke npm/network. scripts/build-rmqtt.sh builds
frontend then release Rust and forwards target/profile flags; CI builds UI first.

Verified572 workspace Rust tests including doctests, strict Clippy and fmt,4 std-only
build-input guard tests,4 copied-release embedded runtime tests and14 existing API/
MQTT acceptance tests. Copied release runs from an empty cwd with PATH excluding Node,
serves linked JS/CSS/favicon with MIME/CSP, preserves auth and actual MQTT state,
ignores cwd dist, and respects explicit override/no fallback. Real incremental probe
confirmed added/deleted assets are re-embedded without cargo clean. Full build wrapper
also tested using --profile dev. Optimized native macOS arm64 executable produced;
no Linux binary, server deployment or hosted CI execution claimed this change.

## Phase5a SQL + Webhook — 2026-10-30

Frozen SQL subset, observer/runtime/management contracts before isolated core,
Webhook, frontend and quality worktrees. Implemented native mqtt-rules, nonblocking
accepted-publication hook, mqtt-webhook runtime, explicit --rules-config startup,
authenticated config/status/test endpoints, and actual Dashboard rule/connector/
action CRUD with SQL preview. No Kafka/PostgreSQL/plugins implemented.

Four bounded SQL shards execute outside network threads; actions have independent
queues, batching, limited retry and per-generation connection pools. Global logical
queued bytes16MiB and HTTP concurrency16 are finite. Config persists atomically,
secrets are redacted, and save/directory-sync uncertainty is explicit/failclosed.
Distinct source/event delivery IDs preserve intentional fanout and receiver dedup.

Verified600 Rust tests including doctests, strict workspace/all-target Clippy/fmt,
50 frontend tests plus TypeScript/production build. Final release72 unique independent
methods pass: rules12, dashboard14, embedded4, TLS12, WS/WSS18, admission/mTLS12.
Initial parallel TLS sendall BrokenPipe is retained; two unchanged full serial TLS
reruns passed. Other red tests preceded fixes for numeric precision, expired observer
inputs, missing delivery IDs, pending UI delete after stop and old HTTP pool lifetime.

Browser created a real connector/action/rule through UI, ran SQL preview, and observed
actual MQTT→HTTP output/counters. Final release restored these configs and continued
real delivery; desktop1440 and phone390/edit-dialog widths fit, console clean. Runtime
fixtures stopped; existing user Broker was not restarted or reconfigured. Evidence:
`docs/verification/rules-webhook-20261030/`, usage `docs/rules-webhook.md`.

Remaining: durable event/action outbox and ACK admission, durable dead-letter/replay,
Kafka/PostgreSQL, aggregation/windows, plugins, finer per-rule metrics/history,
config version administration and high-load TPS/p99 acceptance. Current best-effort
queues may lose work on crash, overflow or configuration replacement; no durable or
exactly-once external delivery/performance guarantee is claimed.


### Rules enabled by default (2026-10-30)

Normal broker startup now enables SQL rules/Webhook with working-directory
`rules.json`; `--rules-config PATH` only overrides the location. Missing files
start with empty rules, existing invalid files still fail closed, and static asset
sensitive-file checks include the resolved default. Export-state remains free of
rule configuration reads/writes. Dashboard copy now describes user operations and
status without internal execution/outbox details.
Validation: default-path regression reproduced before the parser fix; cargo fmt,
workspace Clippy (-D warnings), full workspace tests, and release build pass. A
separate temporary release broker without --rules-config created rules.json and
returned authenticated config/status with running=true; existing services were
untouched. Network tests required local-listener sandbox permission.


### Phase5b — SQLite-only rule configuration (2026-10-30)

Completed with independent storage/runtime/quality worktrees and final read-only
review. The user explicitly removed JSON-file compatibility/import from scope.
Rules remain enabled by default, now using `rmqtt-rules.sqlite`; `--rules-config`
only changes the SQLite path. Old `rules.json` is neither read nor written.
The separate bounded configuration store uses an atomic singleton transaction,
FULL synchronization, dedicated application/schema identity and lifetime ownership
lock; the strict MQTT session database schema is unchanged. All database IO runs on
blocking workers; snapshot publication follows successful commit. Uncertain save
failures stop rule dispatch; restart recovers actual committed data.

Validated: Fmt, workspace Clippy all targets with -D warnings, 616 Rust tests,
release build, 85 independent release blackbox tests: rules/Webhook15, Dashboard14,
persistent MQTT14, WS/WSS18, TLS12, admission/mTLS12. Real SIGKILL and uncommitted
transaction rollback, API acknowledged-config recovery with subsequent HTTP
delivery, external SQLite write-lock failure, cancellation-safe shutdown/reopen,
no legacy JSON reads, secret redaction, foreign/invalid schema and static companion
hardlink rejection are covered. No existing user broker was restarted.

Evidence: `docs/verification/rules-sqlite-20261030/README.md`. Configuration
persistence does not add durable message queues/action outbox. Physical power loss
and fresh performance baselines were not measured; remaining scope is unchanged.


### Phase5c — Unified typed outbounds (2026-10-30)

Completed in independent backend/UI/quality worktrees after freezing
`docs/contracts/phase5c-unified-outbounds.md`. UI now exposes Rules and Outbounds,
with one editor for type, destination, credentials and delivery policy. Rules bind
`outbound_ids` directly. API config version2 contains `outbounds`/`rules`; status
contains typed `outbounds`; Webhook envelopes use `outbound_id`. Unsupported types
and unknown fields are rejected; only Webhook is implemented.

Generic lifecycle/queues/batching/timeouts/retry pacing/counters/SQL admission and
SQLite configuration now live in mqtt-egress. mqtt-egress-core provides the native
async Sink interface, typed retry/permanent result and adapter batch capabilities.
mqtt-webhook only owns Webhook config/validation/redaction, HTTP encoding/client
and status classification. Common workers contain no reqwest, HTTP status or
envelope logic; non-HTTP fake sink tests prove retries, timeout and capabilities.
Registry/config variants are explicit; new native adapters add their variant/factory
and form without editing SQL evaluation or generic scheduling.

Existing strict SQLite container remains unchanged. Old recognized connector/action
blobs are fully validated and transactionally converted to version2, preserving
action IDs as outbound IDs, credentials and rule bindings. Invalid unused
connectors, excessive counts, malformed refs and unknown versions fail without
overwriting. No JSON file compatibility/import was reintroduced. Existing queue
volatility and MQTT ACK/outbox semantics remain unchanged.

Verification: cargo fmt; workspace all-target Clippy -D warnings; 620 Rust tests;
51 frontend tests, strict TS/Vite production build/Prettier; native release build;
88 release blackbox methods (outbound18, Dashboard14, persistent MQTT14, WS/WSS18,
TLS12, admission/mTLS12). Browser CRUD, direct binding, SQL preview and real
MQTT->HTTP confirmed, desktop1440/mobile390 layout and masked headers checked.

Final testing exposed a preexisting parallel test isolation race: configuration
crash workers could temporarily inherit another test's exclusive-file descriptor
between fork and exec. A deterministic failing fixture-gate regression preceded a
test-only shared gate fix, using the same gate as durable crash tests; no production
lock or retry/sleep workaround. Storage36 tests passed five normal-parallel runs,
then the full workspace passed. Initial failure and correction evidence retained.

Evidence: docs/verification/unified-outbounds-20261030/README.md. No actual user
broker restarted, deployment or new performance claim. Native Kafka/PostgreSQL/Redis
adapters and durable message delivery remain future work.


### Dashboard component dropdown fix (2026-10-30)

Native OS select menus were replaced in outbound type and MQTT version filtering
with a reusable existing-Radix UI single-choice menu, Tailwind styled, full-width
where appropriate. Native-dialog-contained portals preserve top-layer visibility;
nonmodal menus do not inject scroll-lock CSS or weaken CSP. A reproduced Escape
regression is fixed by honoring defaultPrevented in Drawer keyboard handling.
54 frontend tests, TS/Vite/format, 620 Rust tests, workspace strict Clippy and native
release build passed. Real browser pointer/keyboard/Escape/outside-click, desktop
and390px mobile widths, protocol filtering, zero CSP/console errors verified.
Evidence: docs/verification/select-component-20261030/README.md.


### Dashboard automatic identity and component consolidation (2026-10-30)

Independent UI primitives and identity/integration worktrees completed and were
reviewed read-only before integration. Rule/outbound create forms no longer ask
for IDs; each new draft gets a WebCrypto UUID with kind prefix, randomUUID plus
getRandomValues fallback, bounded collision checks and safe unavailable-crypto
feedback. IDs survive edits and validation/HTTP retry. Existing SQLite/API identity
and name-independent outbound references remain unchanged. Internal IDs are hidden
from list subtitles, binding summaries, execution status, delete subtitles and
action ARIA labels; protocol Client ID fields are intentionally preserved.

Reusable Radix Switch/Checkbox/Label/Tabs and shared Textarea/Field/Alert replace
raw business-page controls and duplicate error styling. Existing Table/Button/Card/
Badge/SingleSelect and tested strict-CSP Drawer remain in use; no new dependency.
Duplicate outbound names expose destination context in selection.

Validation: 69 frontend tests, strict TS/Vite/Prettier, workspace Fmt/strict Clippy,
620 Rust tests and embedded release build passed. Real isolated Chrome created two
same-name destinations without ID inputs, bound only the second by its visible URL,
verified MQTT->HTTP delivery, renamed without changing identity/references, tested
actual browser randomUUID-unavailable fallback, masked credentials, hidden IDs and
390px mobile shared controls. Zero captured CSP/console errors. Existing jsdom
window.scrollTo not-implemented warning remains test-environment-only.
Evidence: docs/verification/auto-id-components-20261030/README.md. Existing user
Broker was not restarted; only owned temporary QA services/browser were used.


### Webhook basic and advanced configuration (2026-11-02)

Dashboard basics now expose only name, address and authentication/headers. Tuning
parameters, enabled/type controls live in a default-closed shared Radix Collapsible.
Queue capacity is not editable; new resources use existing system default256, and
editing preserves existing bounded capacity/non-default settings. Outbound list
rows no longer expose tuning summaries. No API/schema/backend limit change.

Invalid hidden tuning is rejected before PUT, reopened and focused/scrolled into
view. Closing the section never resets values. Credential masking, stable IDs,
rule enable control and stopped guards remain intact.

74 frontend tests, TS/Vite/Prettier, Fmt, workspace strict Clippy, 620 Rust tests and
embedded native release build passed. Real Chrome confirmed collapsed defaults,
keyboard interaction, preserved legacy custom policy/capacity777/disabled state,
invalid hidden values and correction, actual MQTT->HTTP, 390px mobile and zero
CSP/console errors. Evidence: docs/verification/webhook-advanced-20261102/README.md.
Existing user Broker untouched; temporary QA services/browser cleaned up.


### New Webhook defaults: single message (2026-11-02)

New outbounds now default to batch_size1/batch_wait_ms0; existing100/50 or custom
policies are preserved. Batch users opt in through advanced settings. Only the
frontend authoring factory defaults change; API/SQLite/envelope and other defaults
remain intact. Explicit model/closed-form literals failed before the factory fix.
76 frontend tests, TS/Vite/format, Fmt, strict workspace Clippy,620 Rust tests and
embedded Release build pass. Real UI saved1/0 without opening advanced; a real
MQTT5 burst of10 messages produced10 POSTs, each containing exactly one event.
Evidence: docs/verification/webhook-single-default-20261102/README.md. No new
latency/TPS guarantee or production deployment; temporary QA services cleaned up.


### Bounded concurrent outbound requests (2026-11-02)

Frozen phase5d contract; backend, UI and independent quality work integrated from
separate worktrees. Each outbound defaults to8 concurrent requests, advanced1..16;
global local-attempt limit16 spans all outbounds/generations. Shared reqwest pools
match per-outbound concurrency. Single-message defaults1/0 remain. Existing missing
concurrency defaults8 without changing SQLite schema or other explicit policies.
Set1 for ordered sending including retries; higher values may reorder completion.

Structured bounded jobs preserve retries/delivery IDs, cancellation, byte budgets
and exact terminal accounting. Backoff releases global permits. Storage failure,
replacement and shutdown clean up children. Dashboard separates active requests
from pending messages; queue capacity stays system-managed. MQTT ACK/session/outbox
and best-effort volatile outbound delivery remain unchanged.

Fmt, strict workspace Clippy,627 Rust tests,81 frontend tests,TS/Vite/format and
embedded Release build pass.98 Release black-box tests pass. Actual HTTP peaks:
default8,global16,configured3;16 requests reused8 TCP connections on204 responses.
Real Chrome showed3 concurrently held requests in receiver/API/UI, then drained
all local budgets;390px layout and CSP/console checks pass. No production TPS/p99
claim. Evidence: docs/verification/outbound-concurrency-20261102/README.md.
Remaining: dedicated end-to-end throughput/p99 tuning and durable outbound delivery
are separate future work. Existing user service untouched; own QA processes closed.


### Client-ordered concurrent outbounds (2026-11-02)

Frozen phase5e contract at8a2dfcd; isolated backend, independent HTTP quality and
Dashboard worktrees implemented it. Exact(outbound,sourceClientID) lanes preserve
admitted event order across topics/rules/reconnects in a generation. Homogeneous
batches execute one at a time perclient; differentclients overlap. Backoff releases
both outbound and global attempt permits, including concurrency1. Terminalfailure
countsfailed and continues the nextclientbatch. No new UI setting/API/schema or
MQTT session/ACK/outbox change. Dashboard explains clientordering and retryisolation.

Aggregate resident-event admission bounds channel+lanes+jobs. Completed JoinSet
results retain batch reservations until reaped, fixing a review-discovered metadata
retention gap; eager reaping and idle-lane removal prevent clientchurn accumulation.
5000immediate-success clients verified lane/job peaks<=cap9; saturation,cancel and
panicking-child tests verify counters/slots/bytescleanup. ClientID metadata counts
against globalbytebudget. Exactkeys avoid unrelatedclient hashbucket serialization.

Acceptance evidence: docs/verification/client-ordered-outbounds-20261102/README.md.
Queued delivery remains volatile/best effort; durability and productionTPS/p99
benchmarking remain separate work. No production service restarted or main merge.

Final validation: Fmt,strict workspaceClippy,631Rusttests,81frontendtests,TS/Vite/
Prettier and embeddedRelease pass.107Releaseblackboxtests pass, including9neworder
and10existingconcurrency cases. ActualHTTP peaks8/16/3 and16requests over8TCP
connections retained. Independent review found no remaining integration blockers.


### Generic durable outbound queue (2026-11-02)

Frozen phase5f contract before implementation, including a coordinated metadata
preview API addition before its callers changed. Storage, common runtime and
independent quality used separate Git worktrees/branches. New mqtt-egress-log has
no HTTP, Sink, SQLite or Tokio dependency. All production outbound adapters now
consume committed records through the shared durable scheduler; message bodies
are segmented append files, SQLite continues to store configuration only.

Per-outbound/exactClientID FIFO persists across restart. Different clients remain
parallel; known failure counts/absolute retry deadlines survive crash, uncertain
in-flight attempts replay stable delivery IDs, terminal completion is synchronized
before advancing a lane. Final failure still continues the next record. The
blocking actor coalesces appends, begin markers and compatible terminal outcomes.
Bounded metadata preview reserves exact body windows before reading payloads;
perclient disk heads avoid a single slow client's backlog filling RAM. Idle waits
use watch notifications/deadlines, not read-triggered feedback or periodic polling.

Versioned header/body/payload checksums, private ownership lock, strict tail vs
complete-corruption behavior and atomic generation manifests protect recovery.
Checkpoint compaction streams bounded groups and reclaims old generations only
after publication sync. Missing manifests fail closed without deleting messages.
Directory/file/object counts and decode vectors are bounded; completed historical
outbound indexes are removed. Physical file-byte limits reserve half for progress,
compaction transiently allows up to2x plus small control files; filesystems may
allocate additional block/inode metadata. Overflow rejects new records.

Pending target changes/removals return409 before SQLite save. Disable pauses and
retains backlog, rename/rule changes preserve saved work. Generations stop/join
before replacement claims. CLI provides queue directory/segment/byte/record limits;
Dashboard backlog uses durable counts and survives restart. No implementation
banner, message body/credential read endpoint or static-data exposure was added.

Durability starts at synchronized append, not MQTT ACK. Pre-append admission is
still bounded best effort; no MQTT/outbound atomic transaction, replicated queue,
dead-letter replay or production TPS/p99 certification is claimed. Remaining work:
measure group-commit/compaction latency and throughput under real backlog, consider
more efficient readiness indexing for many retrying clients, and separately design
optional ACK-coupled admission/dead-letter operations if requested.

Evidence: docs/verification/durable-outbounds-20261102/README.md. Existing user
services untouched; all tests use temporary local Broker/log/SQLite/HTTP resources.

Final phase5f validation: Fmt,strict workspaceClippy,670Rusttests(including28journal
and41egress),82frontendtests,TS/Vite/Prettier and embeddedRelease pass.119/119
Releaseblackboxtests pass on final62a7b00 candidate, including12durability cases.
Observed concurrency8/16/3 and16requests reusing8TCPconnections retained. Review
also fixed self-wake capacityspin with required-byte waits and initialization
cancellation/error storage cleanup. CLI/UI changes included in this validation.


### Durable journal performance baseline (2026-11-02)

Added standalone benches/egress-log without production changes. M4/16GiB/macOS,
internal FileVaultAPFS and externalUSBAPFS,50sequential Releaseprocesses completed
in199s.Three-run medians,1024appendcalls/case,128/1024B,groups1/8/64;read afterwrite
withCRC andfullrecordverification. p99 isperAPIbatch,notamortizedpermessage.

128B appendrecords/s internal234/1900/13647,external507/3170/18463. Warmreads
~25k–60krecords/s. Fullappend/read/begin/completecycle group64 internal4378/s,
external6212/s (noHTTP/actor).65536x1KiB livecheckpoint~1.86s,reopen~0.18s;
IOprocesspeakRSS~23MiB,checkpoint~42MiB,OSpagecacheexcluded. No processswaps.

3benchmarktests,fmt,strictclippy,Release/smokepass;all50runs/countchecks/cleanup
pass. Exactrawresults,machineinfo,commands,rangesandfullpercentiles under
 docs/verification/egress-log-performance-20261102/README.md. Thesearelocalengine
measurements,noproductionTPSclaim. Followup:targetLinuxbaseline,profileperrecord
open/stat/read andgroupcommit/compactionbeforechangingdurabilityorimplementation.


### Shared rule-result streams and independent consumers (2026-11-02)

先冻结 phase5g，再由日志、运行时、质量 Agent 在独立 worktree 中并行实施。
每条规则的转换结果只保存一次，原子附加所有获准出站引用；物理日志共享，逻辑流按规则划分。
投递 ID、目标版本、重试期限及进行中状态独立保存；最后一个消费者完成后才回收正文。
保序键调整为出站/规则/Client，不同规则独立推进，批次不混合规则，共享 8/16 并发上限。

Webhook 格式及稳定 ID 保持不变。正文读取、封装和发送均计入有界预算，保留无空转轮询
及容量唤醒修复。启动时原子迁移 v1，保留未完成投递的旧 ID 和状态；损坏、冲突或容量
不足时保留旧数据并拒绝迁移。接近满额的旧队列可能需要提高容量。实际旧 Broker 已验证
拒绝 v2 且不修改文件。旧索引重建缺陷先由失败测试复现，再修复。

690 项 Workspace Rust 测试（含日志 43、出站 46）、83 项前端测试、3 项独立基准测试，
fmt、严格 Clippy、TypeScript/Vite、Prettier 和嵌入前端的 Release 构建通过。
132 项 Release 黑盒通过，含 13 项新增多规则、扇出及迁移测试，验证不同 SQL 输出、
正文去重、3 引用/2 结果计数、独立重试、部分 ACK、重启恢复及旧记录升级。

基准已适配共享 API，目前仅重跑冒烟，旧性能数据仍标为历史基线。文档及 Dashboard
统计单位已更新，没有合并 main 或重启生产服务。验收证据：
[规则结果共享日志验收](verification/rule-result-streams-20261102/README.md)。
剩余：新模型 Linux 性能基线、队列 profiling；可选 ACK 耦合准入、死信和复制队列。


### Shared-result journal performance matrix (2026-11-02)

新增独立 topology 基准，直接复用生产共享结果/消费者 API，无生产代码改动。
完成多规则及单规则多出站、1 KiB/128 字节、组提交/逐条同步，共 39 次顺序 Release
测量（三轮/配置），核对所有消费者正文及唯一结果计数。6 项基准测试、fmt、严格 Clippy 通过。
1 KiB、64 条组提交：写入约 9,885–13,744 结果/s，读取约 45,856–57,060 投递/s。
同为 65,536 引用，16 规则单出站与单规则 16 出站分别峰值 RSS 52.12/23.95 MiB，
日志 79.53/12.34 MiB。逐条同步约 272–287 结果/s，明确与组提交分开报告。
这是 M4 本机热缓存、直接引擎测量，不是 MQTT/Webhook 生产 TPS。
[完整环境、原始数据及复现命令](verification/shared-log-performance-20261102/README.md)。
后续：Linux/NVMe 目标基线、持续排空/状态写入、文件读取路径 profiling；本轮未部署。


### Bounded journal IO optimization and repeat matrix (2026-11-02)

先冻结 phase5h（含可信追加前缀澄清），日志/质量 Agent 在独立 worktree 实施与验证，
集成9049def、0cf7609，质量c7c4faf。最多64KiB帧写合并、兼容CRC slicing8，Unix
最多16个校验FD、8MiB/8192条多消费者正文缓存。追加前验证、成功sync后按段推进可信
前缀，使Begin/Retry/部分Complete交错仍共享一次正文读取；换代/最后消费者/异常指纹
正确失效。API、磁盘格式、sync和poison/recovery语义不变，非Unix旁路缓存。

699项Workspace Rust、18项独立基准/完整性、62项Release MQTT/Webhook黑盒通过，
fmt、严格Clippy、Release构建通过。新回归旧版4次读盘失败，优化后为1次；覆盖外部
篡改、mtime恢复、权限/链接/替换、FD逐出、缓存容量和重启/崩溃恢复。

原39场景全部复测并保留旧数据，工作负载源码SHA一致，逐轮配置/统计/日志字节相同。
1KiB batch64写入提升11%–26%、读取10.4–21.8倍；单规则16出站读取57,060→1,242,824/s，
峰值RSS23.95→28.78MiB。128字节/逐条同步历史写入有持平或下降，未隐藏；额外18次
旧/新交替诊断，小包16出站写入约+6%、1KiB4出站约+24%、逐条sync持平。读取工作窗口
较短，非隔离M4实测，不是生产TPS承诺。未部署或合并main。
[完整数据、环境、限制及复现](verification/shared-log-io-optimization-20261102/README.md)。
剩余：隔离Linux/NVMe基线、持续入队/排空与确认写入、缓存工作集/调度局部性profiling。


### Multi-frame Group Commit and segment acceptance (2026-11-02)

先冻结phase5i与借用输入/真实同步计数契约，日志、运行时、质量Agent分别在独立
worktree实施。集成dacd4e6+84620d0日志、a5ebed5运行时、4622acf+3ae3ec1质量。
Journal新增borrowed append_result_groups，最多4个旧Results帧，每帧64结果；全量
预检后写入、所有脏段sync成功才apply/reply。帧格式及原子扇出保持，失败仍poison、
允许未确认完整前缀冷恢复，非跨帧事务。可信正文缓存按段保留，无全状态/正文clone。

Actor仅聚合已排队连续Append，最大256结果/8MiB raw/16MiB估算；不等定时窗口，
其余命令FIFO屏障不变。纯预检Full逐帧fallback保留可接受前缀；checkpoint自身Full
依旧poison后Io停止。追加数据sync计数含轮转、排除state/checkpoint/init/dirsync。

722项Workspace Rust、29项独立基准/完整性、62项Release MQTT/Webhook黑盒通过，
fmt/strictClippy/Release通过。日志66、运行时55包含在Workspace中。覆盖4帧1sync、
跨4段4sync、后帧错误零写入/不耗ID、部分写/首段sync后失败、撕尾、poison恢复、
96/300请求聚合、屏障/低负载/取消/admission与Full前缀、旧新帧逐字节兼容。

完成54次分段×组宽直接测量、6次逐请求确认、原39控制（三轮/配置），无覆盖旧数据。
16MiB段组宽1→4，1KiB正文：单规则单出站15,530→55,406结果/s，四规则13,804→46,700，
单规则16出站11,925→38,075；写完RSS分别增加约0.25/0.33/0.95MiB。并发确认吞吐
13,332→18,012/s、确认p99 12.24→13.64ms，明确未获延迟改善。历史单条控制下降
额外6次旧新交替诊断约257→257/s，保留全部原始数据，不伪称逐条同步加速。
默认Group Commit已生效，默认分段保留1MiB兼容小容量；提供16MiB现有CLI调优命令。
64MiB无稳定全面优势，非隔离M4数据不是生产TPS承诺。未部署、重启或合并main。
[验收、原始数据及复现](verification/group-commit-20261102/README.md)。
剩余：Linux/NVMe长时基线、真实混合命令聚合程度与p99、持续排空/背压；状态操作
Group Commit暂未实现，不计作完成。


### Kafka outbound and Dashboard (2026-11-02)

先完成rdkafka0.39.0的cargo zigbuild静态探针，再冻结phase5j，后端/React/质量
真实Agent分别在独立worktree实现。集成a590af6、646d07c后端，0c8a199页面，
9054992、21f34c1、1fbbd1a质量；975b969修复审查发现的历史绑定字节兼容缺陷。
1822825隔离复制二进制测试的规则数据库路径，保留全部无解压断言。

复用共享规则结果日志/outbox状态，无新增消息SQLite或每出站正文副本。
Kafka默认Client ID key、原始SQL JSON value、稳定身份headers，同规则/出站/Client
确认/最终失败后推进，其他Client并发。单出站默认128、全局512，与HTTP16独立。
有界生产者、TLS/mTLS、PLAIN/SCRAM256/512、安全占位符编辑和高级调优已接入页面。
等待delivery report才Complete，重试/重启/SIGKILL可稳定ID重放，仍可能重复。

743项Workspace Rust、96项前端、95项Release独立黑盒（含14项真实Kafka）通过；
fmt、严格Clippy、helper检查、嵌入React Release通过。确认并修复native并发关闭
生命周期、旧HTTP连接池滞留及旧Webhook耐久descriptor序列化兼容，不放宽日志校验。
最终完整Broker本地zigbuild到Linux amd64 musl，通过无解释器/无共享库ELF检查；
尚未在Linux执行/部署。独立Kafka CI已提交，未宣称远端执行通过。
[环境、测试、故障证据和复现](verification/kafka-outbound-20261102/README.md)。

剩余：Linux静态产物实际运行验证、隔离环境Kafka多副本故障/长时吞吐与p99基线、
动态Topic及其他出站类型。未实现Kafka事务/Schema Registry/OAuth/Kerberos，
不承诺端到端exactly-once。消息落盘前入口仍best effort；终止失败会结束当前投递。
本轮未部署或合并main，代码保存在codex/kafka-outbound。


### Structured process logging (2026-11-02)

接入tracing/subscriber/appender，默认Broker INFO/依赖WARN，支持RUST_LOG静态模块
过滤和JSON事件。stderr后台有界8192条，拥堵丢日志；guard在Tokio runtime外，
保留stdout帮助/导出与CLI错误诊断，不改业务耐久日志。生命周期事件不记录消息正文
或凭据，没有新增逐包/逐连接INFO。日志首行就绪标记回归先RED后GREEN。

748项Rust、30项Release管理/嵌入/mTLS黑盒、fmt、严格Clippy通过；宿主Release与
Linux amd64 musl静态交叉编译通过，未部署或在Linux运行。工具RUST_LOG=warn导致
依赖INFO行的旧监督器等待超时，正式验收显式设定rmqtt=info，未放宽安全验证。
[证据与复现](verification/structured-logging-20261102/README.md)。
剩余：按业务故障需要补充安全/限流埋点；文件轮转、热更新尚未实现，当前由运行环境
采集stderr。没有测量或承诺吞吐/p99不变。分支codex/structured-logging。

### Phase5k — krafka replacement (2026-11-02)

Production Kafka SDK is now pinned krafka0.28.1/rustls-ring; librdkafka/OpenSSL/
libcurl are absent from production dependencies. Existing SQL output bodies,
durable shared logs, configuration and per-client lanes remain unchanged. Private
owner runtime fences uncertain delivery/background tasks before retries or
replacement; admission512/8MiB, SDK buffer8MiB, owner16 and connection64 bounds.

801 Rust tests, strict Clippy/fmt,16real Kafka cases (15+codec) and static Linux
amd64 musl build/ELF inspection passed. Three paired benchmark runs independently
verified49152records per SDK, no errors; lower RSS and binary size, but measured
TPS declines3.7..20.2% in this local flush-heavy workload. Multi-partition P99
improved; single-partition median-run P99 worsened. No universal performance gain
or MQTT end-to-end TPS claim. Linux artifact23.03→18.45MiB.

[Reproduction, complete metrics, regressions and limits](verification/krafka-migration-20261102/README.md).
Remaining: Linux execution, multi-replica fault/long-soak and performance profiling;
at-least-once replay remains. Preexisting HTTP management changes were preserved;
this phase does not complete that separate integration gate. No deployment/main merge.


### Stable main publication (2026-11-02)

Kafka replacement integrated from stable main, excluding unfinished HTTP integration.
Final scope:760 Rust,96 UI and16real Kafka cases passed; strict Clippy/fmt, Release
and Linux amd64 musl static build/inspection passed. HTTP development is preserved
on codex/http-access-integration-wip (7026380), with the full integration gate still
pending. [Final merge acceptance](verification/main-publish-20261102/README.md).


### Phase6b — scoped HTTP admission completed (2026-11-02)

Supersedes the historical contract-only/expanded Phase6a status. Reused HTTP runtime,
cache, SQLite configuration and Dashboard; integrated CONNECT authentication and
PUBLISH/SUBSCRIBE admission before effects. NewWill checks once at CONNECT; ACKs,
accepted QoS2 retries, delivery/shared/retained replay and recovery do not re-query.
Compact admitted policies release old generations; stopped/unavailable services
fail new operations closed without revoking accepted state. No durable schema change.

814Rust/114UI/29realHTTPMQTT cases,20local-auth/25PLAIN SQLite regressions, strict
Clippy/fmt, native Release and locked Linux amd64musl static build passed.
[Evidence/reproduction/limits](verification/http-admission-20261102/README.md).
No performance benchmark or main merge/push/deployment in this phase; no PostgreSQL,
receive-time/retroactive HTTP revocation or HTTP custom TLS identity.

## 2026-11-03 — Phase6d client lifecycle system topics accepted

Delivered real `$SYS/events/client/connected` and `disconnected` publications,
QoS1/nonretained, no schema_version. MQTT subscribers and existing SQL/outbounds
receive the same event with admitted username and boot/connection identity.
Connected is emitted after successful CONNACK write; cleanup covers normal close,
EOF, KeepAlive, takeover and graceful shutdown. Broker origin preserves No Local;
observer affected client_id preserves generic per-client outbound scheduling.

Three independent worktrees implemented transport, session spoof protection and
real wire acceptance. New client PUBLISH/Will cannot impersonate `$SYS` even under
permissive ACL; legacy reserved Wills retire durably without schema changes.
A real shutdown failure was reproduced and fixed with a bounded executable drain
before existing egress shutdown. No new connector or event-specific queue storage.

Actual829Rust tests pass (one explicit live-PG ignore), final40 executable tests,
strictClippy/fmt,25 lifecycle wire cases and20 auth cases pass. Basic14,
Retained/Will20,Shared22 regression cases passed on the preceding integrated binary.
NativeRelease rebuilt; no deployment/main merge/push. Event admission remains
bounded best effort; crash-complete audit and performance improvement not claimed.
[Evidence](verification/system-client-events-20261103/README.md).

## 2026-11-03 — Phase6e privileged HTTP publishing accepted

User replaced the initial Basic/cache proposal with the same Dashboard Bearer token.
Canceled auth worktree restored; mqtt-auth/mqtt-auth-http/lock/CLI unchanged. Delivered
POST/api/v1/mqtt/publish with topic,payload,qos(default1),retain(defaultfalse), HTTP
support and existing management protection. MQTT source authentication/ACL are not
called, even when configured backends deny or are unavailable. Ordinary MQTT clients
still follow authentication and subscription permissions.

Independent transport/quality worktrees delivered bounded host ingress and wire
acceptance; coordinator integrated HTTP/state/wiring/docs. Reused existing routing,
QoS/offline sessions/retained durability/shared/SQL/outbounds; no synthetic sessions.
Actual837Rust tests pass (1explicitlivePGignore), strictClippy/fmt,23newHTTPwire,
25systemevent and29HTTPaccess cases pass on identical nativeRelease. No newperfclaim,
deployment or mainmerge/push. [Evidence](verification/http-publish-20261103/README.md).

## 2026-11-03 — Phase7a first embedded cluster gate accepted

User resumed cluster work and superseded the old sharded-directory proposal.
Frozen phase7a Controller/Broker contract and generation/lease safety amendments
before coordinated implementation. Three real specialist agents wrote independent
Git worktrees for control/RPC/storage, transport and independent wire acceptance;
coordinator integrated CLI, neutral APIs, review fixes and evidence.

Delivered actual three-member OpenRaft quorum with SQLite WAL/FULL metadata/log/
vote/snapshot persistence, token/mTLS peer adapter, Broker ownership and monotonic
leases, local snapshot+revision-delta routes, bounded direct forwarding and live
MQTT3.1.1/5 QoS0/1/2. Existing HTTP downlink and rules observer are reused. Thin
lib.rs structure and standalone durability/public APIs remain preserved.

Independent failure tests exposed stalled leader watches and expired Broker rejoin;
fixed progress/idle/revision detection, confirmed Controller preference and safe
re-registration. Review also fixed stale subscription generations, canceled receipts,
snapshot allocation limits and mixed-controller lease durations. Per-incarnation
maximum outstanding grants persist so leadership changes cannot shorten them.

Final identical native binary: 20 real cluster cases and 77 standalone wire cases
pass; 852Rust tests pass with one explicit live-PG ignore; official OpenRaft storage
conformance, strict Clippy and fmt pass. Source through 1bef408; logs and reproduction
are in [verification](verification/cluster-20261103/README.md). No main merge/push or
deployment, Linux/mTLS wire acceptance, capacity or performance claim in this gate.

Remaining: durable cross-node forwarding responsibility/receipts, persistent-session
failover, retained/Will/shared cluster semantics, real compaction/snapshot-transfer
faults, peer mTLS interoperability, auth/config distribution, large-topology profiling
and optimized indexed metadata/routing. Unsupported cluster features are explicitly
refused; this is not full MQTT cluster compatibility or production HA acceptance.

## 2026-11-03 — Phase7b reliable forwarding and persistent failover accepted

Frozen phase7b transaction contract/core API, then used three independent writing
worktrees for runtime, transport and quality. Added explicit durable mode with
three fixed Broker data replicas and W2 acknowledgment; Controller Raft still holds
metadata only. New group/mode and disk identities are durably anchored, so mixed
replica groups or empty replacement voters cannot silently discard confirmed data.

Existing SessionEngine snapshots/QoS/outbox are reused. Source snapshot and stable
forward intents commit together; recipient snapshot and receipt watermark commit
together. FIFO source streams freeze recipient identities/logical generations and
retain original IDs through migration. A logical reset clears old recipient state
without erasing already accepted source obligations. Authenticated inspection and
epoch CAS precede takeover; recovering an abandoned owner drains pending work
without requiring the source client to reconnect. Old local RowStore rows remain
mirrors and cannot overrule quorum recovery.

Independent failures/review exposed host lazy-initialization races, empty-disk
voting, full-queue duplicate progress, automatic recovery racing a live owner,
historical payload cache retention, stale filter/mirror state and target reset
isolation. Added regressions and integrated fixes. MQTT5 completed-PUBREL test
correction accepts the normative0x92 only at the three completed-retry call sites;
initial-success and no-duplicate-delivery assertions remain strict.

Actual final identical native binary through7fa2784: 27 new durable cluster wire,
20 prior cluster wire, and77 standalone wire cases pass. 872Rust tests pass with
one explicit live-PG ignore; formal OpenRaft storage suite, strictClippy/fmt pass.
CI adds protoc and the real cluster/system-event/HTTP gates; no remote Actions run
is claimed. [Reproduction, logs and limits](verification/cluster-durable-20261103/README.md).

This is a fixed-three-data-node correctness gate, not large-cluster capacity or
production-complete MQTT acceptance. Retained/Will/shared cluster support, replica
repair/replacement, topology changes, legacy/rolling migration, generic tombstone
reclamation, invalid-actor retirement and performance acceptance remain. QoS0
subscriber delivery stays best effort. Byte/count exhaustion fails closed. No new
Linux execution, cluster mTLS wire, physical power-loss, TPS/P99, mainmerge/push or
deployment result. Next work: safe replica repair and cleanup, then profile pending
scheduling/indexed matching/quota accounting with reproducible equivalent loads.

## 2026-11-03 — Phase7c replica repair and empty-disk replacement accepted

Frozen the repair/generation/learner contract before three independent runtime,
Journal and quality worktrees implemented and committed their owned modules.
Coordinator integrated CLI and reviewed transaction boundaries. Maintenance repair
freezes the two original donors, imports their bounded version/fence union into an
excluded learner, verifies SHA256 and installs atomically. Controller proof checks
precede disk-anchor replacement and voter activation. Ordinary requests enforce
persistent generations; Abort restores the originals while the target stays excluded.

Independent tests and review fixed frozen-donor restart readiness, transient freeze
on ordinary transactions and blocking Journal ownership release. Crash-test process
startup now isolates test-only fork/exec file-lock inheritance; deterministic
regressions preceded the fixes. lib.rs remains declarations/re-exports only.

Actual server 47.84.224.186: locally zigbuilt static Linux amd64 musl release,
checksum verified, no server compilation. Repair12, durable27, prior cluster20 and
standalone77 wire cases all passed. Local Rust887 passed/one live-PG ignore, strict
all-target Clippy and fmt passed. Full environment, binary hash, timings, logs and
reproduction: [verification](verification/cluster-repair-20261103/README.md).

This is a single-host multi-process correctness gate; no capacity/TPS/P99,
physical power-loss or multi-host HA result is claimed. Test processes exited and
no production service was installed. Branch integration only, no main merge/push.
Remaining: online incremental repair; membership changes and rolling migration;
tombstone/abandoned-host cleanup; cluster Retained/Will/shared semantics; peer mTLS
wire and multi-machine network-fault acceptance; indexed routing/quota/scheduling
profiling and reproducible performance gates. Use the new
[repair runbook](cluster/replica-repair.md), not directory deletion, for disk replacement.

## 2026-11-03 — Phase7d remote topology comparison completed; load gate failed

Added supervised Controller+Broker cohosting in one process using the existing two
role configurations/journals. Froze the topology/metric contract, used independent
loadgen and quality worktrees, integrated bounded setup, cyclic cross-node endpoint
assignment, raw-metric checks, Controller observation probes and CI commands.
No new role/storage schema or lower quorum was introduced. lib.rs remains thin.

Locally zigbuilt static amd64 musl releases were uploaded to47.84.224.186; Rust
loadgen ran on47.84.241.200 over172.16.240.180/181. Tested3 combined processes,
then3 pure Controllers+3 pure Brokers, all on the same8vCPU/one-disk Broker host.
Each topology had12 configured Tokio workers and RF3/W2 durability.

Both topologies passed all QoS0/1/2 at the stable12-connection30/s baseline:
each cohort900/900, no missing/duplicate or client-detected corrupt payloads;
MQTT3.1.1 interoperability150/150 eachQoS also passed. Bounded-window10s delivery
TPS forQ0/1/2 was49.2/49.4/39.1 combined and49.2/50.2/40.4 split. These are not
maximum capacity numbers. Independent raw reconstructions match all latency
series, event counts/TPS, finalACK and completion records.

IMPORTANT: all six60-connection maincases per topology failed DURING WARMUP with
0x83/0x8b errors. No measured cohort exists; TPS/p99 capacity and zero-loss claims
are invalid for those failed gates. Splitting roles did not remove the failure.
Default auth-limit4 refusal was a separate preserved preflight failure; final
settings128 and setup-concurrency1 still reproduced the data-path failures.

Controller probes matched all observed apply/revision advances without read errors
or censored catch-up; all data replicas finally reported the same metadata revision.
10ms sampling/ordered reads and differing setup traffic limit quantitative latency
comparison. Both paced route probes passed30trials. Overall baseline resource
meanCPU/peakaggregateRSS:0.60core/142.9MiB combined,0.52core/195.3MiB split; these
whole-phase windows include setup, warmup, drain, probes and idle. Shared physical
disk and failure domains preclude extrapolation to3/6 physical hosts.

Local893Rust tests passed/one livePG ignore; strictClippy/fmt and9 new topology/probe
checks passed; CI wiring added but no hostedActions run claimed. Full hashes,
rawrecords, independent audits, resource windows, reproduction and limitations:
[report](performance/cluster-topologies-20261103/README.md).

Six-process test deployment remains running on privateMQTT18831/18832/18833, with
no autostart; loadgen/monitors exited. Remaining blocker: identify actual rejection
branches via bounded counters/profiling before altering Durable32/Journal32 quotas,
100ms work scheduling, complete-record/quota scans and sync behavior. Preserve
quorum, atomic outbox/receipts and generation fencing. Large-load acceptance,
multi-machine/multi-disk tests, existing cluster feature/cleanup limits remain open.

## 2026-11-03 — Phase7e confirmed admission failure, bounded-backpressure fixes

Linux diagnostics confirmed Journal32 immediate rejection precedes quorum failure,
cache poisoning/reacquisition and disconnection. Journal waiting exposed the same
fail-fast policy in upper Durable32. Both layers now retain execution limits and
use finite32-entry/750ms async waiting; business body16MiB and actor8 bounds remain.
Body is charged before waiting; actor capacity precedes global business admission.
Background Work/Forget skip without occupying business waiting budget. W2,
outbox/receipt atomicity and persistent generation fencing are unchanged.

Old deterministic saturation regressions failed before fixes. Final904Rust tests
passed/one livePG ignore; strictClippy/fmt passed. Five selectedLinux quorum,
receipt/QoS2 recovery and non-voting repair tests passed; selector errors from first
command retained separately. Code review and implementation used independent WTs.

On the same60-connection100/s128B case, no failed pairs remained; QoS0/1/2 sent and
delivered1773/1531/1101 with complete finalACKs and no duplicates. HOWEVER planned
3000 percase was not achieved: missing sends1227/1469/1899, so capacity gate remains
FAILED. Offering deliveryTPS58.1/50.033/35.7 andp99~908/1006/984ms. This resolves the
false-failure/disconnect chain, not the remaining throughput problem. Shared-disk
IO sample util averaged36.8%/peaked82.6%; no proof hardware saturation explains it.
Remaining: profile serialization/SQLite critical section/sync/scheduling separately;
evaluate Group Commit and pending-work indexing/notifications without weaker ACKs.

Finalsource d8d0eb0 locally zigbuilt and hashverified onserver; six-process test
fleet now runs under /opt/rmqtt-cluster-diagnosis/20261103/fixed2/separate (same
privateMQTT ports), olddirectories preserved/stopped. No new mixed-role or physical
multi-host capacity claim. [Evidence](verification/cluster-backpressure-20261103/README.md).

## 2026-11-03 — Phase7f typed RAM replicas

User replaces data-plane SQLite with bounded typed RAM; contract frozen before
isolated storage/runtime/quality worktrees. Integrated storage f163821, runtime
677cca5/c573b1e, quality a8ef9c8; CLI forbids local SQLite mirrors. Controller Raft
and rules/access configuration retain SQLite; generic outbound append logs unchanged.
Historical durable.replicas configuration now denotes volatile replication only.

905 Rust tests passed, one external PG test ignored; fmt and strict all-target
Clippy passed. Native real-process RAM17, retained legacy cluster20 and observation
probe4 passed. Both MQTT versions/allQoS, offline/inflight owner failover, blank
replica exclusion, same-process repair, interrupted staging and whole-group RAM
loss failclosed are covered. Earlier disk-restart data guarantees are superseded,
not claimed passing. Independent review found no blocking cross-layer regression.

Local Zig musl release deployed fresh six-process cluster on47.84.224.186 under
/opt/rmqtt-cluster-memory/20261103; private MQTT172.16.240.180:18831/18832/18833.
Old SQLite directories preserved/stopped. Data directories contain only data.lock
and memory.identity. Benchmark evidence and final results:
[Phase7f report](verification/cluster-memory-20261103/README.md).

Remaining: RocksDB backend and disk durability contract (not implemented), reducing
record serialization/100ms background-poll latency, automated incremental catch-up,
membership/cleanup and actual multi-host fault-domain acceptance. Two live copies
survive one owner loss; whole-group process loss cannot recover data. Cluster
Retained/Will/shared subscriptions remain unsupported. lib.rs stays modules/exports.

RAM server reproduction: all QoS cases planned/sent/received3000, zero duplicates,
zero failed pairs, complete QoS1PUBACK/QoS2PUBCOMP. Offering deliveryTPS99.933/99.833/
99.933 (few final deliveries in drain); p99send→delivery107.115/106.881/110.069ms.
287 one-second whole-run samples including setup/idle: six-process CPU mean39.37%
of one core, RSS sum peak241.45MiB (data-process sum peak142.87MiB). This clears
100/s only; not maxTPS or disk-equivalent durability. Residual100ms poll candidate
and record serialization still need profiling.

Deployed Linux musl ELF also passed the full17 RAM real-process fault/repair tests
in103.434s, after load measurements. First remote harness copy missed an imported
helper and failed before test execution; complete helper upload resolved it without
source changes. Evidence retains that setup failure and the full passing log.

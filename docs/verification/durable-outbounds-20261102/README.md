# Generic durable outbound queue acceptance — 2026-11-02

Frozen contract: ../../contracts/phase5f-durable-outbound-queue.md, initial commit
1b00aa7. Storage, runtime integration and independent quality used distinct Git
worktrees and branches; coordinator owns CLI/status UI/docs and integration.
Environment: Darwin arm64, rustc1.98.1, cargo1.98.1, Node24.12.0, Python3.14.

## Guarantee

All generic outbound Sinks read committed records through the same segmented
journal. Only configuration remains in SQLite. MQTT observer admission remains
bounded best effort and is not coupled to MQTT ACK. Successful journal append/sync
marks durable acceptance. Known failure counts/deadlines and terminal outcomes are
persisted; uncertain in-flight sends may repeat with identical delivery IDs.
Terminal failure deliberately ends a record; no dead-letter replay is provided.

Exact outbound/client FIFO persists through restart; other clients can progress.
Disabled outbounds retain backlog; nonempty target bindings cannot be removed or
changed. Name/rule changes preserve saved work. Shared adapter pools and global16 /
per-outbound default8 active request limits remain. Per-batch durable state max64
records is an additional upper bound on configured batch size.

## Storage

Private lifetime-owned directory, framed/versioned/checksummed transactions,
metadata-only client-head indexes and bounded body reads. Default1MiB segments,
256MiB active physical budget and100000 pending records. Conservative logical live
capacity is half physical budget to reserve progress. Generation compaction may
transiently use up to2x capacity plus small control files. Full rejects new records;
existing records can still complete/retry/reclaim. Payloads/opaque bindings are
private files; no payload/credential read endpoint or static asset exposure.

No full-backlog clone/serialization on ordinary mutation. Checkpoint compaction
streams bounded groups into a new generation, syncs files/directory, publishes
CURRENT atomically and syncs root before deleting old generation. Missing manifest,
complete-frame corruption and sealed-segment damage fail closed. Only a provably
incomplete active tail can be truncated. Header lengths are protected by checksum.

## Reproduction

```sh
(cd dashboard/web && npm test && npm run build && npx prettier --check src tests)
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --offline -- -D warnings
cargo test --workspace --offline
cargo build --release --offline -p rmqtt
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/outbound_durable_blackbox.py -v
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/outbound_client_order_blackbox.py -v
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/outbound_concurrency_blackbox.py -v
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/rules_webhook_blackbox.py -v
```

SIGKILL is a process-crash test, not physical power-loss simulation. Temporary
Broker/SQLite/log/HTTP resources belong to tests. No production service is restarted;
functional concurrency/recovery results do not establish production throughput/p99.

## Final results

Final integrated runtime commit62a7b00 + enginee247f79, CLI/UI work included in the
final validation build. Fmt and strict workspace Clippy pass. All670 Rust tests
(including28 journal and41 egress),82 frontend tests, TypeScript/Vite/Prettier and
embedded native Release build pass. Final Release black-box119/119:

| Suite | Passed |
| --- | ---: |
| Durable outbound restart/crash/failure | 12 |
| Outbound concurrency/pool/cancellation | 10 |
| Client ordering | 9 |
| SQL rules/Webhook/configuration | 18 |
| Dashboard management | 14 |
| MQTT session persistence | 14 |
| WS/WSS | 18 |
| TLS | 12 |
| Admission/mTLS | 12 |

The12 durability cases cover graceful restart, SIGKILL with perclient FIFO and
otherclient progress, stable payload/event/delivery IDs after uncertain acceptance,
retry deadlines/failure budget, crash during the last retry, pause/re-enable,
name/rule updates,409 pendingbinding mutation/removal withoutSQLitecommit,
private segment rather thanSQLitepayload storage, backlog beyondRAMwindow,
record/diskfull recovery and filesystem rotationfailure failclosed. IOpermission
injection ran withoutskip on this nonroot Unix environment. SIGKILL does not prove
physical power-loss behavior.

Actual functional receiver peaks remain8(default),16(global) and3(configured).
Sixteen requests reused8TCP connections; three outbounds completed24 requests.
No production TPS/p99 or speedup is claimed.

## Regression evidence and review fixes

Original Release lost committed heldbacklog after graceful restart (egress-red).
Journal review regressions reproduced missingCURRENT deletion, corrupt header
length mistaken for partialtail, live payload corruption reblessed bycompaction,
unbounded orphan enumeration and historicalempty target indexes before fixes.
Engine fault injection exercises commit before/after sync and CURRENT publication,
unknown write result poisoning, segment reclamation and bounded decoding.

Runtime regression reproduced ownmetadata-budget release waking a payload window
that still couldnotfit. Required-byte capacity waits now check actual availability
with a cancellation-safe memory epoch, avoiding CPUspin and lost externalrelease.
Idle multioutbound tests verify no read-notification feedback or polling. Generic
nonHTTP rawbyte Sink tests exercise retry and uncertain recovery. Startup store
RAII handles cancellation/failure on blocking workers; begin completion rechecks
stopped/cancelled before calling Sink. No awaited locks or async file IO added.

Two test observation races were corrected to wait for durable terminal commit
rather than HTTP receiver acceptance. Assertions/required counts unchanged.
Early generic fixture failures did not reproduce after isolated/full reruns;
fixture paths now use an atomic counter and test-only typed initialization errors.
Final full workspace and black-box suites pass. Existing Python temporary-directory
ResourceWarnings and jsdom/build notices remain nonfatal test-environment output.

All owned test resources were cleaned up. No production restart, main merge or
remote deployment performed. Future work: production profiling of commit/compaction
and retry-head selection under large backlog; ACK-coupled admission, replication
and dead-letter replay require separate contracts.

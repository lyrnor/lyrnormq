# Client-ordered outbound acceptance — 2026-11-02

Contract: ../../contracts/phase5e-client-ordered-outbounds.md.

Implementation, independent HTTP tests and Dashboard wording were assigned to
three distinct Git worktrees/branches rooted at frozen contract commit 8a2dfcd.
Coordinator integrates and reviews in codex/webhook-settings-integration.

## Behavior and limits

Exact (outbound, source Client ID) lanes preserve admitted enqueue order across
topics, matching rules and source reconnections within one configuration generation.
Batches contain only one client. Different clients can send concurrently (default8,
configured1..16, runtime global16). A retrying batch owns its client's lane, while
backoff releases both attempt permits. Final failure counts failed and continues
the client's next batch. Concurrency1 limits active attempts; other clients can
send during backoff, so it does not imply global FIFO.

Aggregate resident admission is bounded by queue_capacity + concurrency *
min(batch_size, adapter.max_events) + 1. Reservations cover internal client identity
bytes, lane queues, running jobs and completed results awaiting collection. Idle
lanes are removed; cleanup cancels/joins all child work. Existing global16MiB event
budget, shared HTTP pools, response body handling and status units are preserved.

No SQLite/API schema migration, MQTT session/ACK/outbox change or durable action
queue. Admission remains best effort; overload may drop new events. Replacement
cancels the old generation. HTTP timeouts can leave remote work executing, so
remote processing order/idempotency still requires receiver cooperation.

## Reproduction

Environment: Darwin arm64, rustc1.98.1, cargo1.98.1, Node24.12.0.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --offline -- -D warnings
cargo test --workspace --offline
(cd dashboard/web && npm test && npm run build && npx prettier --check src tests)
cargo build --release --offline -p rmqtt
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/outbound_client_order_blackbox.py -v
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/outbound_concurrency_blackbox.py -v
RMQTT_BIN="$PWD/target/release/rmqtt" python3 tests/rules_webhook_blackbox.py -v
```

All tests use owned temporary Broker/SQLite/receivers, not production services.
Functional concurrency/order measurements do not establish production TPS or p99.

## Final results and review

Fmt and strict workspace Clippy pass. All 631 Rust tests and 81 frontend tests pass;
TypeScript/Vite/Prettier and the embedded Release build pass. Release black-box:
9 client-order + 10 concurrent-outbound + 18 rules/Webhook + 14 Dashboard +
14 persistence + 18 WS + 12 TLS + 12 mTLS = 107 passing tests.

RED evidence: the former scheduler failed the heldsameclient unit test and 8/9 new
HTTP cases; the remaining case already worked. New suite covers cross-topic FIFO,
multiple matching rules, same-ID source reconnect, retry identity/ownlane isolation,
concurrency1 backoff yielding to another client, 400 terminalfailure,503 retry
exhaustion followed by successor, homogeneousclient batching and40client overload/
replacement cleanup. Existing cap/pool/cancellation tests now publish from distinct
clients where crossclient parallelism is intended, retaining all prior assertions.

Actual receiver peaks remain default8, global16 and configured3. Sixteen requests
reused8TCP connections; three outbounds completed24 requests within globalcap.
These are functional concurrency measurements, not productionTPS/p99 benchmarks.

Coordinator and independent read-only reviewer found and fixed a completed-job
bookkeeping bound gap and eager-reap panic escape. 5000 immediate-success clients
assert both job/lane peaks<=residentcap9. Heldclient overflow and panic cleanup tests
assert all resident/byte/request permits return. The retained completed batches
keep every unreaped key covered by admission; idle lanes are removed on reaping.

Root integration commits: UI0791255, quality897867e, backend743e1a9. No owned source
changes outside assigned paths. Existing user services were not restarted. Existing
Python temporary-directory ResourceWarnings and frontend build/test environment
notices remain nonfatal; no acceptance failures.

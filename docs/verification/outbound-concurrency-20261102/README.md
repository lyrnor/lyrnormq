# Bounded outbound concurrency acceptance — 2026-11-02

Independent backend, Dashboard and black-box agents used separate Git worktrees.
Integrated on `codex/webhook-settings-integration`; no production service restarted.
Contract: ../../contracts/phase5d-outbound-concurrency.md.

## Behavior

Each outbound defaults to 8 concurrent batch jobs, configurable 1–16 in advanced
settings. A runtime-wide semaphore limits active local HTTP attempts to 16 across
all outbounds and configuration generations. One shared reqwest client per sink
retains connection pooling. Set concurrency to 1 for sequential sending including
retries. Otherwise arrival/completion may reorder. Existing configuration missing
the new field defaults to 8; other explicit settings and SQLite schema are preserved.
Single-message defaults remain batch size 1 / wait 0 ms.

Bounded tasks, queues and message-byte budgets remain enforced. Retry backoff
releases the global permit; stable delivery IDs survive retries. Replacement,
storage failure and shutdown cancel and join child work, releasing local counters,
permits and reservations. Remote business work may continue after local cancellation;
receivers still require delivery-ID idempotency. Outbound queues remain best effort
and volatile. MQTT session/ACK/outbox semantics and HTTP response-body policy are
unchanged.

## Reproduction and results

Use Rust stable and Node with the repository lockfiles, on the local macOS machine.
No distributed or production throughput/latency benchmark is claimed.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --offline -- -D warnings
cargo test --workspace --offline
cargo build --release --offline -p lyrnormq
cd dashboard/web
npm test -- --run
npm run build
npx prettier --check .
```

Run black-box suites from the repository root, for example:

```sh
LYRNORMQ_BIN="$PWD/target/release/lyrnormq" python3 tests/outbound_concurrency_blackbox.py -v
LYRNORMQ_BIN="$PWD/target/release/lyrnormq" python3 tests/rules_webhook_blackbox.py -v
```

 Logs record the exact suite results. Workspace: 627 passing
Rust tests; Dashboard: 81 passing tests and TS/Vite/format pass. Release black-box:
10 concurrency + 18 rules/Webhook + 14 Dashboard + 14 persistence + 18 WS + 12 TLS
+ 12 mTLS = 98 passing tests.

Real HTTP receiver measurements (functional bounds, not a TPS benchmark):

- Default outbound: peak 8; 16 requests reused 8 accepted TCP connections with 204
  keepalive responses.
- Three outbounds: global peak 16; 24 requests completed; observed per-outbound
  peaks 5, 5 and 6. Includes shared authority and a different receiver port.
- Explicit concurrency 3: peak 3; 11 requests completed.
- Tests also exercise serial retry order, stable delivery IDs, permit release in
  backoff, blocked requests, batch-size/byte splitting, cancellation and restart.

## Real browser verification

An isolated headless Chrome profile used the embedded Release Dashboard and temporary
SQLite/Broker/HTTP receiver. A real MQTT publisher triggered three simultaneously
held HTTP requests. Receiver active/peak, API total/per-outbound in-flight counts
and the Dashboard request column all read 3. After releasing the receiver, sends
completed and all local queue/in-flight/byte budgets drained. Basic configuration
stays collapsed; advanced defaults are 8/1/0 and the saved concurrency is 3.

See browser-checks.json, live-requests-desktop.jpg and concurrency-mobile.jpg.
390px layout has no horizontal overflow; no captured CSP/console errors. Pending
messages and active requests have separate units. Own QA processes/profile were
closed; existing user services/browser were untouched.

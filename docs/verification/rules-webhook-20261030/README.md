# Phase5a SQL/Webhook verification — 2026-10-30

Local macOS arm64; Rust stable1.98.1, Node24.12.0/npm11.6.2. Native optimized release
built after production React build, frontend assets embedded; no deployment or
throughput/p99 performance acceptance. Test server traffic is synthetic QA data,
not fabricated counters or production customer messages.

## Final checks

- cargo fmt --all -- --check: pass.
- cargo clippy --workspace --all-targets --offline -- -D warnings: pass (clippy.log).
- cargo test --workspace --offline:600 pass including doctests (rust.log).
- npm test:50 pass; strict TypeScript + production Vite build pass (frontend logs).
- Release independent unique methods72 pass: rules12/dashboard14/embedded4/TLS12/
  WS18/admission-mTLS12. Logs included. First parallel TLS oversized PUBLISH raised
  BrokenPipe during client sendall (early header rejection race); original retained.
  Two subsequent full unchanged serial TLS runs12/12 passed. No assertion weakened.
- sql core8 + transport151 tests passed in owner worktree; included in full600.
- Webhook15 real HTTP/queue/persistence tests included in full600; final owner log
  pool-final.log additionally shows dedicated keepalive lifetime regression.

## Behavior and security

Actual MQTT311/5 × QoS0/1/2 reaches SQL-filtered HTTP output while ordinary
subscription delivery remains correct. Nonmatches and binary metadata queries,
SQL whitelisting/NULL/errors, dry-run no network, real batching,600KiB events split
under1MiB HTTP request cap, retry/permanent errors, delivery IDs, same-target fanout,
slow-target isolation/overflow, CLI/auth/body limits, redaction, configuration restart
and sensitive static aliases are covered. Queues are explicitly best-effort, not
crash-durable; config persistence is not a durable action outbox.

First red logs retain SQL precision and observer-expiry bugs, missing delivery_id
and original test collection/header-case fixture mistakes. HTTP field names are
case-insensitive: corrected fixture normalizes names while still asserting exact
credential values. Runtime tests reproduce then fix old keepalive pool retention;
config snapshot swaps now release old clients. An injected directory-sync failure
checks published configuration remains inspectable and execution fails closed.
Frontend pending-delete-after-stop red test is preserved before its fix.

## Browser

Actual local Broker on separate8767 test port, MQTT5 publisher and stdlib HTTP
receiver. Browser created connector qa-webhook, action qa-send, rule qa-high-temp,
used SQL preview (temperature85), and observed successful live output for temperature92.
Final release restores copied QA configuration and delivers again; browser-delivery.json
contains the actual synthetic received envelope with event_id/delivery_id/rule_id.
Screenshots rules-desktop.jpg and rules-mobile.jpg. Desktop1440 viewport/page width,
phone390 viewport/page width/dialog width verified, no horizontal page overflow.
Final console browser-console.json empty. Temporary viewport restored, temporary
Broker/receiver stopped after QA. No existing user Broker restart/config mutation.

## Limits

Rules/actions/credentials are configured through explicit authenticated admin write
routes. No Kafka, PostgreSQL, Wasm, durable event admission/outbox, persistent dead
letters, raw payload history or exactly-once external side effects. No benchmark
result implied by functional counters. CI updated but hosted CI not executed here.

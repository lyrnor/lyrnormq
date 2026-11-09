# Unified outbounds acceptance — 2026-10-30

Local Darwin arm64, Rust/Cargo1.98.1 stable, Python3.14.7. Native release
`target/release/lyrnormq`; no server deployment or throughput/p99 measurement.

Contract: [Phase5c](../../contracts/phase5c-unified-outbounds.md). Backend, UI and
quality agents used independent branches/worktrees and submitted tested commits;
coordinator integrated and reviewed. A separate storage test owner fixed a test
isolation issue in a fourth isolated worktree without production lock changes.

## Results and reproduction

- cargo fmt --all -- --check: pass.
- cargo clippy --workspace --all-targets --offline -- -D warnings: pass.
- cargo test --workspace --offline: 620 passed including doctests/child helpers.
- cargo build --release --offline -p lyrnormq: pass.
- dashboard/web npm test: 51 passed; npm run build strict TS/Vite passes;
  npx prettier --check src tests passes.
- Real release blackbox: unified outbound18 + Dashboard14 + MQTT persistence14
  + WS/WSS18 + TLS12 + admission/mTLS12 = 88 passed. Quality initially estimated
  19 outbound methods; actual runner output is18 and is authoritative.

For blackbox reproduction set LYRNORMQ_BIN to the absolute release binary and run
tests/rules_webhook_blackbox.py, dashboard_blackbox.py, persistence_blackbox.py,
websocket_blackbox.py, tls_blackbox.py and admission_mtls_blackbox.py with python3
and -v. Each helper owns its ports/configuration. Local network permission needed.

## Verified boundaries

Version2 API has only rules/outbounds, typed endpoint config and shared delivery
policy. Unknown future types (Kafka/PostgreSQL/Redis), unknown fields/versions,
legacy PUT bodies and dangling references fail without config changes. The old
SQLite blob converts transactionally, preserves source action IDs as outbound IDs,
credentials and bindings; SIGKILL restart and actual HTTP delivery confirm recovery.
Invalid old blobs, including unused malformed connector IDs/counts, remain unchanged.
No old JSON-file backend or import exists.

The generic scheduler talks to Sink; adapter owns protocol-specific encoding and
error classification. Fake non-HTTP sink tests exercise retry/permanent outcomes,
shared timeout, max-events/bytes, zero/overflow limits. Record separator budget
comes from adapter capabilities rather than an HTTP delimiter constant. Webhook
still verifies TLS, refuses redirects, redacts credentials and caps full requests
at1MiB. SQL/MQTT durable/session semantics and bounded memory are retained.

## Actual browser evidence

A separate native headless Chrome profile controlled through its local CDP endpoint
was used because the provided Node Playwright import failed to load its default
export. No installation or existing user browser/profile was used. QA used only a
temporary local Broker, synthetic token/publisher and local HTTP receiver.

Created the Webhook outbound in one editor, directly bound a new SQL rule, saved
both, observed actual MQTT publication->SQL->HTTP and live counters, reopened
credentials as masked [redacted], and ran a matching SQL preview. Desktop1440x1100
and mobile390x844 checked; document and dialog width390 in mobile. Final screenshots
show real QA state, not sample production data. Browser console events captured
during final preview had no errors. Screenshots and synthetic delivery JSON here.

## Recorded test failure and fix

First workspace run failed in a storage reopen test with Locked, caused by a
concurrent crash-helper fork transiently inheriting another test's descriptor.
The preexisting durable test gate did not cover new configuration fixtures. A
deterministic gate regression first failed, then cfg(test)-only shared fixture
isolation fixed it; no retry/sleep or production lock change. Storage36 tests passed
five normal-parallel runs, followed by the full workspace. Initial logs retained.
A temporary shared-build-target metadata clash between worktrees was resolved by
ending concurrent builds and performing final checks sequentially.

## Limits

Only Webhook is implemented. Additional adapters require native typed config/factory
and UI forms, not SQL/scheduler rewrites. Queues/counters remain volatile best-effort.
SIGKILL proves process-crash behavior, not physical power-loss durability. Old API
connector/action fields and Webhook action_id envelope are intentionally replaced.
No existing user Broker was restarted; temporary QA services/browser are cleaned up.

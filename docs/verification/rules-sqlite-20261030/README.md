# SQLite-only rule configuration acceptance — 2026-10-30

Environment: local Darwin arm64, Rust/Cargo1.98.1 stable, Python3.14.7.
Native release: `target/release/rmqtt`. This is not a Linux deployment.

Contract: [Phase5b](../../contracts/phase5b-rules-sqlite.md). Real independent
storage/runtime/quality worktrees were integrated on codex/rules-webhook-integration;
subagents submitted tested commits. Root reviewed and ran integrated checks.

## Results

- cargo fmt --all -- --check: pass.
- cargo clippy --workspace --all-targets --offline -- -D warnings: pass.
- cargo test --workspace --offline: 616 passed including doctests.
- cargo build --release --offline -p rmqtt: pass.
- Real release blackbox: rules15 + Dashboard14 + MQTT persistence14 + WS/WSS18
  + TLS12 + admission/mTLS12 = 85 passed.

Run each blackbox script with RMQTT_BIN set to the absolute release path; corresponding
files are tests/rules_webhook_blackbox.py, dashboard_blackbox.py,
persistence_blackbox.py, websocket_blackbox.py, tls_blackbox.py and
admission_mtls_blackbox.py. Network tests need local loopback listener permission.
Each helper owns isolated data/configuration and ports; existing user broker untouched.

## Verified boundaries

Configuration is pure SQLite: default rmqtt-rules.sqlite, no JSON file compatibility
or import. Existing invalid rules.json is ignored and remains unchanged. JSON
files passed as SQLite paths are rejected without overwriting them. The internal
64KiB configuration blob encoding and HTTP JSON API are unchanged.

Acknowledged PUT survives SIGKILL/restart and subsequently drives real authenticated
Webhook delivery. Storage tests also kill a child before commit and verify rollback.
An external SQLite writer lock causes save failure, running=false and unchanged
committed data; MQTT remains operational and restart restores rules. Cancellation
of mutation/shutdown does not leave the database ownership lock held.

Schema identity, size-before-copy, normal/symlink paths, private permissions,
redaction and static aliases of DB/journal/WAL/SHM/ownership files are tested.

## Limits

SIGKILL tests prove process-crash recovery, not physical power-loss behavior.
Configuration uses a separate database so session durable schema remains unchanged.
Queues/counters remain volatile best-effort; no durable action delivery or
throughput/p99 claim. The cooperative path lock assumes a trusted directory and
does not prevent external SQLite clients or alternative hardlinks from modifying
the DB. Existing historical JSON evidence is retained only as prior-stage evidence.

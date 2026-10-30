# Dashboard verification — 2026-10-30

Local macOS arm64, Rust stable1.98.1, Node24.12.0/npm11.6.2. No remote deployment
or performance/capacity claim. API and real local MQTT tests use the debug build.

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --offline -- -D warnings`: passed.
- `cargo test --workspace --offline`: passed; see `rust.log`.
- `RMQTT_BIN=.../target/debug/rmqtt python3 tests/dashboard_blackbox.py -v`:
  final14/14 passed; see `blackbox.log`.

Independent tests cover MQTT311/5 QoS0/1/2 actual publication/delivery and observed
counters; subscription metadata/options/identifier; retained update/delete/expiry;
clean removal, graceful and ungraceful persistent disconnect, SQLite SIGKILL queued
and QoS1/AwaitComp recovery with packet IDs/DUP/PUBREL correctness. They also test
bearer auth/redaction, URI/query/method/path/body bounds, static caps/symlinks,
64 stalled HTTP connections and MQTT PING liveness, CLI validation and bind failure.

`secret-red.log` preserves the failing regression for a configured authentication
policy hardlinked as a public .js asset. Startup now rejects token and configured
sensitive-file aliases, and only loads static build asset extensions. Token equality
check additionally rejects a copied token asset. TLS key and token aliases pass
negative regression checks.

`first-blackbox.log` preserves the original run: expected sensitive-alias regression
plus two fixture errors. Source comparison to frozen base confirmed MQTT311
persistent sessions already require `--storage-path`; those lifecycle/paging
fixtures now explicitly use SQLite, keeping all protocol/state assertions. Memory
mode remains exercised by live QoS, retained and HTTP tests.

No ACK/outbox persistence change or durable schema migration. Management reads are
eventually consistent bounded metadata scans, not constant-time/no-contention
monitoring; performance impact under high connection counts is not yet measured.

# PostgreSQL access acceptance — 2026-11-02

Source base: e4fb4e9. Worktree: codex/postgres-access. Environment: Debian13 x86_64,
Rust1.99.1, Node24.19.0/npm11.9.0, SQLx0.8.6, disposable PostgreSQL17 Docker.
Native Linux GNU release includes the Dashboard; no changes were pushed remotely.

## Recorded checks

- `cargo fmt --all -- --check`: passed.
- `cargo test --workspace --locked --offline`: 819 passed, 0 failed, 1 ignored.
  The ignored real-PostgreSQL test was run separately with `--include-ignored`:
  `cargo test -p mqtt-auth-postgres --locked --offline -- --include-ignored`:
  5 passed, 0 ignored. Includes exact binary passwords, custom SQL/output aliases,
  absent/duplicate/invalid hashes, parameter binding, ACL matching/priority/deny
  intersection, rule overflow, read-only transaction state, timeout/unavailable,
  and TLS refusal without trust.
- `cargo clippy -p mqtt-auth-http -p mqtt-auth-postgres --all-targets --locked
  --offline -- -D warnings`: passed.
- Whole-workspace strict Clippy is blocked by pre-existing deprecated Atomic
  `fetch_update` calls in session profiling, Kafka owner, transport and egress on
  Rust1.99.1. `cargo clippy --workspace --all-targets --locked --offline --
  -D warnings -A deprecated`: passed. No unrelated source changes or global lint
  allowances were added to silence this baseline issue.
- `npm ci` using the repaired existing lockfile: passed. `npm run build`: passed.
  `npm test`: 117 passed across13 files. Existing Radix use-client bundling warnings
  and jsdom scrollTo notices are unchanged and do not fail checks.
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked --offline`:
  passed (existing dependency compilation deprecation warnings remain).
- `cargo check -p rmqtt --no-default-features --locked --offline`: passed.
  Its dependency tree contains neither mqtt-auth-postgres nor SQLx. Dashboard
  backend availability is supplied by `/api/v1/access-control/status`.
- `cargo build --release --locked --offline -p rmqtt`: passed.
- `tests/postgres_access_blackbox.py -v` against the release and real PostgreSQL:
  5 passed, including TLS enabled with a CA-signed localhost certificate. Tested
  trusted CA/hostname success, mismatched hostname denial and untrusted CA denial.
  TLS test requires `RMQTT_POSTGRES_TEST_CA`; without a TLS fixture CI skips only
  that test. Other tests cover MQTT311/5 credentials and takeover protection,
  CONNECT Will authorization, QoS0/1/2 publish/delivery, mixed SUBSCRIBE decisions,
  PostgreSQL/HTTP composition, secret restoration, connection probes for both
  stages, authorization cache, SQLite configuration recovery and eight updates
  with an idle client.
- Existing `tests/http_access_blackbox.py -v`: 29 passed.
- Existing `tests/auth_acl_blackbox.py -v` volatile mode: 13 passed, 7 SQLite-only
  cases skipped. SQLite mode (`RMQTT_TEST_STORAGE_PATH=enabled`): 20 passed,
  0 skipped.

## Limits

These are finite local correctness/regression checks, not a production load or
availability certification. No musl cross-build was performed for this change.
PostgreSQL is a business-data provider only; read-only queries require a minimally
privileged database role. Configuration secrets remain in private SQLite storage,
not encrypted at rest. SQL templates are trusted administrator input, not a SQL
sandbox. See ../../postgres-auth.md and the Phase6c contract.

The frontend lockfile's existing missing @emnapi dependencies were carried into
this branch as the minimal21-line repair required for reproducible npm ci.

Release artifact: `target/release/rmqtt` (Linux x86_64 GNU, dynamically linked).
SHA256: `f9ff68ac12a77b7ae321e4a11563ee9fd6417494c5b998ea04407b271f5d0ff8`.

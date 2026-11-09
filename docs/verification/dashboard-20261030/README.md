# Dashboard verification — 2026-10-30

Local macOS arm64, Rust stable1.98.1, Node24.12.0/npm11.6.2. No remote deployment
or performance/capacity claim. API and real local MQTT tests use the debug build.

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --offline -- -D warnings`: passed.
- `cargo test --workspace --offline`: passed; see `rust.log`.
- `LYRNORMQ_BIN=.../target/debug/lyrnormq python3 tests/dashboard_blackbox.py -v`:
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

The frontend uses Tailwind v4 and six official shadcn/ui controls rather than the
prototype's large handcrafted CSS. CI now includes independent Node24 test/build
and live Dashboard blackbox checks; hosted GitHub Actions was not run locally.
Browser QA caught a real subscription/retained tab-transition null-page failure
missed by initial tests. The original console/screenshot and then failing deferred
component regression are saved as `topics-transition-red.*` and
`topics-transition-test-red.log`. The owner fixes active-view page guards, without
mock fallback or relaxed assertions, before final browser acceptance.

Final integrated frontend: `npm ci --offline` succeeds from the locked dependency
set; `npm test`30/30 and `npm run build` (strict TypeScript + Vite) pass. Upstream
radix-ui module directives produce build-time Vite warnings, not build failures;
rendered controls/native modal require no inline-style CSP exception. See frontend
logs. Existing TLS12 + WS/WSS18 + admission/mTLS12 methods all pass with no skips
against the final root binary; logs are included.

Real IAB browser acceptance used the production assets served by the actual local
SQLite broker, two connected MQTT5 QA clients, one offline persistent session,
two subscriptions and one retained message. A real publisher sends QoS0 telemetry;
rate values come from its observed wire counters, not benchmark measurements.
Verified1440×1024 desktop and390×844 phone viewports (page widths1440/390), login,
real session search/empty version filter, offline metadata detail, native Escape
close, both topic-tab transitions, retained metadata and live-rate graph. Phone
menu navigation and detail modal fit390px. Logout removes all broker views and
returns an empty token field. Final console capture `browser-console-final.json`
is empty (no JS/CSP error/warn after fixed-bundle reload). Temporary viewport was
reset and the test broker/clients stopped after verification; no server deployment.

Screenshots: `overview-desktop.jpg`, `details-desktop.jpg`, `details-mobile.jpg`.

# Phase6b — scoped HTTP authentication and authorization acceptance

Completed on codex/http-access-basic, 2026-11-02. Reused the existing HTTP backend,
SQLite settings and React management page; supersedes Phase6a's expanded receive,
shared-preflight and remote-recovery scope. Those21 cases are preserved explicitly
as historical, non-active tests, not silently represented as passing.

## Actual behavior

- CONNECT HTTP204 accepts named identity;401 refuses credentials. Failure or
  unavailability cannot take over an existing connection or erase its session.
- PUBLISH and each SUBSCRIBE filter require admission before retained changes,
  routing, persistence or success. Unknown topic aliases fail before an HTTP call;
  resolved aliases authorize the concrete topic. Shared subscription ACL uses the
  existing inner filter, removing $share/group once.
- HTTP403 refuses the current operation; HTTP failure/timeouts/5xx fail closed.
  MQTT5 established operation unavailability uses DISCONNECT89 (ServerBusy),
  pre-CONNECT unavailable uses CONNACK88. No success on unknown permission.
- CONNECT requesting a new Will checks its publish topic once after credentials,
  before CONNACK. This prevents a denied PUBLISH being bypassed with a Will.
  Failed credentials cause zero Will ACL requests. Later Will delivery and startup
  recovery do not query HTTP. ACL-only mode requires named newly admitted users.
- Accepted QoS2 DUP/PUBREL and ACK/control flow, delivery/retained replay/shared
  routing and stored recovery do not repeat HTTP checks. A new QoS1 PUBLISH after
  completion remains a new admission even if DUP=1. Old state is not retroactively
  revoked because an HTTP service fails or changes configuration.
- Exact scoped cache, bounded resources, generation fencing, credential redaction,
  SQLite configuration and draft-only UI probes remain in place. HTTP custom CA,
  certificate and private-key fields remain removed; default HTTPS verification.

Preparation occurs on a cloned model, only for the current operation. Resolve then
rerun unchanged state and seal before side effects, including terminal verdicts.
Installed/pending accepted policies hold compact flags/static fallback, never HTTP
clients/generation slots/attempt budgets. No detached receive/recovery pipeline or
new durable schema was introduced. Local auth mode remains the original fallback.

## Actual tests

- 814 Rust workspace tests; zero failures or ignored tests.
- Strict workspace all-target Clippy, cargo fmt check and diff check passed.
- 114 React tests, TypeScript/Vite production build passed.
- 29 independent real HTTP/MQTT311/5 cases passed in13.863s against immutable
  Release hash c3d8750f19c8511fd498d1886f3a51670d251d7ad2dc4091bc4527cdc01dbdd4.
- 20 original local auth/ACL and25 original PLAIN authentication cases passed in
  SQLite mode (8.601s/5.652s), each suite in its own temporary current directory.
  Legacy fixtures use CWD default settings, so isolated CWD avoids touching or
  sharing a running development Broker's default configuration files.
- Native Release and locked/offline Linux amd64 musl zigbuild passed; ELF inspected
  static with no interpreter/shared dependencies. Linux binary not executed/deployed.

HTTP cases cover credential status distinctions/binary encoding, denial and outage
with all QoS0/1/2 in both versions, mixed SUBACK, retained/fanout preservation,
exact cache0/TTL/clear, stale generation, named persistent ownership/takeover,
no receive/control/recovery RPCs, shared filter bodies and initial Will admission.
All resources/processes are disposable loopback test fixtures.

## Run

```sh
mkdir -p data
./target/release/lyrnormq --listen 0.0.0.0:1883 \
  --admin-listen 127.0.0.1:8766 --admin-token-file /tmp/lyrnormq-admin.token \
  --rules-config data/rules.sqlite --access-control-config data/access.sqlite \
  --storage-path data/sessions.sqlite
```

Use an existing regular token file containing32..256 visible ASCII characters.
Open Dashboard at http://127.0.0.1:8766/#security; configure/enable authentication
and authorization URLs. They default disabled while no endpoint is configured.
For a deliberately plaintext HTTP service, explicitly allow HTTP in advanced
settings. The service returns status only: auth204/401, ACL204/403, no JSON body.
Existing running processes must be restarted to use the newly built binary.

```sh
cargo test --workspace --locked --offline
cargo clippy --workspace --all-targets --locked --offline -- -D warnings
cargo fmt --all -- --check
npm --prefix dashboard/web test
npm --prefix dashboard/web run build
cargo build --release --locked --offline -p lyrnormq
LYRNORMQ_BIN="$PWD/target/release/lyrnormq" python3 tests/http_access_blackbox.py HttpAccessBlackbox -f -v
CARGO_BUILD_JOBS=4 cargo zigbuild --release --locked --offline --target x86_64-unknown-linux-musl -p lyrnormq
python3 scripts/check-static-elf.py target/x86_64-unknown-linux-musl/release/lyrnormq
```

## Limits

HTTP latency remains on the current operation's admission path (default1000ms
service timeout, configurable100..5000ms); exact-key cache can avoid repeated RPC.
This report makes no throughput/P99 claim. No per-recipient or retroactive HTTP
revocation, PostgreSQL backend, custom HTTP TLS identities or new ACL conditions.
Usernames must not be reassigned to another device to inherit old persistent state.
No main merge or remote deployment is part of this phase.

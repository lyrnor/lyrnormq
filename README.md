# rmqtt

Rust 2024 / Tokio TCP MQTT3.1.1 broker under development, supporting QoS0/1/2,
Retained Messages, Last Will and local authentication/ACL, with optional SQLite-backed persistent sessions.
**Full MQTT conformance and production-readiness are not yet claimed.**

```sh
cargo build --workspace
cargo run -p rmqtt -- --listen 127.0.0.1:1883
# Enable durable sessions, retained records and active/pending Wills:
cargo run -p rmqtt -- --storage-path ./sessions.sqlite --max-retained 10000
```

Both modes use the existing bounded SessionEngine/outbox transaction pipeline.
Without SQLite, clean sessions and global retained/Will state are volatile across
process death. With SQLite, Clean Session=false retains subscriptions, offline
QoS1/2 and inflight stages/IDs. Clean Session=true discards the prior session;
retained messages remain independent of that reset. MQTT5, TLS and WebSocket
remain unsupported. Default bind is loopback.

## Authentication and ACL

No policy file keeps open anonymous access for compatibility. With `--auth-policy
policy.json`, unlisted users and actions are denied. CONNECT supports UTF8 usernames
and binary passwords; the local provider verifies Argon2id version19 PHC hashes.
Passwords are redacted in Debug and are never stored in broker snapshots.

Generate a fresh PHC by piping exact password bytes to the owned helper (no newline
trimming). For example, in a shell supporting silent `read`:

```sh
read -r -s MQTT_PASSWORD
printf '%s' "$MQTT_PASSWORD" | cargo run -q -p mqtt-auth --example hash_password
unset MQTT_PASSWORD
```

Paste its output into `password_hash` below; do not store plaintext in the policy:

```json
{
  "allow_anonymous": false,
  "users": [{
    "username": "alice",
    "password_hash": "<generated Argon2id PHC>",
    "client_id_prefix": "alice-",
    "publish": ["tenant/alice/#"],
    "subscribe": ["tenant/alice/#"]
  }]
}
```

```sh
cargo run -p rmqtt -- --auth-policy policy.json --storage-path sessions.sqlite
```

`allow_anonymous:true` additionally needs an `anonymous` object with the same
`client_id_prefix`, `publish` and `subscribe` fields to grant actions; omission
means empty deny lists. Each user must specify all five fields. Prefixes are literal.
Unknown fields, duplicate users, invalid filters and unsupported/unbounded hash
parameters fail startup. Policy file<=1MiB, users<=1024, username<=256 bytes, each
publish/subscribe list<=128. Hash work is bounded to m<=65536KiB,t<=10,p<=4;
the helper generates standard library defaults (m19456,t2,p1).

Publishing uses exact-topic permission; a subscription filter must fit within one
allowed filter. This conservative containment can reject some equivalent or union
coverage. Root wildcards do not grant `$SYS` access; add explicit system filters.
Mixed SUBSCRIBE requests return0x80 only for denied filters. Denied PUBLISH closes
without success acknowledgement; denied connections return CONNACK4/5 before close.
A forbidden Will rejects CONNECT before takeover or registration.

Session and Will records bind a stable identity. Another user cannot take over or
clear that Client ID, including with Clean Session=true. Anonymous clients share
one identity; disable them when distinct ownership is required. Existing schema1/2
records migrate to anonymous, never to the first authenticated claimant. Enabling
an authenticated-only policy may therefore make old anonymous sessions inaccessible;
explicit offline identity conversion/management remains future work. Owner labels
include the username. Migration metadata counts toward --max-state-bytes; a legacy
snapshot near that limit may need additional configured headroom.

Policy is loaded once before listening. After a restart with reduced rights,
unauthorized subscriptions, queued messages and payload-bearing retransmissions are
removed transactionally; Packet IDs are released. Pending inbound QoS2 checks its
original topic again on PUBREL. Already accepted outgoing AwaitComp may finish its
payload-free handshake. Retained reads and Will publication are reauthorized;
revoked Wills are suppressed atomically. `BrokerConfig.access_control` and
`mqtt_auth::AccessControl` support embedded custom bounded policies.

TCP is currently unencrypted: authentication does not provide wire confidentiality.
TLS, remote providers, reload and authentication rate limiting remain unimplemented.
Argon2 runs on the bounded blocking worker; authentication bursts and global ACL
reconciliation can delay other traffic. No security/performance certification is claimed.

## Retained Messages

A retained publication replaces the exact topic's stored payload and QoS. Empty
payload deletes that record while still forwarding the empty live message.
Non-retained publications never alter retained state. Existing subscribers receive
RETAIN=0; an accepted new or identical replacement subscription receives matching
retained records with RETAIN=1 and QoS=min(stored,granted). Wildcards and system-topic
isolation reuse the existing matcher. Multiple filters in one SUBSCRIBE are handled
sequentially and may each replay a copy; ordinary live routing remains deduplicated.
Incoming retained QoS2 changes state only on PUBREL, atomically with source completion
and recipient outboxes. Retained replay flags survive outgoing retries/restarts.

## Last Will

CONNECT accepts binary Will payloads at QoS0/1/2, with optional retain. A normal
DISCONNECT commits cancellation before closing. EOF/RST, protocol error, Keep Alive
timeout and successful connection takeover publish the old Will once through the
same outbox path. Replacement registration/cancellation and old-Will effects share
a transaction; stale connection cleanup cannot fire the replacement's Will.
A retained Will updates/deletes retained state using the same empty-payload rule.

Capacity-blocked Wills remain bounded pending intents and retry after progress,
without partial delivery or silent loss. A same-ID CONNECT carrying a new Will is
rejected while the old one cannot drain; reconnecting with no new Will can resume
the session without overwriting the pending intent, including draining a full
self-subscription outbox. Graceful close of that new connection preserves the old
pending intent. SQLite stores accepted Wills even for clean-session origins, and
recovers/publishes them on restart before serving clients. Without SQLite, process
failure loses volatile state. Server shutdown closes connections as unexpected
ends unless their DISCONNECT was already committed.

## Persistence and limits

SQLite uses WAL/FULL and schema3 JSON whole-registry transactions. Valid schema1/2
snapshots are explicitly migrated, preserving IDs/order/subscriptions; the opaque
SQLite container remains version1. Unknown/corrupt schemas or incompatible lowered
quotas reject startup instead of dropping data. Atomic state/outbox commits precede
ACKs and first outgoing transmission. QoS2 AwaitComp reconnects retransmit PUBREL,
never PUBLISH; other unresolved publishes keep their IDs and set DUP.

| CLI flag | Default | Meaning |
| --- | --- | --- |
| `--auth-policy` | disabled | Bounded JSON authentication/ACL policy; invalid policy fails startup |
| `--listen` | `127.0.0.1:1883` | TCP bind address |
| `--storage-path` | disabled | SQLite path; parent must exist |
| `--max-packet-size` | 1048576 | Maximum complete packet bytes |
| `--max-connections` | 1024 | Accepted concurrent connections |
| `--max-sessions` | 1024 | Online/offline session count and bounded Will records |
| `--outbound-capacity` | 64 | Per-session pending outbox / per-connection write queue |
| `--max-subscriptions` | 256 | Distinct filters per session |
| `--max-inflight` | 32 | Each incoming QoS2 and outgoing QoS1/2 window |
| `--max-retained` | 10000 | Global retained topic count |
| `--max-state-bytes` | 67108864 | Serialized live registry and durable snapshot limit |
| `--handshake-timeout-ms` | 10000 | CONNECT deadline |
| `--write-timeout-ms` | 5000 | Per-frame write deadline |

All numeric limits are positive; packet size is at least2 and inflight<=65535.
`--help` and BrokerConfig expose the controls. Count and logical-byte limits do not
constitute an aggregate process-memory or disk budget. Full retained replay/outboxes
abort a proposal without a success ACK; pending Will publication waits for capacity.

A permanent canonical-path `.broker-lock` sidecar holds a lifetime OS lock; it is
not an existence lock and is not deleted. A second cooperating broker is rejected.
Existing symlinks share the canonical lock; dangling symlinks are rejected. Hard-link
aliases, externally replacing/renaming/modifying an active DB are unsupported.

Whole-registry cloning/JSON and per-proposal routing-index rebuilding have costs
that are not benchmarked. Worker transaction waits can delay controls; operational
fault/stress testing, disk quotas, migrations beyond schema1/2->3, backup/admin and
observability remain work. SIGKILL and SQLITE_FULL tests do not establish physical
power-loss behavior or application-side exactly-once processing.

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
python3 tests/blackbox.py
python3 tests/qos_blackbox.py
python3 tests/auth_acl_blackbox.py
python3 tests/auth_acl_paho.py
python3 tests/persistence_blackbox.py
python3 tests/retained_will_blackbox.py
python3 -m venv /tmp/rmqtt-client-venv
/tmp/rmqtt-client-venv/bin/pip install paho-mqtt==2.1.0
/tmp/rmqtt-client-venv/bin/python tests/paho_interop.py
/tmp/rmqtt-client-venv/bin/python tests/persistence_paho.py
/tmp/rmqtt-client-venv/bin/python tests/retained_will_paho.py
```

Set `RMQTT_TEST_STORAGE_PATH` to a fresh temporary SQLite path to run the reused
basic/QoS/Retained/Will/Paho suites in SQLite mode. Tests needing process persistence
are explicitly skipped in volatile mode; CI runs both. Each lib.rs contains only
documentation, module declarations and re-exports.

See [architecture](docs/architecture.md), [current contracts](docs/contracts/phase2d.md),
[actual results](docs/progress.md) and [remaining dependencies](docs/tasks.md).

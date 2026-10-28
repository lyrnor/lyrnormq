# rmqtt

Rust 2024 / Tokio TCP MQTT3.1.1 broker under development, supporting QoS0/1/2,
Retained Messages and Last Will, with optional SQLite-backed persistent sessions.
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
retained messages remain independent of that reset. MQTT5, credentials/auth/ACL,
TLS and WebSocket remain unsupported. Default bind is loopback.

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

SQLite uses WAL/FULL and schema2 JSON whole-registry transactions. Valid schema1
snapshots are explicitly migrated, preserving IDs/order/subscriptions; the opaque
SQLite container remains version1. Unknown/corrupt schemas or incompatible lowered
quotas reject startup instead of dropping data. Atomic state/outbox commits precede
ACKs and first outgoing transmission. QoS2 AwaitComp reconnects retransmit PUBREL,
never PUBLISH; other unresolved publishes keep their IDs and set DUP.

| CLI flag | Default | Meaning |
| --- | --- | --- |
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
fault/stress testing, disk quotas, migrations beyond schema1->2, backup/admin and
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

See [architecture](docs/architecture.md), [current contracts](docs/contracts/phase2c.md),
[actual results](docs/progress.md) and [remaining dependencies](docs/tasks.md).

# rmqtt

Rust 2024 / Tokio MQTT broker under development. Supports MQTT 3.1.1 live
**QoS 0/1/2** over TCP, with optional SQLite-backed persistent sessions. **Not a fully conformant MQTT broker yet.**

```sh
cargo build --workspace
cargo run -p rmqtt -- --listen 127.0.0.1:1883 --max-inflight 32
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Supports CONNECT (persistent sessions require storage), publish/subscribe/unsubscribe, MQTT wildcards,
maximum matching subscription QoS, PING, disconnect, Keep Alive and Client ID
takeover. Incoming QoS2 is routed once on PUBREL; outgoing QoS2 releases its Packet
ID only at PUBCOMP. Full inflight windows pause delivery while ACK reads continue.
Saturated bounded delivery queues disconnect slow clients.

MQTT5, retained publishes, Will and credentials remain unsupported and are
explicitly rejected. There is no TLS or authentication yet; default binding is
loopback. Without --storage-path, sessions remain clean-only and volatile. With
storage enabled, Clean Session=false preserves state and replays it on reconnect.
This does not guarantee application-side exactly-once processing; see Phase2b below.

Independent client checks (after build):

```sh
python3 tests/blackbox.py
python3 tests/qos_blackbox.py
python3 -m venv /tmp/rmqtt-client-venv
/tmp/rmqtt-client-venv/bin/pip install paho-mqtt==2.1.0
/tmp/rmqtt-client-venv/bin/python tests/paho_interop.py
```

Run `target/debug/rmqtt --help` for resource limits. Defaults:

| CLI flag | Default | Meaning |
| --- | --- | --- |
| `--listen` | `127.0.0.1:1883` | TCP bind address |
| `--max-packet-size` | 1048576 | Maximum complete MQTT packet bytes |
| `--max-connections` | 1024 | Concurrent accepted connections |
| `--outbound-capacity` | 64 | Pending delivery packets per connection |
| `--max-subscriptions` | 256 | Distinct filters per connection |
| `--max-inflight` | 32 | Each of inbound QoS2 and outbound QoS1/2 windows |
| `--handshake-timeout-ms` | 10000 | CONNECT deadline |
| `--write-timeout-ms` | 5000 | Maximum time per socket write |
| `--storage-path` | disabled | SQLite DB path for persistent sessions |
| `--max-sessions` | 1024 | Online + offline session records in durable mode |
| `--max-state-bytes` | 67108864 | Serialized live state and durable snapshot limit |

All numeric limits must be positive; packet size must be at least 2 and inflight
must be at most 65535. `mqtt_transport::BrokerConfig` exposes the same controls for
embedding. Limits bound allocations but are not an aggregate process-memory
budget; tune packet, queue and connection limits together. In the legacy clean-only path an active write can defer shutdown/takeover/Keep
Alive processing until its write timeout. The durable path has a separate bounded
writer which is cancelled on takeover/shutdown; broad slow-consumer stress testing
remains pending.

See [architecture](docs/architecture.md), [current contract](docs/contracts/phase2b.md),
[progress and actual checks](docs/progress.md), and [remaining task dependencies](docs/tasks.md).

## Phase2b persistent sessions

To enable MQTT3.1.1 Clean Session=false, supply an SQLite path whose parent exists:

```sh
cargo run -p rmqtt -- --listen 127.0.0.1:1883 \
  --storage-path ./sessions.sqlite --max-sessions 1024 --max-state-bytes 67108864
```

The SQLite-enabled path retains subscriptions, bounded offline QoS1/2 queues,
incoming QoS2 records and outgoing QoS IDs/stages. Successful MQTT responses follow
a committed state/outbox transaction. Reconnect retransmits unacknowledged PUBLISH
with its original ID and DUP; after PUBREC it retransmits PUBREL instead. Clean
Session=true resets the previous session and remains volatile. Without storage,
the original clean-session-only path remains available.

`--max-sessions` counts both online and offline session records (default1024).
`--max-state-bytes` bounds the serialized live registry, including clean sessions,
and loaded durable snapshots (default64MiB). `--outbound-capacity` also bounds each
session's queued outbox; an overflowing durable recipient aborts the publishing
transaction without a success ACK. Lowering limits below existing records rejects
startup rather than dropping them. MQTT5 session expiry is not implemented.

SQLite uses WAL/FULL and one schema1 whole-registry JSON snapshot per committed
change. A canonical-path `.broker-lock` sidecar holds a lifetime OS lock and must
remain in place; it is not an existence-based lockfile. A second cooperating broker
is rejected. Existing symlinks share the canonical lock; dangling symlinks are
rejected. Hard-link aliases, externally renaming/replacing the active database,
and external modification of an active database are unsupported. Database and
session schema corruption is an error, never an automatic reset.

Whole-registry cloning, JSON serialization and per-proposal trie rebuilding are
correctness-first tradeoffs with substantial potential write/CPU amplification.
Process-crash recovery tests do not establish physical power-loss behavior or
application-side exactly-once processing. No durability performance baseline is
claimed. Additional checks:

```sh
python3 tests/persistence_blackbox.py
/tmp/rmqtt-client-venv/bin/python tests/persistence_paho.py
```

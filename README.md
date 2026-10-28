# rmqtt

Rust 2024 / Tokio MQTT broker under development. Supports MQTT 3.1.1 live
**clean-session QoS 0/1/2** over TCP. **Not a fully conformant MQTT broker yet.**

```sh
cargo build --workspace
cargo run -p rmqtt -- --listen 127.0.0.1:1883 --max-inflight 32
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Supports clean-session CONNECT, publish/subscribe/unsubscribe, MQTT wildcards,
maximum matching subscription QoS, PING, disconnect, Keep Alive and Client ID
takeover. Incoming QoS2 is routed once on PUBREL; outgoing QoS2 releases its Packet
ID only at PUBCOMP. Full inflight windows pause delivery while ACK reads continue.
Saturated bounded delivery queues disconnect slow clients.

MQTT5, persistent sessions, reconnect recovery, offline queues, retained publishes,
Will and credentials remain unsupported and are explicitly rejected. There is no
TLS or authentication yet; default binding is loopback. QoS state is volatile and
is discarded on disconnect. This does not provide durable delivery or a
crash-safe application exactly-once guarantee. Storage memory transactions are
independent groundwork, not runtime persistence.

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

All numeric limits must be positive; packet size must be at least 2 and inflight
must be at most 65535. `mqtt_transport::BrokerConfig` exposes the same controls for
embedding. Limits bound allocations but are not an aggregate process-memory
budget; tune packet, queue and connection limits together. An active socket write
can defer shutdown/takeover/Keep Alive processing until its write timeout; targeted
slow-reader validation and improved interruptibility remain pending.

See [architecture](docs/architecture.md), [current contract](docs/contracts/phase2a.md),
[progress and actual checks](docs/progress.md), and [remaining task dependencies](docs/tasks.md).

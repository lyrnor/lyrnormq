# rmqtt

Rust 2024 / Tokio MQTT broker under development. Phase 1 targets a bounded,
TCP-only MQTT 3.1.1 QoS 0 subset. **Not a fully conformant MQTT broker yet.**

```sh
cargo build --workspace
cargo run -p rmqtt -- --listen 127.0.0.1:1883
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Supports clean-session CONNECT, QoS 0 publish, subscribe (grants QoS 0), unsubscribe,
MQTT wildcards, PING, disconnect, keepalive and client ID takeover. Requests for
persistent sessions, Will, credentials, retained publishes and QoS 1/2 publishes
are explicitly unsupported. MQTT 5 is rejected. No TLS or authentication yet;
default binding is loopback. Storage/session crates are independent groundwork,
not broker runtime persistence.

See [architecture](docs/architecture.md), [contracts](docs/contracts/phase1.md),
and [progress](docs/progress.md) for implementation boundaries and next work.

Independent client checks (after `cargo build --workspace`):

```sh
python3 tests/blackbox.py
python3 -m venv /tmp/rmqtt-client-venv
/tmp/rmqtt-client-venv/bin/pip install paho-mqtt==2.1.0
/tmp/rmqtt-client-venv/bin/python tests/paho_interop.py
```

`mqtt_transport::BrokerConfig` exposes packet, connection, subscription and
outgoing queue limits plus handshake/write timeouts for embedding applications.
The binary currently uses defaults; it only exposes the bind address on the CLI.
A saturated outgoing queue disconnects the slow client. QoS 0 provides no retry
or durable delivery guarantee. Configuration files, TLS, WebSocket and cluster
operation remain future work.

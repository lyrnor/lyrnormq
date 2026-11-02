# Independent real Kafka acceptance client

This standalone Rust admin/consumer helper talks to an actual Apache Kafka KRaft
process. It does not replace the production adapter or use a mocked Broker. The
helper and fixture are test tools only; point them only at disposable test clusters.

```sh
python3 tests/kafka_fixture.py --download
CARGO_TARGET_DIR="$PWD/target" CARGO_BUILD_JOBS=4 cargo build --release --locked --manifest-path tests/kafka-client/Cargo.toml
RMQTT_BIN="$PWD/target/release/rmqtt" \
KAFKA_CLIENT_BIN="$PWD/target/release/rmqtt-kafka-test-client" \
python3 tests/kafka_outbound_blackbox.py -v
```

The download command uses official Adoptium and Apache URLs and verifies SHA256 /
SHA512 before extracting into an isolated temporary tools directory. It installs no
system Java or Kafka. Cached release metadata and complete archives are reused.
Override `RMQTT_KAFKA_TOOLS_DIR` if needed; or supply `JAVA_HOME` and `KAFKA_HOME`
to already prepared test distributions. Downloads use bounded retries and can
resume partial archives. A failed checksum is an error, not a skipped test.

The fixture has one Kafka broker/controller, 256 MiB maximum heap, three partitions
per test topic, replication factor one, min ISR one, explicit topic creation, and
per-message log flush. It exposes ephemeral loopback PLAINTEXT, TLS and SASL/TLS
listeners, plus a separate mandatory-client-certificate TLS listener. A generated test CA and PKCS12 keystore are used; PLAIN, SCRAM-SHA-256
and SCRAM-SHA-512 users are disposable fixture credentials. The test process is
stopped and its data directory removed. This does not certify multi-replica Kafka
availability or physical power-loss guarantees.

Each helper `consume` call assigns all partitions at the beginning independently
of consumer group offsets, reads at most 10,000 records / 16 MiB aggregate payload,
and has a finite deadline. It returns parsed value/key/headers/partition/offset/
timestamp for assertions. Kafka buffers are bounded. The helper verifies metadata
and authentication even when asked to read zero records. Configuration files are
0600 and removed after use; do not pass real credentials on the command line.

The acceptance suite covers dual MQTT versions and QoS0/1/2, same-client keyed
partition FIFO, multiple clients, raw projected JSON and stable identity headers,
multiple rules/outbounds, protocol budget isolation from held Webhooks, Kafka
outage/retry, normal and SIGKILL restart with pending records, partial-progress
recovery with allowed stable-ID duplicates, security modes and wrong credentials/
CA, encrypted client-key mTLS/redaction and missing-certificate rejection, key-field terminal failure and configuration atomicity. The precise millisecond
Kafka-success/local-completion fault window is supplemented by backend fault tests;
the blackbox partial-progress kill does not claim it always hits that exact window.

```sh
CARGO_TARGET_DIR="$PWD/target" cargo fmt --manifest-path tests/kafka-client/Cargo.toml -- --check
CARGO_TARGET_DIR="$PWD/target" cargo clippy --offline --manifest-path tests/kafka-client/Cargo.toml --all-targets -- -D warnings
CARGO_TARGET_DIR="$PWD/target" cargo test --offline --manifest-path tests/kafka-client/Cargo.toml
```

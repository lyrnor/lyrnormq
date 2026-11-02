# Direct Kafka adapter before/after benchmark

This finite writer calls the actual public `mqtt_kafka::KafkaSink` API. The Python
runner starts the existing checksum-verified real Apache Kafka fixture, creates a
fresh topic per scenario, samples writer-process RSS, and independently reads the
records with the existing **rdkafka** admin/consumer helper. The writer does not
import rdkafka directly. Do not point these tools at a production cluster.

```sh
# Build the same source with each adapter revision; save each executable before
# checking out/integrating the next revision. Its standalone lock must be updated
# whenever the production adapter dependency changes, then committed.
CARGO_TARGET_DIR="$PWD/target" cargo build --release --locked \
  --manifest-path tests/kafka-bench/Cargo.toml
cp target/release/rmqtt-kafka-adapter-bench /tmp/rmqtt-adapter-before
# Integrate the other adapter; update this standalone lock and build again.
# Save the new executable as /tmp/rmqtt-adapter-after.

CARGO_TARGET_DIR="$PWD/target" cargo build --release --locked \
  --manifest-path tests/kafka-client/Cargo.toml
python3 tests/kafka_fixture.py --download
python3 tests/kafka_adapter_benchmark.py \
  --writer /tmp/rmqtt-adapter-before \
  --reader "$PWD/target/release/rmqtt-kafka-test-client" \
  --warmup 64 --label rdkafka-before --output /tmp/rdkafka-before.json
python3 tests/kafka_adapter_benchmark.py \
  --writer /tmp/rmqtt-adapter-after \
  --reader "$PWD/target/release/rmqtt-kafka-test-client" \
  --warmup 64 --label krafka-after --output /tmp/krafka-after.json
```

Run before/after **sequentially**, with no other benchmark, compilation or Kafka
acceptance suite running. The tool requires permission to bind loopback ports,
start disposable Java processes and inspect the child writer PID (`ps`). It
installs no system services. Fixture configuration is fixed by `kafka_fixture.py`:
one local Kafka 4.2.2 broker/controller, RF=1, min ISR=1, forced per-message log
flush, and 128..256 MiB Java heap. These measurements are not a production Kafka
capacity claim or an end-to-end MQTT Broker benchmark.

Default matrix: 64 concurrent clients, 64 messages per client (4096 total), exact
128/1024-byte projected JSON values, and one hotspot partition versus 12 partitions
with keyed distribution. One request per client remains outstanding until its
final send outcome, matching the existing same-client scheduler. Compression is
LZ4, linger 1 ms, batch size 64 KiB, per-record policy timeout 30 s, and Tokio uses
four workers. The CLI defaults to zero warmup for explicit cold-session measurement.
Use `--warmup 64` on **both** revisions for a warmed-session comparison: the same
sink confirms 64 records on the same topic before the timed barrier. Warmup failures
abort the run; IDs use a separate `warmup-` prefix, and independent readback verifies
warmup records separately. They are excluded from timed throughput/latency counts.
The JSON report includes exact warmup count and elapsed time. Do not compare a cold
run to a warmed run or interpret cold-session p99 as warmed steady-state latency.
The runner has a 180-second per-scenario watchdog and finite message, byte,
partition and process bounds. All processes/files are cleaned up on normal/error
exit. No rules, source bodies or production configurations are modified.

## Metric boundaries

- Throughput: confirmed records divided by wall time from releasing the client
  barrier until every send has a final outcome. Excludes adapter construction,
  explicit warmup, shutdown, topic creation and independent readback; includes payload generation
  and any SDK connection work remaining during the workload.
- Latency: `KafkaSink::send` entry to its confirmed delivery result, including
  adapter parsing, SDK buffering/retries and connection setup encountered there.
  Event generation is outside each latency sample. p50/p95/p99 use nearest rank.
  Only confirmed calls contribute samples; attempted/confirmed/error counts are
  reported separately, and the runner fails if any send fails.
- RSS: writer PID's resident memory (`ps rss`, KiB converted to bytes) sampled
  every 50 ms. This is a sampled peak, **not** an exact high-water mark. Kafka JVM,
  Python runner and independent consumer memory are excluded. The sampled peak
  includes writer startup, optional warmup and shutdown, so it is not steady-only RSS. Sampling failures
  and successful sample counts are included; zero successful samples fail.
- Readback: outside the timing window. A two-second drain asks for one more than
  the expected count to detect no-fault duplicates. Exact counts/unique stable
  IDs, raw value size/content, keys, source headers/timestamp, partition offsets,
  per-client FIFO and consistent keyed partition selection are verified. This
  proves no-fault workload completion, not a physical power-loss guarantee.

The reader returns at most 10,000 records / 16 MiB raw payload, while this runner
uses at most 9999 records including warmup / 12 MiB projected JSON. Default raw payload is 0.5/4 MiB.
JSON objects are generated before each latency timer and contain numeric client
and sequence fields plus deterministic padding to reach the exact payload size.

```sh
python3 tests/kafka_adapter_benchmark_test.py -v
cargo fmt --manifest-path tests/kafka-bench/Cargo.toml -- --check
cargo test --locked --manifest-path tests/kafka-bench/Cargo.toml
cargo clippy --locked --manifest-path tests/kafka-bench/Cargo.toml \
  --all-targets -- -D warnings
```

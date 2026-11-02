# krafka replacement — actual acceptance, 2026-11-02

Replaced production rdkafka0.39/librdkafka2.12.1 with pinned krafka0.28.1. Public
configuration, durable result journal, stable IDs and per-client scheduling remain
unchanged. Uses rustls/ring, explicit CA or native roots, TLS/mTLS, bounded encrypted
PKCS8, SASL PLAIN/SCRAM256/SCRAM512, acks=All and idempotent delivery.

## Results

- Integrated workspace: 801 Rust tests passed, no failures or ignored tests.
- Strict workspace all-target Clippy, cargo fmt and diff check passed.
- 15 existing/extended real Apache Kafka blackboxes passed in39.287s; one further
  real codec/configuration-replacement test passed in7.419s (16 cases total).
- Real tests cover MQTT311/5 QoS0/1/2, raw SQL JSON, stable headers/IDs, keyed FIFO,
  empty/Unicode Murmur2 compatibility against independent rdkafka, TLS/mTLS with
  encrypted keys, PLAIN/SCRAM, wrong credentials/CA, independent HTTP capacity,
  outage retries, graceful and SIGKILL recovery, shared result bodies and atomic
  configuration validation. Codec test checks actual on-disk Kafka compression
  identifiers0/3/4 as well as independent none/LZ4/Zstd readback.
- 23 adapter unit/fault tests include TCP fake-broker ACK-loss deduplication,
  unknown outcome socket fence/reconnect, caller cancellation retaining capacity,
  concurrent close, bounded cleanup-overrun and strict TLS validation. A fake
  broker fault test is not represented as real Apache Kafka interoperability.
- Locked offline Linux amd64 musl zigbuild passed; ELF has no interpreter or
  needed shared libraries. This Linux artifact was inspected, not executed/deployed.
- Production dependency tree has no rdkafka,rdkafka-sys,openssl-sys,openssl-src or
  curl-sys. openssl-probe is only native certificate-path discovery, not OpenSSL.
  The separate test-reader intentionally retains rdkafka for independent checks.

## Comparable before/after measurements

Local macOS27 arm64 Mac16,12,10logical cores,16GiB RAM; JRE21.0.13+9, Apache
Kafka4.2.2, one local broker/controller, RF1/minISR1, forced per-message flush.
Heap128..512MiB, cleaner dedupe16MiB, offsets topic1partition. No container or
production server was used. Java and the independent reader are excluded from
writer RSS. Both SDKs use the same fixture and exact benchmark source/workload.

64 clients, one outstanding record per client,64records/client (4096/case),
64 separately verified warmup records on the same Sink/topic before timing, LZ4,
linger1ms,batch65536B, idempotent acks=all. Three alternating paired runs. Each SDK
confirmed49152formal records plus768warmup records, every record independently
read back with exact payload/key/header/ID/count/FIFO checks; zero errors.

Throughput is confirmed direct-adapter calls per second, not MQTT end-to-end TPS.
Latency runs from Sink::send entry through final confirmed response; it includes
parsing/buffering/retries, excludes event construction, warmup and readback. Table
values are medians of three run metrics; the P99 column is median of per-run P99,
not a percentile pooled across runs. Full ranges are in performance-summary.json.
RSS is a50ms sampled whole-writer-lifetime peak (startup/warmup/shutdown included),
not an exact high-water mark or steady-state resident measurement.

| Partitions | JSON bytes | TPS before | TPS after | Change | P99 before ms | P99 after ms | RSS before/after MiB |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 128 | 10811 | 8623 | -20.2% | 17.63 | 23.72 | 12.55/9.97 |
| 1 | 1024 | 8181 | 7861 | -3.9% | 11.00 | 18.26 | 12.95/10.33 |
| 12 | 128 | 1308 | 1260 | -3.7% | 161.86 | 63.75 | 12.73/9.81 |
| 12 | 1024 | 1359 | 1201 | -11.6% | 145.76 | 95.74 | 13.00/10.14 |

This is a short finite local flush-heavy comparison. It shows lower memory and
binary size, lower TPS in these four cases, improved multi-partition P99 and
worse single-partition median-run P99. It does not establish production capacity,
global tail guarantees or causally attribute the differences. krafka permits one
batch in flight per partition, a candidate for future profiling rather than a
proven explanation of all measured changes. No performance improvement is claimed.

## Production artifact size

| Target | Before bytes | After bytes | Before/after MiB |
|---|---:|---:|---:|
| macOS arm64 | 26226832 | 22289568 | 25.01/21.26 |
| Linux amd64 musl | 24153128 | 19343608 | 23.03/18.45 |

Before/after use the same release profile and embedded Dashboard assets. Binary
hashes, compiler and environment are recorded in environment.json. Existing
uncommitted HTTP management work was preserved and present identically in the
working-source before/after Broker builds. Its root dependency edges are excluded
from the Kafka-only staged lockfile and validated in an isolated integration tree.

## Defects found and fixed

- SDK producer Drop can continue background delivery: keep all SDK tasks inside
  a private owner runtime and fence runtime before uncertain retry replies.
- Waiting for runtime teardown could hang a caller: policy deadline plus5s cleanup
  bounds the reply; overrun stops the owner while retaining real resource ownership.
- Preserve512records/8MiB admission across queued,active and canceled callers;
  suppress raw SDK diagnostics on owner and blocking workers, retain typed errors.
- Reject malformed/mismatched TLS and expensive encrypted-key derivation offline.
- Existing logging tests could create the same timestamp-based directory in two
  threads; add a process-local atomic suffix (production logging unchanged).
- Full real15-case ordering exposed Kafka fixture heap exhaustion on restart after
  creating internal offsets partitions. Focused8-topic+coordinator+restart RED
  reproduced the JVM OOM; bounded heap/cleaner/internal-partition settings made it
  GREEN without changing delivery assertions. All comparable performance runs use
  the corrected fixture; earlier diagnostic runs are not used in the table.

## Reproduce

```sh
npm --prefix dashboard/web ci
npm --prefix dashboard/web run build
cargo build --release --locked -p rmqtt
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
CARGO_BUILD_JOBS=4 cargo zigbuild --release --locked --target x86_64-unknown-linux-musl -p rmqtt
python3 scripts/check-static-elf.py target/x86_64-unknown-linux-musl/release/rmqtt
CARGO_TARGET_DIR="$PWD/target" cargo build --release --locked --manifest-path tests/kafka-client/Cargo.toml
RUST_LOG=warn,rmqtt=info RMQTT_LOG_FORMAT=text RMQTT_BIN="$PWD/target/release/rmqtt" KAFKA_CLIENT_BIN="$PWD/target/release/rmqtt-kafka-test-client" python3 tests/kafka_outbound_blackbox.py -v
CARGO_TARGET_DIR="$PWD/target/krafka-quality" cargo build --release --locked --manifest-path tests/kafka-bench/Cargo.toml
python3 tests/kafka_adapter_benchmark.py --writer "$PWD/target/krafka-quality/release/rmqtt-kafka-adapter-bench" --reader "$PWD/target/release/rmqtt-kafka-test-client" --warmup 64 --label krafka --output /tmp/krafka-result.json
```

Run performance cases without simultaneous compilation or other Kafka tests.
Existing test distributions can be reused; otherwise tests/kafka_fixture.py
--download obtains official checksum-verified Kafka/JRE archives. Older immutable
baseline helper/binaries remain under target/krafka-baseline and target/krafka-quality.

## Remaining limits

Linux execution/deployment, multi-replica Kafka failover, prolonged load/soak and
MQTT end-to-end performance are not verified here. Traditional encrypted PEM
formats are explicitly unsupported; encrypted PKCS8 KDF costs are bounded.
At-least-once crash recovery can replay a Kafka-committed record; stable delivery
headers support downstream deduplication but do not create cross-restart exactly-once.
Uninterruptible OS DNS/native-root/compression work may outlive cleanup timeout;
the stopped owner retains its resources and blocks replacement until actual teardown.
No cluster, consumer group, transaction or schema-registry functionality was added.

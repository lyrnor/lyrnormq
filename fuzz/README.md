# Bounded fuzz campaigns

This standalone workspace leaves the production workspace on stable Rust. Run:

```sh
cargo fmt --manifest-path fuzz/Cargo.toml -- --check
cargo clippy --manifest-path fuzz/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path fuzz/Cargo.toml
cargo run --release --locked --manifest-path fuzz/Cargo.toml -- 20000 332593189941
cargo install cargo-fuzz --version 0.13.2 --locked
mkdir -p /tmp/rmqtt-fuzz-artifacts
cp -R fuzz/corpus /tmp/rmqtt-codec-corpus
cp -R fuzz/corpus /tmp/rmqtt-session-corpus
ASAN_OPTIONS=quarantine_size_mb=64 cargo +nightly fuzz run --fuzz-dir fuzz/libfuzzer codec /tmp/rmqtt-codec-corpus -- -max_total_time=30 -max_len=4096 -timeout=2 -rss_limit_mb=512 -seed=20261029 -artifact_prefix=/tmp/rmqtt-fuzz-artifacts/codec-
ASAN_OPTIONS=quarantine_size_mb=64 cargo +nightly fuzz run --fuzz-dir fuzz/libfuzzer session /tmp/rmqtt-session-corpus -- -max_total_time=30 -max_len=4096 -timeout=2 -rss_limit_mb=512 -seed=20261030 -artifact_prefix=/tmp/rmqtt-fuzz-artifacts/session-
```

The guided campaigns require nightly; CI pins nightly-2026-10-28. Keep generated corpora outside the repository. `seed-corpus DIRECTORY` regenerates the checked-in deterministic hand seeds, which include both versions' control packets, all supported packet types, valid QoS traces and durable snapshots. Mutation includes truncation, insertion, replacement and bit changes. The stable campaign is deterministic seeded mutation, separate from the actual libFuzzer campaigns.

`codec` checks bounded decoding, deterministic results, exact first-frame consumption including concatenated frames, incremental truncations, and semantic encode/decode round trips. Legal noncanonical encodings need not preserve wire bytes. It probes unknown/3.1.1/5 protocol context and limits of 2, 16, 256 and 4096 bytes.

`session` runs both legacy packet and MQTT5 frame traces against bounded engines and a controlled clock. The MQTT5 branch covers subscription options/identifiers, mixed RAP, Message Expiry, Receive Maximum, QoS transitions, reconnect and Session Expiry. Its hand trace must actually start two QoS2 mixed-RAP deliveries. Failed proposals are discarded. Accepted state must survive durable validation and a restore/snapshot fixed point after the initial normalization. Session traces are limited to 64 legacy and 32 MQTT5 operations; input is capped at 4096 bytes, with four sessions/inflight entries and eight queued/retained entries. These model oracles do not implement network Topic Alias state, an authentication provider, Keep Alive scheduling or arbitrary concurrent actor interleavings; independent wire tests cover those paths.

Phase 2o local measurements (Apple silicon/macOS, ASAN quarantine 64 MB): stable mutation passed 20,000 runs, seed 332593189941, 1,146,707 input bytes. Guided codec completed 2,528,551 executions in 31 seconds (cov 1953, features 4674, RSS 195 MB); strengthened legacy+MQTT5 session completed 2,261 executions in 31 seconds (cov 5201, features 29991, RSS 170 MB). No crash was found. These are finite campaigns and instrumentation counters, not protocol-coverage percentages, exhaustive verification or a performance benchmark. Codec measurement used the unchanged codec oracle before the additional MQTT5 state oracle was added.

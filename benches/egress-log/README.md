# Direct durable journal benchmark

This standalone package calls the production `mqtt-egress-log::Journal` directly.
It does not alter persistence, disable synchronization, launch a Broker, or include
HTTP/network time. Use an optimized build and record hardware, filesystem, free
space and the engine commit alongside the JSON.

```sh
CARGO_TARGET_DIR="$PWD/target" cargo build --release --offline \
  --manifest-path benches/egress-log/Cargo.toml
/usr/bin/time -l target/release/egress-log-bench \
  --queue-dir /private/tmp --phase io --records 65536 --batch 64 \
  --payload-bytes 1024 > result.json 2> resource.txt
```

`--queue-dir` is a **parent directory**. Each process exclusively creates a unique
private child and cleans up that child after the journal closes. Existing queues
are never opened/deleted. `--keep` retains only this generated child for inspection.
Do not use production queues or consume all free disk space.

Defaults: 8,192 records, 128-byte payload, batch1, 1MiB target segments, 512MiB active
queue budget, 200,000 maximum pending records and one read round. CLI validation
bounds work, sample counts, bytes and dataset capacity. A group is at most64 records /
8MiB payload. No unbounded payload preload is performed; a single deterministic
payload template is cloned into bounded append groups. The queue maintains its
normal metadata indexes and disk payloads.

## Timing and topology

- `io` (default): durable append, warm reads and indexed head metadata. `append`,
  `read`, `heads` select only the respective measured output; read/head setup still
  appends the dataset outside their measured stages.
- Record construction is outside the `append_batch` API latency timer, but inside
  append wall throughput. Append returns only after production `sync_all` succeeds.
  Automatic compaction/rotation, if triggered, is included in call latency.
- By default every append batch has a unique Client ID. Thus each head identifies
  one full batch, and read-only traversal verifies **all** records without mixing
  durable completion writes into read time. `--clients N` can reuse clients; in that
  mode only each client's earliest batch is read, and JSON explicitly reports
  `unique_records_verified`. `--read-rounds N` repeats these same heads.
- Read API time includes production file open/stat/seek/read/allocation and payload
  CRC validation. The extra driver equality/client validation is outside the API
  timer but inside read wall throughput. Head snapshot collection is outside read
  time. Reads are warm OS page-cache reads following writes; no cold-device/cache
  purge claim is made. `heads` is an in-memory index benchmark, not disk reads.
- JSON reports per-call nearest-rank p50/p95/p99/max, call count, wall and summed API
  time. `mean_engine_ns_per_item` is an arithmetic amortization, **not** per-message
  p99. Never divide batch p99 by batch size and call that per-message tail latency.
- Throughput counts raw delivery records and raw payload bytes only. It excludes
  framing/metadata bytes and is not filesystem write bandwidth or MQTT Broker TPS.
  External `/usr/bin/time` includes startup/setup/cleanup and reports process-wide
  peak RSS; it is not isolated per-stage memory.
- `debug_assertions` in JSON must be false for reported Release runs. Single owner /
  single synchronous writer; this does not benchmark multiple parallel queues.

## Optional stages

```sh
# Per-batch Begin and terminal Complete each synchronize, no HTTP included.
target/release/egress-log-bench --phase lifecycle --records 2048 --batch 64
# One live checkpoint and reopen scan, explicitly single observations, not p99.
target/release/egress-log-bench --phase maintenance --records 65536 --batch 64
# Bounded sustained append/read/begin/complete with small quota to force reclamation.
target/release/egress-log-bench --phase cycle --records 8192 --batch 8 \
  --segment-bytes 4096 --max-bytes 65536
```

Lifecycle/cycle nested API metrics measure isolated call latencies; their wall
clocks span the entire corresponding pipeline, as stated in JSON. Use pair/cycle
throughput for those pipelines. `observed_disk_byte_decreases` is evidence of byte
reclamation, not an exact count of checkpoint generations.

Physical power-loss behavior still depends on the filesystem/device honoring sync;
this benchmark does not replace crash/fault tests or claim F_FULLFSYNC semantics.

## Tool checks

```sh
CARGO_TARGET_DIR="$PWD/target" cargo fmt --manifest-path benches/egress-log/Cargo.toml -- --check
CARGO_TARGET_DIR="$PWD/target" cargo clippy --offline --manifest-path benches/egress-log/Cargo.toml --all-targets -- -D warnings
CARGO_TARGET_DIR="$PWD/target" cargo test --offline --manifest-path benches/egress-log/Cargo.toml
```

Tests verify nearest-rank calculation, bounded CLI input, payload verification,
state completion, reopen correctness and cleanup of generated directories.

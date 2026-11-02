# Direct durable journal benchmark

This standalone package calls the production `mqtt-egress-log::Journal` shared
result/consumer API directly. Each synthetic result has one consumer; it measures
one stored body and one reference per input record. JSON schema2 identifies this
model. The November2 baseline before Phase5g used the legacy per-delivery API and
is historical; do not silently compare the old numbers as new-model measurements.
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
- Record construction is outside the `append_results` API latency timer, but inside
  append wall throughput. AppendResults returns only after production `sync_all` succeeds.
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

## Multiple rules and shared fanout

The `topology` binary exercises production shared APIs with distinct bodies per
`(rule,input)` and all destination references in the same durable append. It verifies
all destinations read every result, checks rule/client isolation and validates
unique-body byte accounting. This directly measures the journal; it does not time
SQL evaluation or HTTP delivery.

```sh
CARGO_TARGET_DIR="$PWD/target" cargo build --release --offline \
  --manifest-path benches/egress-log/Cargo.toml
/usr/bin/time -l target/release/topology --queue-dir /private/tmp \
  --inputs 4096 --rules 1 --fanout 16 --batch 64 --payload-bytes 1024
```

Each input matches every configured rule, each result targets every configured
outbound. `--inputs 4096 --rules 16 --fanout 1` writes 65,536 different results;
`--rules 1 --fanout 16` writes 4,096 results and the same 65,536 delivery references.
The append item unit is **rule results**; multiply by fanout for admitted references,
or divide by rules for synthetic source-input equivalents. Read units are **consumer
deliveries**: fanout consumers each read the body through the current production API.
Read wall time includes bounded head-page traversal and validation; API latency
excludes driver work. Reads are warm OS-cache reads with CRC, never a cold-disk claim.
Every append retains production `sync_all`; `--batch 1` is a separate durability
boundary, not equivalent to group commit at 64 results.

RSS is sampled using `ps` outside stage timings (KiB); unavailable permissions produce
null rather than zero. External macOS `time -l` reports peak RSS in **bytes** over the
whole process, including setup/read/cleanup. This is not Broker RSS, page-cache size,
or isolated index allocation. Dataset admission is conservatively capped at 200,000
references and an estimated 240 MiB; only a bounded append/read batch is materialized.
Temporary child directories are exclusive and automatically cleaned, never existing
queues. Every run uses 1 MiB segments and a 512 MiB queue budget.

[2026-11-02 measured matrix](../../docs/verification/shared-log-performance-20261102/README.md)
contains the sequential three-repeat runner, JSON, resource output and environment.

## Multi-frame Group Commit and segment experiments

The separate `group_commit` binary leaves the original `topology` control source
unchanged. Every frame remains at most64 results. `--group-width 1|4` selects one
or up to four frames per successful durable API call; a width4 call is a larger
confirmation barrier, **not the original width1 latency boundary**. All writes use
production sync. Multiple dirty segments require their own synchronization; the
read-only append_data_syncs counter records successful append-path data syncs,
including rotation. JSON reports counter deltas and frames per data sync, alongside
logical group calls and observed segment files. The counter excludes initialization,
state operations, checkpoint/reopen and directory syncs; it is not all filesystem syncs.

```sh
CARGO_TARGET_DIR="$PWD/target" cargo build --release --offline \
  --manifest-path benches/egress-log/Cargo.toml
/usr/bin/time -l target/release/group_commit --inputs 4096 --rules 1 --fanout 16 \
  --group-width 4 --segment-bytes 16777216 --mode direct --queue-dir /private/tmp
python3 benches/egress-log/run-group-commit-matrix.py --output /private/tmp/new-group-matrix
```

The default sequential runner performs54 measurements: three repeats of1rule1out,
4rules1out,1rule16out ×1/16/64MiB segments ×width1/4, each with1KiB payloads.
The multi-rule case uses16,384 source inputs (65,536 distinct results, over64MiB of
physical journal) to actually cross the largest segment; other cases use4,096.
Existing output directories are rejected. Workloads/binary hashes, resource output,
full readback and cold-reopen counts are retained. The binary conservatively bounds
pending references at200,000 and estimated live reservation at240MiB, within512MiB
queue quota. Only bounded groups enter memory. Peak RSS includes readback/cache,
reopen and cleanup; stage RSS after append is reported separately.

Direct mode reports **per-group durable API call p99**, with sample count. Preparation
is outside the API timer, inside write wall time. It is not per-request p99 and must
not be divided by the group's result count. Short cases may have only16 group calls,
so nearest-rank p99 then equals maximum; use the raw samples/counts honestly.

`--mode concurrent` uses16 producer threads, eight outstanding results per producer,
a128-slot request channel and a single journal owner. Owner drains only already
queued requests, without a batching timer. Every request records its own timestamp
before bounded send. `confirmation_latency` measures that instant through successful
sync-confirmed API return (including sender backpressure and queueing);
`observed_reply_latency` also includes reply/thread scheduling. Both are per-result
latency samples, not batch amortizations. `mean_frames_per_group` reports observed
aggregation; the configured width is an upper bound, not a guarantee.

```sh
python3 benches/egress-log/run-group-commit-matrix.py --output /private/tmp/new-confirmation \
  --mode concurrent --case fanout --segment-mib 16
```

These are direct journal experiments, not the production actor or HTTP/MQTT TPS.
No SQL, network calls, completion writes or steady-state queue reclamation are
included. Independent `group_commit_integrity` tests cover zero-write rejection,
IDs, cross-segment recovery, byte-compatible frame layout and uncertain torn tails;
private production tests verify actual synchronization counts and injected failures.

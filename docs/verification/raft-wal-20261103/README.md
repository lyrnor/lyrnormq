# Independent Controller Raft WAL verification — 2026-11-03

User-directed replacement of the SQLite Raft log backend. Contract:
[Phase7j](../../contracts/phase7j-raft-wal.md). Native macOS/arm64, Rust1.98.1 stable;
Linux release cross build uses local cargo-zigbuild/Zig0.16, amd64 musl.

## Storage separation

- `raft-wal/*.wal`: Raft Entries, votes, committed and purged watermarks, storage
  identity/version. Library raft-log0.4.6 / chunked-wal0.2.4 / codeq0.6.3.
- `metadata.sqlite`: business state-machine checkpoints/snapshots, directory identity,
  migration marker. Existing migrated SQLite log rows are inert backup; no normal
  Raft append/vote/watermark writes or dual-write.
- Business directory in RAM is incrementally applied. Snapshot policy remains
  LogsSinceLast1000, durable checkpoint before log purge. Replay committed-only tail.
- Legacy source validates checksum/IDs/contiguity and checkpoint coverage before
  staging import, full sync, close, directory sync/rename/parent sync, then marker.
  Missing migrated WAL never falls back to stale SQLite. Interrupted publication
  requires exact source/destination match before adopting it.

Every mutation waits for sync=true completion. No upstream default1ms batching wait.
Read buffer64KiB; flush queue16MiB/128 ready requests; chunks8MiB/4096 records with
bounded one-record/checkpoint overshoot. Payload-cache target16MiB/8192 entries is a
SOFT threshold, with additional bounded open-chunk retention; not an exact16MiB hard
memory ceiling. Strict dump preflight refuses damaged/torn/oversized chunks before
upstream may remove incomplete tail files. Poisoned instances cannot report success.

## Evidence

Initial integrated source9038a29, immutable native binary SHA256
`a6093b2fbf98c7a64bc2347c88adb670dda7497435fa5316b300e6ae4572d2b2`:

- New independent process suite7/7, first pass98.850s. Includes real old-binary
  SQLite import and second restart; bitflip/torn/missing-WAL refusal; committed-tail
  recovery with expired authority rejected and QoS2 state recovered; no SQLite Raft
  writes;2100 distinct subscriptions across periodic snapshots/log retention, then
  SIGKILL/restart and surviving-session routing.
- Three combined Controller+Broker processes: dual MQTT versions and all QoS,1/1,
  3.818s. Uses current MemoryCombined fixture; historical disk topology fixture is
  unsupported by RAM mode and was not counted as a passing compatibility gate.
- Raw logs: `/private/tmp/raft-wal-quality-final.log`,
  `/private/tmp/raft-wal-combined-memory-initial.log`.
- Old migration executable source10f3002, SHA256
  `d9bb04b7656c3911014347d98f9068fa03ee5450b71189cf7d36eaea6e019b51`.

Later source079851d includes bounded chunk/index tests and blocking cleanup ownership.
Adapter9/9 passed0.15s. Storage owner105/105 incl full OpenRaft suite passed17.13s
before final adapter-only integrity additions. Additional whole-workspace and final
binary checks recorded below when complete.

## Scope and limitations

SIGKILL/reopen is not physical power-failure certification. Failed flush-completion
unit injection happens after real fsync, not a simulated failing physical disk.
Corruption fails closed; no automatic deletion of acknowledged records. Homogeneous
upgrade only; downgrade/mixed-version clusters unsupported. Data replicas remain
RAM, so loss of the entire data group is not recoverable from Controller logs.

This is a backend rewrite, not proof of improved maximum connection rate. Previous
1000/s pressure failure was source10f3002 (SQLite log backend). Known shared write
admission, barrier-capacity mismatch and background ownership-reacquisition overload
remain separate tasks. No repeat of the failed pressure matrix or fabricated
throughput/latency comparison in this acceptance.

## Final source checks

Production sourceb2cfac4 (following test-only rotation coverage4ff3478):

- Workspace tests984passed/0failed/1external PostgreSQL ignored before the final
  two rotation-test additions; raw `/private/tmp/raft-wal-workspace-test.log`.
- Final cluster crate116 cases:112 passed in the sandbox;4 loopback-listener tests
  received OS PermissionDenied. Those4 passed0.03s with loopback permission, no
  code change. Full OpenRaft storage conformance is included among the112.
  Logs `/private/tmp/raft-wal-cluster-final-test.log` and
  `/private/tmp/raft-wal-control-network-final-test.log`.
- Adapter11/11 including normal byte/count chunk rollover, corruption/oversize,
  callback poison and bounds; strict full-workspace all-target Clippy and fmt pass.
  Logs `/private/tmp/raft-wal-owned-tests.log`, `/private/tmp/raft-wal-clippy-final.log`.
- Final native immutable SHA256
  `2a9b81a49ad0880002696eb73d12a5d6190f7bb9b32318559a2b31c91d3bd1c3`.

Reproduce (use immutable built executables; old source10f3002 is required to test
actual SQLite migration rather than synthesizing only a database fixture):

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
RMQTT_BIN=/absolute/new/rmqtt RMQTT_LEGACY_BIN=/absolute/old/rmqtt \
  python3 -W error::ResourceWarning tests/cluster_raft_wal_blackbox.py -f -v
RMQTT_BIN=/absolute/new/rmqtt PYTHONPATH=tests \
  python3 -m unittest cluster_memory_blackbox.MemoryCombined -f -v
cargo zigbuild --release --locked --target x86_64-unknown-linux-musl -p rmqtt
```

Final immutable native (2a9b81a4…) recovery suite:7/7 passed first run98.591s with
`-W error::ResourceWarning -f`, no warnings/retries. Log
`/private/tmp/raft-wal-quality-final-source.log`. This rechecks the final chunk
preflight limits and async-safe cleanup changes rather than assuming initial
binary results cover them.

Final MemoryCombined1/1 passed2.783s on the same immutable final native binary,
covering all three cohosted roles, both MQTT versions and QoS0/1/2. Raw log
`/private/tmp/raft-wal-quality-final-combined.log`; both executable hashes unchanged.

Local locked/offline Zig Linux release completed1m13s. `file` reports ELF64 x86-64,
statically linked/stripped. SHA256:
`8321c9cbf18cb1715a6b16d10ba463f59bce9053f17253e5d9575735d898e426`.
Artifact `target/cluster/x86_64-unknown-linux-musl/release/rmqtt`, immutable copy
`/private/tmp/rmqtt-raft-wal-linux`. Build log `/private/tmp/raft-wal-linux-release-build.log`.
The Zig linker emitted one warning that deprecated optimization setting1 is ignored;
compilation/linking succeeded. This is cross-build validation, not Linux runtime
or physical durability certification. No server compilation/deployment/pressure
run performed in Phase7j.

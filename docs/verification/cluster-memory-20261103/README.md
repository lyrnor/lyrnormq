# Phase7f — typed RAM data replicas / 2026-11-03

## Scope and reproducibility

Source: a8ef9c8 (production source c573b1e plus memory integration f163821).
Storage original 7d04984, runtime 582ff3f/2855f5d, quality 03a84bb, isolated worktrees.
Contract: [Phase7f](../../contracts/phase7f-memory-replicas.md).

Local environment: macOS arm64, Rust/Cargo 1.98.1, Zig 0.16. Local command:
`CARGO_TARGET_DIR=target/cluster cargo zigbuild --release --locked --offline --target x86_64-unknown-linux-musl -p lyrnormq`.
No Rust server compilation. ELF SHA256:
`24e861eaf294ddc0aa40aee1a36ca0ae1cd64b855f12761d2cd286ba9ab077fb`.
Native immutable test binary SHA256:
`bc441d1df383ce4ab74a0bbce0b050b57314583d38027491f4d1183532e45b49`.

## Correctness checks

- Rust workspace: 905 passed, 1 external PostgreSQL test ignored.
- Strict workspace all-target Clippy and cargo fmt check: passed.
- Native RAM wire acceptance: 17/17, 109.692s; legacy cluster regression: 20/20, 78.056s.
- Controller observation probe tests: 4/4 (quality agent).
- Initial sandboxed Rust invocation failed on denied TCP bind; complete rerun with
  local-network access passed. Both logs retained; no source fix needed.
- Read-only cross-layer review found no blocking regression in W2, atomic record
  replacement, generation fencing, startup cancellation or same-process repair.

RAM tests cover both MQTT versions/all QoS, live-majority session/packet-state
failover, offline delivery, restarted empty replicas excluded from W2, same-process
repair, interrupted staging with new boot identity, whole-group RAM loss failing
closed, old SQLite refusal and rejected local session mirrors. Historical all-disk
restart tests were replaced, not claimed to pass with RAM. CI itself was not run.

## Semantics and remaining work

Data nodes keep typed records and bounded canonical caches in RAM. Controllers use
SQLite for Raft; rules/access configuration SQLite and outbound append logs remain.
Normal data IO does not write SQLite, fsync or scan database quota tables. Record
serialization remains; 256MiB accounted bytes are not an RSS cap. No performance
maximum, physical power-loss recovery or all-group restart durability is claimed.

Future: RocksDB implementation/durability contract, serialization and scheduler
profiling, automatic incremental repair, cleanup/membership and multi-machine
failure-domain acceptance. Retained/Will/shared cluster features remain refused.

## Server reproduction

Broker host 47.84.224.186 / 172.16.240.180; load host 47.84.241.200 /
172.16.240.181. Both 8 logical CPUs, about 15GiB RAM, Ubuntu 26.04.1 x86_64.
Three Controllers plus three data processes on one broker host; this is not six
physical failure domains. Fresh cluster at `/opt/lyrnormq-cluster-memory/20261103`;
old SQL deployment stopped with PID identity checks and directories preserved.
Peer token and configuration secrets are deliberately excluded from evidence.

Same loadgen ELF as preceding SQL run: SHA256
`f955aa4d0843c48f0a689c5ea8ade782910709ae145a23afe77fdf494e19d7ef`.
30 pairs/60 TCP, MQTT5, 128B, 100 messages/s, inflight1/pair, warmup3s,
measurement30s, drain30s, setup concurrency1. Publisher rotates three data endpoints;
subscriber uses next endpoint, forcing cross-node forwarding. Exact command and
single-process monotonic timing/nearest-rank percentiles appear in each JSON report.
Offering-window TPS excludes late drain deliveries; final cohort counts include them.

Results below compare a persistent SQL backend
with volatile replication and therefore are not equivalent durability benchmarks.

| QoS | Planned / sent / unique received | Source final ACK | Offering delivery TPS | p99 send→delivery |
| --- | --- | --- | --- | --- |
| 0 | 3000 / 3000 / 3000 | N/A | 99.933 | 107.115 ms |
| 1 | 3000 / 3000 / 3000 | 3000 | 99.833 | 106.881 ms |
| 2 | 3000 / 3000 / 3000 | 3000 | 99.933 | 110.069 ms |

All three cases: not_sent=0, not_delivered=0, duplicates=0, failed_pairs=0.
All 20 connections per endpoint observed. Final Controller applied=3803/revision546
on all three at slightly different sample instants (not an atomic snapshot or
synchronization latency measurement).

Across 287 one-second samples (287.385s, includes startup/setup/warmup/measurement/
drain/idle), all six processes summed CPU averaged39.37% of one logical core,
peak106.47%; summed RSS peak241.45MiB, data-process summed RSS peak142.87MiB.
RSS sums include shared pages multiple times and are not heap allocation counts.
These are whole-run figures, not isolated per-message CPU cost. Raw counters and
sampling denominators are in resource-summary.json and server-evidence.tar.gz.

Previous persistent SQL backend delivered1773/1531/1101 at offered100/s, with
p99~908/1006/984ms; RAM delivers the complete3000 each. This clears this load only,
not a maximum TPS gate. Residual p99~107–110ms remains. Source review identifies
100ms background polling in broker/durable.rs as a candidate, not a measured
causal attribution. Further latency optimization requires a separate experiment.

Linux deployed ELF: memory wire acceptance17/17 passed in103.434s, after capacity
runs so fault injection did not interfere with performance measurements. Initial
remote invocation had an incomplete copied Python harness (missing imported helper),
failed before any case; full test-helper upload fixed execution, no code change.
Both logs retained. Six-process deployment remains running after isolated tests.

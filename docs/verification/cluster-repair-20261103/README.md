# Phase7c replica repair acceptance — 2026-11-03

Actual implementation source: `69b6ad2`; production Linux source through
`3666fe7` (the subsequent commit changes tests only). Three specialist agents
implemented runtime, Journal and independent wire tests in separate Git worktrees;
the coordinator integrated and reviewed their commits.

The release was built **locally on macOS**, with Rust stable 1.98.1 and Zig 0.16.0:

```sh
CARGO_TARGET_DIR=target/cluster cargo zigbuild --release --locked --offline --target x86_64-unknown-linux-musl -p lyrnormq
```

The uploaded static x86_64 ELF SHA256 is
`ed8dcbbecc17064ce3b3ab59bc29139e1cf445fbb722db32dba32279f0b2710c`.
Server-side checksum verification passed. No Rust compilation ran on the server.
The release build emitted one nonfatal linker deprecation warning; see the log.

## Actual remote results

Server `47.84.224.186`: Ubuntu 26.04.1 LTS, x86_64, 8 logical CPUs,
15,687,640 KiB RAM. Exact collected environment is in [environment.json](environment.json).
Each fixture runs separate Controller/Broker processes with isolated temporary
directories and loopback ports on this **one host**.

| Suite | Passed | Wall time | Evidence |
|---|---:|---:|---|
| Repair/replacement | 12 | 156.117 s | [repair.log](repair.log) |
| Durable forwarding/recovery | 27 | 164.950 s | [durable.log](durable.log) |
| Prior cluster behavior | 20 | 70.511 s | [legacy.log](legacy.log) |
| HTTP publish, lifecycle events, HTTP access | 77 | 36.858 s | [standalone.log](standalone.log) |

All four unittest suites returned OK. No lyrnormq process remained after the tests.
Artifacts remain under `/opt/lyrnormq-cluster-acceptance/phase7c-3666fe7/`; existing
services were not replaced and no persistent production Broker was started.

Reproduce from that directory, or use the same binary and repository scripts:

```sh
LYRNORMQ_BIN="$PWD/lyrnormq" python3 tests/cluster_repair_blackbox.py -v
LYRNORMQ_BIN="$PWD/lyrnormq" python3 tests/cluster_durable_blackbox.py -v
LYRNORMQ_BIN="$PWD/lyrnormq" python3 tests/cluster_blackbox.py -v
PYTHONPATH=tests LYRNORMQ_BIN="$PWD/lyrnormq" python3 -m unittest http_publish_blackbox system_client_events_blackbox http_access_blackbox -v
```

## Local verification and discovered defects

`cargo test --workspace --locked --offline`: **887 passed, one explicit live
PostgreSQL environment ignore**. Strict workspace all-target Clippy, fmt and diff
checks passed. See [Rust log](rust-tests.log), [Clippy log](clippy.log) and
[Linux build log](linux-build.log). The official OpenRaft storage suite is included.

Earlier native repair acceptance passed 12 cases in 159.194 s; its binary hash was
`6878d77a938ee6a14969a77775ca4f2f3a8e38eb2fd43409c930cf01b02f1f80`,
which differs from the final Linux binary. See [native log](native-repair-v2.log).

Initial independent tests exposed donor restart incorrectly waiting for data
readiness during a durable freeze, and an overloaded concurrent fixture setup.
The former was fixed by separating control readiness; fixture seed admission now
uses bounded retry without relaxing repair assertions. Review also removed a
temporary frozen flag from ordinary Journal transactions. A separate reproducible
file-lock regression identified the test child fork/exec inheritance window;
test-only spawn/open serialization and explicit blocking-owner release fix it
without production sleeps or retries. Before-fix logs are retained alongside the
final results.

## Accepted scope and remaining work

Two original donor journals freeze persistently; the excluded learner imports a
bounded union, verifies a manifest and installs atomically. The controller checks
proofs before replacing the disk anchor. Old generations, late requests, stale
disk IDs and corrupted pages cannot bypass fencing. Installed-but-uncommitted
targets cannot vote. Abort resumes the two originals and leaves the target excluded.
Tests cover driver/learner/donor restarts, pre/post-commit crash boundaries, retries,
bad digests, tombstones/fences and preserved QoS/outbox state on both MQTT versions.

This is maintenance-window repair for the fixed three-replica group. It does not
establish online repair, arbitrary membership changes, multi-host partitions,
physical power-loss safety or TPS/P99/memory performance. Cluster Retained, Will
and shared-subscription semantics remain unsupported. See the
[operational guide](../../cluster/replica-repair.md) and
[frozen contract](../../contracts/phase7c-replica-repair.md).

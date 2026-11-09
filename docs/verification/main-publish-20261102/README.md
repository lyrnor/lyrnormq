# Stable main publish acceptance — 2026-11-02

Prepared from stable main d1c409b plus the reviewed Kafka replacement commits.
No force push; target private repository https://github.com/biaogd/lyrnormq.

- 760 Rust workspace tests passed, zero failures/ignored.
- 96 React tests, production frontend build passed.
- Workspace fmt, strict all-target Clippy and diff checks passed.
- Native Release, locked/offline Linux amd64 musl zigbuild passed.
- ELF static check: no interpreter or required shared libraries. Linux was not executed.
- All 16 real Apache Kafka cases passed together in41.932s on this exact Release,
  including TLS/mTLS/SASL, three codecs, keys, retries and graceful/SIGKILL recovery.

The Kafka adapter/egress/benchmark source trees are identical to the previously
measured feature branch. Paired performance data remains under
../krafka-migration-20261102; its original801-test count includes HTTP development.
This final stable-main scope has760 Rust tests because unfinished HTTP integration
was excluded. The final main Linux artifact is18527744bytes (17.67MiB); see cross-elf.json.

All HTTP work, including existing uncommitted management changes, was preserved in
commit7026380 on codex/http-access-integration-wip for a separate remote branch.
Its complete protocol/receive/fanout/recovery acceptance gate remains pending;
no HTTP authentication/authorization completion claim is made for main.

The lockfile was pruned only to remove the excluded mqtt-auth-http workspace
package; existing dependency versions were retained. Benchmark throughput regressions
and platform/multi-replica/soak limits remain documented; no deployment was done.

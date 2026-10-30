# Rust load generator verification — 2026-10-30

Code revision `f44b65736eca53254c2a07a115e6e1ab6e3bc2d0`, branch `codex/rust-loadgen`.
[Usage](../../../bin/rmqtt-loadgen/README.md), [contract](../../contracts/rust-loadgen.md),
[manifest](manifest.json). Existing broker modules and schema were not modified.

43 tool tests passed:26unit,13independent raw-wire,4CLI. Fullworkspace511passed,
0failed/ignored;fmt and strict all-target Clippy passed. Tests cover actual source
PUBCOMP completion,subscriber PUBREL delivery,duplicates,incorrect/rejected ACKs,
disconnect/corruption/timeouts/quotas,late samples and absolute-rate lag,raw JSONL
nearest-rank recomputation,existing-file protection and valid failure JSON.

Three real subagents used explicitly isolated engine/metrics/quality worktrees.
Source engine52b7d7+8b2c9a8,metricsb05956f+cae19d2,qualityc0bbcac+b866931+1357bcf;
coordinator integrated them sequentially. Crossreview fixed cleanup/drain denominator,
whole-run drain deadline and record-level counter validation. No shared-file writes.

[Actual local broker checks](local-real-broker.json):six MQTT3.1.1/5×QoS0/1/2 profiles,
each8pairs/window8/warmup50ms/offering250ms. All succeeded and all six raw latency
series' p50/p95/p99/max/count were independently recomputed. Raw samples retained
as deterministic gzip files alongside reports. Three MQTT5 fixed-rate800/s×500ms
checks each planned400 messages. This recorded400/400,399/400,400/400 attempts:
one run explicitly returned unsuccessful with not_sent1 because its final deadline
was missed. The verification accepts that truthful accounting,not the load as a
passed fixed-rate gate. No unsent messages or failed protocol cases were hidden.

Linux amd64 musl staticrelease was cross-built locally;SHA256 inmanifest. These
are correctness checks,not an official capacity/latency baseline. Current live
server verification has not completed because47.117.6.20'sSSH host key changed;
server identity confirmation was requested before accepting a new host key.
The old known_hosts record was not removed and verification was not disabled.

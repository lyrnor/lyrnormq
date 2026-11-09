# Rust load generator verification — 2026-10-30

Code revision `f44b65736eca53254c2a07a115e6e1ab6e3bc2d0`, branch `codex/rust-loadgen`.
[Usage](../../../bin/lyrnormq-loadgen/README.md), [contract](../../contracts/rust-loadgen.md),
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
are correctness checks,not an official capacity/latency baseline. The identity gate
was resolved when the user confirmed47.117.6.20 is a new instance. New ED25519 key
was pinned in a task-local known-hosts file;StrictHostKeyChecking remained enabled,
and the old global known_hosts was not modified.

## Actual new-server acceptance

New generator:47.117.6.20/private172.30.139.132,8logicalCPUs,16GB-class RAM,
Ubuntu26.04.1x86-64. Binary`/opt/lyrnormq-loadgen/lyrnormq-loadgen`,locally cross-built,
SHA256 in manifest. Old generator was4cores/private172.30.139.131;do not compare
these results to earlier emqtt runs as a pure broker speedup.

All three QoS runs used2500pairs=5000connections,128bytes,closed-loop inflight1,
warmup1s/offering5s. Every measured record fully wrote,uniquely delivered and
completed its required source protocol;QoS2 only PUBCOMP counted final. All six
raw sample latency series were independently recomputed on server;deliveryp99
rechecked again locally. Zero duplicates/missing/failed measured records. Broker
PID2100933 unchanged,no restarts,active with0test connections after completion.

|QoS|Measured cohort samples|Offering-window delivery TPS|Delivery p50ms|p95ms|p99ms|Late deliveries|
|---|---:|---:|---:|---:|---:|---:|
|0|246454|48790.80|8.206|148.402|270.025|2500|
|1|148181|29162.60|12.133|210.383|389.771|2368|
|2|97451|18991.20|162.315|322.044|462.671|2495|

TPS above excludes events outside the5s offering interval;p99 covers the entire
measured publication cohort,including events completed during bounded drain. These
are different explicit views,not a shorter denominator for late events. Ns is a
software timestamp unit,not physical1ns accuracy. This is a short functionality
acceptance,not saturated maximum throughput,long-duration stability or broker-only
service latency. No new broker CPU/RSS measurements were invented;systemd's memory
peak also includes the Python verifier and is not labeled as broker/tool memory.

[Remote reports and verifier](remote-verification.json),[full raw archive](remote-acceptance-raw.tar.gz),
[verifier source](remote-verifier.py). Archive SHA256 matches both endpoints and is
recorded in manifest. No private keys are included. Reports remain on the server
under`/var/log/lyrnormq-loadgen/acceptance-20261030`.

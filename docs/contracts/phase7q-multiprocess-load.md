# Phase7q — multiple connection-probe processes

User requests multiple load instances after Phase7p failed at the probe2048inflight
boundary. Authorizes re-running that case with4processes on47.84.241.200, each
500new connections/s and2500total, unchanged aggregate2000/s and10000connections.
Broker87ced3e8, three dual-role nodes, limits/deadlines/durability stay unchanged.
Each probe retains2048inflight (8192aggregate),15s CONNECT timeout, no retry and
explicit skipped-arrival accounting. Fail stops further pressure. Higher rates and
message cases remain subsequent gates, not implicitly successful from this test.

Loadgen owner in independent worktree owns bin/lyrnormq-loadgen/src/bin/connections.rs
and optional connections/** only. No rootCargo/lib changes. Coordinator owns docs,
orchestration, aggregation and deployment. Quality independently reads/recomputes.

Retain endpoints rate output [plateaus] positional CLI and existing report fields.
Add optional named flags after positionals:
- --connections-per-step N:default10000;1..100000, N*plateaus<=100000.
- --client-prefix TEXT:ASCIIalphanumeric/_/-,1..48; append global zero-padded index.
  Default must include process identity as well as timestamp to avoid same-host
  concurrent same-second collisions. Explicit prefixes used for distributed runs.
- --index-offset N:default0; shifts client index AND endpoint round robin; checked
  arithmetic.4instances use0,2500,5000,7500, preserving balanced total endpoints.
- --start-unix-ms N:optional common future first-ramp deadline, converted once to
  a monotonic deadline before traffic; reject stale/too-far(>60s) requested start.
- --ping-unix-ms N:optional common future first-plateau PING time; restrict common
  start/PING mode to1plateau, require both flags and ping-start>=10000ms,<=60000ms.
  All accepted sockets remain open until commonPING; report whether ramp completion
  preceded PING by>=10s. A late drain must not count as a successful10s hold gate.
- --raw PATH:optional fresh JSON file containing per-step arrays of success
  latency_us and actual-start schedule_lag_us, with their counts and run settings.
  Write once after network work, bounded<=100000samples; never average shard p99s.

New reports include client_prefix,index_offset,connections_per_step,requested
start/PING timestamps, actual ramp-start/completion/PING wall timestamps and
shared-hold>=10s boolean. `hold_seconds` remains10 and old single-process behavior
unchanged without common timing. Preserve absolute due times relative to scheduled
start even if the task wakes late; record schedule lag, no silent clock reset.
No tighter success claim from process exit0; reports remain authoritative.

Fix old namespace collision with a failing test first, then test option validation,
distinct prefixes/process defaults, count/offset/endpoints, shared timing, exact
raw percentile reconstruction and existing wire behavior. fmt, strictalltarget
Clippy and loadgen tests. Only modified probe is Zig-built locally for amd64musl;
never compile on either server or rebuild unchanged Broker unnecessarily.

Coordinator launches4processes with2Tokio workers each(total8), unique same-length
prefixes, a common start>=5s in the future and PING=start+35s. Verify all actual
ramps complete by PING-10s before claiming10000simultaneously held10s. Aggregate
global completion window from commonstart to latest completion; p99/mean from
merged raw success samples. Record per-process success/errors/skips/clock lag,
CPU/RSS and all endpoints. Source-port range stays unchanged for10000total.
Use freshcluster/data/output paths, preserve old evidence, stop all test processes.

Review clarification: sample actual-start lag inside the connection task, immediately
before TCP connect, including task scheduling delay. Keep a lag for every attempted
request even when it fails; do not fill missing observations with zeros. The common
start anchors monotonic ramp elapsed, used for the aggregate window denominator.
Wall timestamps are auditing fields. Validate the global held interval from latest
ramp completion to earliest common PING, not just four independent hold counters.
Four500/s streams have approximately four arrivals every2ms, which differs from
the previous single2000/s stream; report this and the increased probe window.
Measure simultaneous sum-RSS peak and sum-CPU across all4load processes.

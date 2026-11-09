# Cluster admission and recovery repair — 2026-11-03

Production source48823b2. Contract: docs/contracts/phase7l-admission-recovery.md.
All source changes integrated from independent specialist worktrees after RED/GREEN
regressions; no Controller storage format or Raft/data ACK/quorum guarantees changed.

## Changed behavior

- Actual execution256 per data node; globally queued512, extra admission waiters512
  for750ms; per-client mailbox8 and16MiB aggregate payload budget retained. Queued
  work cannot reserve execution behind the same Client's current transaction.
- Only explicit cache recovery faults start background Acquire. Unknown replies stay
  fenced; proven pre-submit rejection preserves prior cache state. A stale epoch
  rejection stays fenced and permits bounded fresh inspection in the foreground.
  Generic WouldBlock/overload/unknown errors do not repeat foreground Acquire.
- Quiet MQTT3.1.1/5 PING performs no replicated session commit/version increment.
  Local Keep Alive, readiness/fencing and actual expiry/Will mutations retained.
- Controller business256, reserved health16, Applied1024; Broker command256;
  private RPC handlers/HTTP2 streams1024 per connection. Submitted Raft tasks own
  admission until actual completion after response cancellation/deadline. Health
  shares the underlying Raft core/CPU/disk; reserved admission is not IO priority.
- Heartbeat failure reads one linearizable Node, never full Metadata. Preserve
  RPC status/outcome; no resubmission of unknown writes. Total deadlines across
  Controller seed attempts: ordinary2s, Applied5s, repair60s; server Applied4s.
  Existing authentication claim5s and actor reply8s retained; background outer Work
  allowance30s replaces the prior6s/8s inversion. No throughput claim from budgets.

## Verification and artifacts

Native workspace1015passed,1external PostgreSQL ignored; fmt and strict all-target
workspace Clippy passed. Capacity preflight5/offline exporter10 Python tests passed.
New regressions include queued/active separation with a real stalled actor, bounded
aggregate mailboxes, cancellation-owned bodies/admission, health capacity isolation,
unknown write submitted once, explicit epoch-conflict bounded retry, cache poisoning,
node-only registration checks and quiet/due-expiry PING across both versions.

Linux ELF64 x86-64 static stripped release compiled LOCALLY using
cargo zigbuild --release --locked --offline --target x86_64-unknown-linux-musl
--bin lyrnormq --bin connections. No server compilation. Zig emits its existing
'deprecated linker optimization1 ignored' warning; successful build.
Broker SHA2560d37def5783db8e7df28fceeb56aafbea26bc2c6dc3b5541b91a7e0d19d07a21.
Probe SHA256d0acf9899b9ea9d34fd68c8a5234b4f741b97bef816987b00b49f4ad6a28c2ae.
Native immutable release available at /private/tmp/lyrnormq-7l-native.

## Reproduction environment

Broker47.84.224.186/private172.16.240.180; load47.84.241.200/private172.16.240.181.
Both8logicalCPU/~15GiB. Three cohosted Controller+RAM data processes on one host,
4Tokio workers/process, shared ext4/vda. This is a same-host cluster throughput
measurement; it does not measure multi-machine failure independence. Same private
MQTT endpoints18831/18832/18833 and round-robin clients as the previous failed run.

Fresh isolated deployment /opt/lyrnormq-admission-repair/20261103; load directory
/opt/lyrnormq-admission-repair-load/20261103. Both max_connections/max_sessions50000
per node, state256MiB, metadata16711680B, physical records101024, handshake1024,
auth tasks128, auth rate0, FD250000. New admission budgets saved in raw preflight.

Only first1000/s→10000 cohort authorized for this repair gate. Probe now accepts an
optional plateau count(default10); here1. Arrival scheduling,2048 handshake bound,
15s timeout,10s hold and validated PING unchanged. No connection retries in probe.
Existing full-matrix case_passed still requires100k; reproduce.py explicitly checks
one10k cohort, never labels it100k capacity. No source-port sysctl change required.

## First-cohort result (failed; pressure stopped)

Linux WAL/combined7 tests passed33.413s; RAM/event27 tests passed104.154s (the combined
case is included in both selections). No fault-injection claim beyond these tests.

All10000 arrivals attempted,0skipped;417 successful CONNACKs,417 alive after10s/PING,
0PINGerrors.7492CONNACK0x88,2091EOF. Successful-CONNACK mean1799.422ms,
p992179.406ms,max2211.093ms. Ramp+drain11.292s;36.930successful/s describes this
failed run, NOT maximum sustainable throughput. Previous run281accepted/186alive;
these remain failed runs, not comparable successful-load capacity measurements.

Aggregate3Broker CPU average2.381cores, highest sampled interval4.160/8; RSS peak
489.05MiB,FDpeak624,ESTABLISHEDpeak483(includespending).21active rows/20CPU intervals
inside approximately22.010s client window; sampling/host wall alignment is coarse.
Load average0.081core,peakRSS2.99MiB;schedulinglagmean0.624ms,p991.237ms,max9.304ms.
Final Controllers term1/leader1/applied7555,metadataowners2508 aligned; owner count
is not successful/live TCP count. All test processes exited; no sysctl changed.

No Durable admission/byte/wait rejection category appeared, but Control admission
rejections are not separately counted, so this does NOT prove every gate stayed
below capacity. BackgroundWorkReply/Reacquire reached at least4096 EACH per node;
JournalOlderVersion64/32/32. Counts are logarithmic lower bounds, not totals.
Initial vote network error was during peer startup; no evidence of WAL checksum/
bbolt corruption in this run. Unknown first cache fault remains unresolved because
Missing/Poisoned/StaleEpoch and Acquire stages were not logged separately.
No higher load,2000/4000rates,100k or message-TPS case was attempted. On user followup,
cluster admission quotas will be removed separately; this failed result stays intact.

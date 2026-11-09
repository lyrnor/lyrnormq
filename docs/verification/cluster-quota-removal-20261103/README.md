# Cluster quota removal — 2026-11-03

Source950fa70; contract docs/contracts/phase7m-remove-cluster-quotas.md.
User explicitly requested removing quotas, superseding Phase7l admission counts.
Runtime and control specialists used separate worktrees/branches. Integrated commits:
b99068a (runtime), ac2e530 (control),950fa70 (legacy receipt pending/cache split).

Removed global transaction execution/queued/waiter/aggregate-body semaphores and
750ms admission refusal; removed Broker command/forward/delivery, Controller
business/health/watch/Applied permit gates and explicit RPC/HTTP2 concurrency caps.
Per-client FIFO mailboxes asynchronously wait when full. Submitted jobs and Raft
writes continue after caller cancellation; existing deadlines, W2/authority/epoch,
all-live barriers and unknown-result fencing remain. Quiet PING fix retained.

Legacy forwarding no longer rejects the8193rd pending delivery. Completed receipt
cache remains8192; only completed records evict at completion. Pending identities
remain until result/cancellation. Disarm the old ReceiptGuard under the lock so a
completed/evicted entry's guard cannot remove a subsequent retry of the same key.

Physical/session/directory/record/protocol bounds, peer-cache cardinality and actual
repair/blocking-IO worker pools remain; these are not global transaction admission.
No unbounded mailbox or extra task merely waiting for an execution permit introduced.

Regression evidence includes RED old513th task rejected at750ms; new513distinct
mailboxes and17MiB queued bodies accepted; full mailbox>900ms waits thenFIFO; Work/
Received share backpressure; canceled send submits nothing and canceled reply keeps
submitted work. Control514mixedwrites continue after cancellation,1025barriers and
257Broker commands no capacity rejection. Receipt tests cover8193pending, completed
retention, pending dedupe and canceled/retried identity cleanup.

No new server deployment or pressure in this phase. The preceding failed server
cohort was48823b2:417/10000 CONNECTs,417alive. Do not describe that result as a test
of this source or infer a performance improvement from quota-removal unit tests.
The initial cache-fault/reacquisition loop remains unresolved; removal of admission
is not proof that recovery or target connection capacity is now fixed.
See docs/performance/cluster-admission-20261103 for intact measured evidence.

## Final verification

Finalsource950fa70: native workspace1017passed,1external PostgreSQL test ignored;
cargo fmt --all -- --check and strict workspace all-target Clippy passed. Logs:
/private/tmp/7m-final-workspace-tests.log and7m-final-workspace-clippy.log.
Local native release and Linux amd64-musl static release both compiled successfully.
Linux artifact /private/tmp/lyrnormq-7m-final-linux SHA256
 e8f2b38bd16e960d711b517a2eb648307cbb5f019f6cf55871165b9117f4ec8e.
Zig emits its existing deprecated linker optimization warning; build succeeds.

Final immutable native b30f257b186d4d4cb44c74049b8db70b0fa2a2fface518172b6bad54a82b47c8:
14 actual-process tests passed46.013s with fail-fast/ResourceWarning-as-error. Covers
three combined-role processes both versions/allQoS, MQTT5offline/inflightQoS1/2
owner loss and recovery, event-driven delayed/lostreply/readiness recovery, legacy
MQTT3.1.1/5QoS/order, global takeover and no reflected/repeated forwarding rules.
Log /private/tmp/7m-final-process-tests.log; these are functional/fault tests, not
throughput/latency capacity measurements. Preflight Python5tests passed. No server
processes started or deployment changed after the preceding failed pressure gate.

Subsequent explicit user authorization deployed this same source and ran the first
1000/s→10kcohort. It FAILED with0successfulCONNACKs; allprocessesstopped.
[Measured followup](../../performance/cluster-quota-free-20261103/README.md) preserves
actual accounting/resources and remaining diagnostic gaps. The functional-test
results above are not a target-load performance acceptance claim.

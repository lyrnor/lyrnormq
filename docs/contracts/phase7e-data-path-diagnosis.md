# Phase7e durable admission/forwarding failure diagnosis

Reproduce the Phase7d 30-pair, 60-TCP,128B,aggregate100/s MQTT5 workload with RF3/W2
before any fix. Preserve warmup3s, bounded window1, no connect retries and finite
drain; setup concurrency1 separates steady traffic from connection bursts.
Instrument fixed internal failure categories, with bounded/rate-limited logs or
counters; never log client ID, topic, payload, credentials or tokens. Distinguish
Durable operation/byte/actor admission, Journal admission, generation/freeze,
stale epoch/version/conflicting value, quorum failures, cached revision/authority
and background reacquisition. Generic0x83/0x8b is not a proven cause.

Do not enlarge quotas or weaken W2/ACK/receipt/outbox atomicity to pass. Diagnose
first, add a deterministic failing regression, then coordinate the smallest fix.
Unknown writes still invalidate authority; pre-write failures must be distinguished
from ambiguous durable outcomes. Preserve schemas, repair generation fencing and
thin lib.rs. Runtime specialist owns mqtt-cluster, quality owns tests/cluster_data_path*
and own test harness additions. Each independent worktree commits results.
Coordinator owns docs, CLI/build/integration/deployment. No changes to the existing
six-process deployment until a locally built diagnostic binary is ready; use
separate run directories and preserve prior evidence. Local zigbuild only.

## Confirmed Journal admission fix (Linux diagnostic reproduction)

First JournalOperations rejection precedes QuorumPeer/QuorumInsufficient, then
background reacquisition and cache poisoning; no quota/version fault is required.
Preserve32 active blocking-pool Journal jobs. On saturation admit at most32 separate
async waiters, each waiting at most750ms for FIFO Tokio semaphore capacity. Full
wait queue or timeout fails explicitly; never spawn a DBjob before acquiring its
active permit. Waiting cancellation drops its queue entry and never mutates storage.
Keep transactional freeze/generation/epoch checks at execution time after waiting,
and retain ambiguity poisoning if an already-submitted write loses quorum.
This adds bounded backpressure, not a higher execution limit, weaker ACK, unlimited
queue, or guaranteed throughput on arbitrary disks. Distinguish saturation waiting
from actual queue-full/timeout diagnostics. No100ms scheduler optimization in this fix.

The extra32 waiting requests retain decoded request bodies/closures. This is a new
finite memory budget, not a claim of unchanged peak RSS or a precise RAM cap;
existing frame/record limits and decoding overhead still apply. Sustained stalls
or overload can still fail with explicit full/timeout diagnostics. The fix only
removes immediate quorum failure caused by short Journal admission bursts.

## Upper-layer completion after Journal backpressure

Linux retest removed Journal immediate rejections but exposed DurableOperations
as the next first rejection. Keep32 business operations, actor channel8 and total
16MiB payload permits unchanged. Business admission may queue at most32 async
waiters for750ms; charge payload before waiting, reserve actor capacity and FIFO
operation capacity before submitting any Job. Cancellation/timeout submits nothing.
Work/Forget remain nonblocking best effort and do not consume this waiting budget;
a rejected background admission must not trigger ownership reacquisition. Internal
Release may become async; public ClusterBridge/durable schemas remain unchanged.
Existing committed/ambiguous outcomes still follow original authority fencing.

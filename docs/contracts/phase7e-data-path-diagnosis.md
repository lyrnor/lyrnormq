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

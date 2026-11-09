# Phase7t — bounded FIFO control write pipeline

User requests continuing optimization after Phase7s. Supersedes only Phase7o's
one-outstanding-batch scheduling rule. Preserve batch256/512KiB, mailbox1024 async
backpressure, two-second caller deadline, schemas, WAL flush/committed-only replay,
ReadIndex/all-live barriers, session/outbox semantics and admission limits.
No OpenRaft fork, dependency upgrade or relaxed ACK/durability. lib.rs stays thin.

Ownership in isolated worktrees: control owns controller.rs/controller/**;
storage reviewer owns storage/pipeline_tests.rs plus minimal test-module declaration
in storage.rs only (no production storage changes); quality owns
tests/cluster_pipeline*. Coordinator owns profile.rs, docs/harness and integration.

Submission and completion:
- One owned FIFO worker may hold at most FOUR submitted, unfinished batches.
  Drain only ready commands per batch; no fill timer and no per-RPC spawned task.
- Use pinned OpenRaft0.9.25 public client_write_ff: it submits to Core and returns
  the response receiver. Await each submission in FIFO sequence before submitting
  the next; never spawn concurrent client_write futures whose enqueue order races.
- Retain each receiver and its child reply ownership until completion. Receiving
  the submission handle is NOT success. Only the quorum+applied result can reply.
  Preserve exact child result/revision/epoch mapping and lease_ms response handling.
- Process completed batches in FIFO order; ready completions take priority over
  accepting more work. Waiting for space stays cancellable; accepted work survives
  caller timeout/cancellation. Closing input drains accepted/carry/in-flight work.
- Before actual submit check current leadership. Proven pre-submit rejection keeps
  existing failed_precondition semantics; errors after attempted submission and
  dropped result channels conservatively remain unknown. A partial failure must not
  discard another submitted batch or misroute its answers. Drain on shutdown/fatal.
- Slots apply only to finite active pipeline work, not a new capacity-rejection
  quota. Retain FIFO ready aggregation/count/byte/carry limits and cancellation rules.

Lease preparation:
- Register/Heartbeat timestamps remain controller-generated. Account for committed
  state AND earlier uncompleted prepared/submitted grants for the same node, not
  just the previous applied snapshot. Values remain monotonic in submission order.
- Keep pending grant bookkeeping bounded by current pipeline/current batch/carry;
  do not accumulate an unbounded historical map under continuous input/node churn.
- On completion/failure retire corresponding pending bookkeeping; committed state
  remains authoritative. Term/leader changes never authorize early lease replies.

Diagnostics:
- Existing BatchRaft measures submission attempt through FIFO completion handling;
  it can include head-of-line completion waiting. Existing trace includes submit/end,
  node/index/count/bytes. New BatchSubmit measures client_write_ff submission only;
  BatchWindow observes submitted-uncompleted occupancy (1..4).
- Batch intervals now overlap. Compute interval UNION/peak overlap for utilization;
  do not sum batch durations and label the sum wall-time or utilization.
- OpenRaft may append/apply a range covering several batches. Preserve raw ranges;
  correlate covering intervals explicitly and report shared-stage attribution and
  unmatched counts. Do not pretend one sync per batch or add shared stage work.

Validation before server work:
- Regression first: second/third/fourth batches submit before first completes but
  fifth waits; FIFO submit/replies; no early ACK; cancellation, queued shutdown,
  partial failure, dropped response and malformed/oversized result handling.
- Grant monotonicity across pending batches and bounded cleanup, mixed child
  results, committed-prefix replay and crash recovery with multiple batch entries.
- Independent real-process concurrent control/leader failure test; reuse existing
  dualversion QoS, offline recovery and recipient-generation blackboxes.
- Integrated tests, fmt, strict workspace all-target Clippy; locally build optimized
  symbol-bearing amd64musl with cargo zigbuild. No server compilation.

One same-load cohort on47.84.224.186 /47.84.241.200: three cohosted dualrole nodes,
four500/s2500 probes, unchanged limits and binary-pinned probe. LYRNORMQ_RAFT_PROFILE=1,
49Hz finite perf capture with explicit --clockid mono; archive clock header proof.
Compare Phase7s counts, latency, /procCPU/RSS, logical reads/rounds/RPCs, write queues,
batch sizes/timing/overlap and actual WAL counters. Stop after failure, no higher
or message loads this phase. Stop all processes, archive exact failures and evidence.

# Phase7n — first-failure root-cause diagnostics

Scope: diagnose the failed quota-free 1000/s connection cohort before further
pressure. No further large server load, no quota or deadline changes, and no
weaker quorum/ReadIndex/all-live visibility/durable callback guarantees.

Use fixed-cardinality, power-of-two sampled diagnostics for Acquire stages and
cache recovery reason, Controller commit and RPC read/write completion. Include
monotonic elapsed time, outcome and stage; never log client identities, payloads,
credentials or arbitrary commands. Instrumentation must not change success,
retry, ordering or cancellation behavior. Distinguish first trigger from recovery
amplification; CPU/device averages alone do not prove a storage bottleneck.

Coordinator owns local finite reproduction and this report. Runtime owner owns
broker/durable.rs and broker/durable/** only; Control owner owns controller.rs,
controller/**,rpc.rs,rpc/** only. Each writer uses an explicitly assigned separate
worktree/branch. First review audits, then instrument only missing evidence.
Tests must exercise diagnostic behavior and preserve error semantics. Reproduce
with existing real three-process fixture at finite small connection counts,
retaining raw errors and cleaning up processes on all exits. Any fault injection
must be clearly labeled, cannot stand in for reproducing the actual first trigger.
No performance or complete-root-cause claim without observed supporting evidence.

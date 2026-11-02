# Phase 5h — Shared journal IO optimization

Supersedes implementation costs only; phase5f/5g persistence, queue budgets, wire
format, per-rule/client ordering and independent consumer states remain normative.

## Frozen boundaries

- No on-disk format/schema/version changes, weaker sync, delayed ACK, background
  unsynced append or MQTT ACK coupling. Each successful append/state update still
  returns only after production sync_all. Generation publication/directory sync,
  torn-tail recovery, poisoned-owner behavior, v1 migration and atomic fanout remain.
- Keep existing public Journal APIs, QueueOptions and Vec payload return types.
  Production lib.rs stays declarations/reexports only. No unsafe or new dependency.
- Improve frame/checksum CPU and coalesce writes with finite scratch space (at most
  64 KiB per frame write). No persistent buffered bytes across completed calls;
  flush errors must propagate before sync/apply, poisoning writes as before.
- Reuse validated segment descriptors with a finite bound (at most 16). Reads must
  preserve private-file identity/permissions/link checks and body CRC on actual
  reads. Validate file identity/length/modification fingerprint once per segment
  per read call before trusting cache; changed/replaced/truncated files invalidate
  affected body cache and revalidate or fail closed. No lock held across await;
  journal remains a synchronous exclusive owner on its blocking actor.
- Cache only CRC-verified immutable shared result bodies that have multiple live
  consumers. Bound resident payload bytes at 8 MiB and entries at 8192. Oversized
  bodies bypass cache; no payload preloading or cache seed before sync. Cached
  payloads may be copied to preserve the public Vec API. Cross-outbound hits reuse
  a body read, not consumer metadata/state. Final consumer completion and generation
  replacement must invalidate corresponding cache; retry/partial ACK cannot evict
  or acknowledge another consumer. Reopen starts cold and validates durable files.
- Cache storage is a separate fixed bounded allowance, not a change to queue disk
  quotas or existing in-flight runtime budgets. Report memory increase honestly.
  Tests expose private read/hit counters only under cfg(test), no payload API.

## Acceptance

Regression-first tests cover one physical body read across destinations, partial
completion/retry, generation replacement and reopen, finite byte/entry/descriptor
limits, cache eviction, file tamper/truncation/permissions/replacement and CRC faults.
CRC implementation must match independent standard vectors and legacy frame bytes.
Run fmt/strict Clippy/tests, existing shared-stream and durable HTTP crash/restart
blackboxes. Re-run the exact prior 39-case sequential topology matrix using the
unchanged workload/configuration/production sync, preserving baseline artifacts.
Report write result/s, read delivery/s, per-call latency, stage/peak RSS and actual
log bytes, on the same machine/filesystem; no Broker TPS claim or dropped results.

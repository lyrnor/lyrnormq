# Phase3a — evidence-led serialization and wakeup optimization

Frozen AFTER baseline profiling and ACK diagnostics, before optimization writers.
See docs/performance/phase3a/baseline-profile-pilots.json and summary. Full-topology
64-publication/8-warmup pilots include setup/cleanup (Session also startup); spans
are inclusive elapsed, not exclusive CPU. Memory/many:72 input publications,
9504 empty polls/9576 polls,10120 proposal clones(1.829s),21268 live serializations
(12.001s,9180264982 logical bytes),route-build0.109s. Independent2s sample shows
actor ensure_bounds/Serde stacks. Optimize amplification/materialization first;
route-index caching and copy-on-write engine proposals are deferred pending a new
post-optimization profile. Do not claim routing has zero cost or sum overlapping spans.

ACK-only original queue256/full32-subscriber4096 attempt:600s deadline; warmup128
fully delivered; measured545 issued,537 ACK,9133 receipts. No error response captured;
last checkpoint had no worker errors and is incomplete after forced shutdown.
Independent full-topology queue8 experiment captures e0/body97 and worker reason97.
These distinguish observed quota rejection from deadline-only slow progress; neither
retroactively identifies the earlier unrecorded reply. Partial counters do not prove
loss. Keep resource refusal and admission policy unchanged.

## O1 Session serialization (mqtt-session ownership)

Preserve schema7/container1 EXACT canonical JSON bytes/field order/array order,
including Stage Ack/Receipt byte arrays, Comp unit string, explicit nulls, skipped
Will timer_blocked, fallback empty Properties, BTree order and outgoing.order.
Use borrowed record views instead of cloning application/session payloads into an
owned Envelope. RetainedStore::entries may remain owned; no cross-owner Router API
change is authorized. Engine proposal still fully deep-clones: no COW or mutation
sharing that could leak failed proposals into installed state.

ensure_bounds may serialize into a fixed-size counting writer rather than allocate
a Vec, but must enforce the identical exact JSON byte limit. Reject an oversized
write chunk atomically (do not partially accept); preserve IO->Capacity mapping.
Validate outgoing references in the same projection BEFORE any JSON output, keeping
InvalidRecord priority over Capacity/Json. Keep all other invariant/ACL/expiry checks.
snapshot still validates the complete live registry, then serializes persistent
projection. clock_floor is read once at the same logical projection point, never
replaced by effective_now. Keep old owned records+serializer cfg(test) as an independent
oracle; exact bytes/error categories for live and persistent, tight boundaries,
escaped Unicode/properties, queues, all QoS stages, retained, Wills and recovery.

## O2 Conservative delivery notification hint

Add public SessionEngine::delivery_may_be_ready(&self,client_id:&str)->bool:
unknown/offline false; online && (!queued.is_empty() || !replay.is_empty()) true.
Pure lookup only: no clock, ACL hook, cloning, encoding, mutation or cache.
False describes current installed queues, NOT every possible future poll result.
True is NOT authorization/readiness/admission. Deliberately permit false positives
for expiry, denied data, full ReceiveMaximum, oversized packets and leases.

Transport may filter ONLY delivery wake notifications by this hint, AFTER successful
commit/install, full expiry/ACL/Will maintenance and online registration/removal.
Never filter cancel/takeover/close_reason/control replies; never skip PollDelivery,
BeginDelivery, validation, bounds, commits or existing expiry Tick scheduling.
Network already sets needs_poll after handling input and after pending-output send;
retain these paths so queue draining/direct replies/external leases do not depend
on waking idle clients. Released PUBREL replay must stay eligible even when receive
window is full. Do not add tighter window/ACL/time filters or memoize hints.

Session writer owns hint implementation and unit oracle. Transport writer owns
filtered notify/helper/profiling and deterministic regressions; dependency is the
exact method signature above. Quality owns tests/phase3a_* and CI, not either crate.
All three use explicitly assigned independent worktrees/branches. No shared files.

Required transport regressions: only ready recipients wake; initial single wake
still drains multiple QoS0 frames with capacity1; ACK/PUBCOMP frees window; Released
PUBREL replay with full window; direct replies when hintfalse; takeover/cleanstart;
Tick/WillDelay/SessionExpiry/retry creates queue and notifies idle recipient without
recipient PING; blocked lease still rejects revoked ACL, expired message or invalid
shared epoch. Reuse existing exact tests where sufficient; no lowered protocol rules.

## O3 Integration and comparable measurement

Build immutable baseline(profile-only source3917025) and optimized release artifacts.
Run both through SAME latest diagnostic driver, profiling explicitly OFF, same4096
publications/128warmup/256bytes/3repetitions/memory+SQLite/three full topologies,
SessionExpiry60,delivery flow,queue256,write timeout10000,totalcase600. Run serially
AFTER all builds/tests end. Preserve both datasets, distributions, CPU/RSS samples,
hashes/source/toolchains and any failures; historical driver baseline is separate.
Do not treat changed flow/limits as speedup. Profiles ON and statistical samples
are separate pilots, not comparative throughput evidence. Rerun the full ACK-only
stress diagnosis on optimized artifact, preserving quota/deadline/response distinctions.

Workspace fmt,strict all-target Clippy,tests,rustdoc,existing crash/commit-error tests,
new wakeup tests,both protocol regressions,seeded+guided Fuzz and source guards must
pass. Independent review checks borrowed serializer/error oracle, pure hint safety,
notification ordering, matching workload/accounting and remaining limitations.
lib.rs remains only documentation/modules/exports. No storage schema,durability,
QoS/ACK/properties/session/expiry semantics change or speculative protocol fix.

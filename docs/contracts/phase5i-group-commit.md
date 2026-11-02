# Phase5i — Multi-frame append Group Commit and segment comparison

Frozen before parallel log/runtime/quality implementation. Supersedes append IO
costs only; phase5f/g/h frame formats, exclusive ownership, bounded caches, sync and
per-rule/client FIFO remain normative. No MQTT ACK coupling or early durable ACK.

## Journal API and durability

Add `Journal::append_result_groups(groups: &[Vec<NewResult>]) ->
Result<Vec<Vec<ResultIds>>, QueueError>`. Existing append_results API/imports remain.
Each inner vector is one existing Results frame: 1..64 results, existing per-frame
limits. One call accepts 1..4 frames, at most256results, aggregate raw payload8MiB,
aggregate serialized metadata32MiB. No full-index/session clone. Returned vectors
preserve input grouping/order and every frame still atomically includes each
result's full consumer set. No new durable transaction/schema/version.

Preflight ALL frames, bindings, identifiers, aggregate live/count/physical budgets
before any group frame write. InvalidOptions/StaleBinding/Full return no group
frame appended or logical state applied; checkpoint may reclaim before writing.
Preflight/checkpoint failures do not consume result IDs. Bound serialization and
retained data; do not preload queue payloads. Coordinate any interface changes first.

Write frames consecutively. All touched segments must be synced before success;
when a group fits a segment, exactly one data sync_all per group (plus unchanged
name/directory durability). On rotation sync the dirty old segment before closing,
sync created names and final dirty segment; never only sync the last FD and claim
all durable. No intermediate group checkpoint, deletion or manifest switch can
orphan frames. Preflight segment-count limits as well as bytes.

Apply in-memory frame deltas in order only after every required synchronization
succeeds; no success/reply before that point. Any write/flush/sync/apply failure
poisons owner, no success result is returned; restart may contain an uncertain
complete-frame prefix, exactly as existing failed writes. Group is a durability
barrier, NOT a cross-frame all-or-nothing transaction: unacknowledged frames may
recover. Acknowledged results must all recover with same IDs/body/consumer state.
Read cache trusted-prefix rules apply across grouped writes/rotation/checkpoint.
Keep lib.rs declarations/reexports only, no unsafe/new dependency.

## Runtime integration

Blocking actor drains ONLY already queued contiguous Append commands, at most256
results /8MiB raw, with a conservative16MiB raw+metadata admission estimate. Split
into frames at most64results, and raw8MiB per frame. A command that would exceed
bounds is carried without reordering. No idle wait, sleep or batching timer; a
single append proceeds immediately. Heads/Plan/Read/Bind/Begin/Retry/Complete/Close
remain barriers; state-operation sync semantics remain unchanged in this phase.

Map successful IDs back in enqueue order, refresh view and release admissions only
after durable success. Failed write stops actor as existing; cancellation cannot
skip required sync. If the group preflight returns Full, it is safe to fall back to
original per-frame commits in order so aggregation does not cause avoidable loss
near quota; per-frame replies reflect their own durable outcome. No fallback after
uncertain IO/Corrupt. No public HTTP/SQL/MQTT changes or altered resource policy.

## Segment policy and acceptance

Compare existing1MiB with16/64MiB using existing QueueOptions/CLI. Keep production
1MiB default until reproducible results justify a coordinated change; comparison
is not permission to silently weaken sync or alter caches. Root decides after data.

Tests first: sync counts1vs4, full preflight rejection on bad later frame/quota,
failed/partial write, fault after sync, multiple segments synchronized, torn tails,
acknowledged vs uncertain recovery, cache reuse, runtime ordering/barriers/low-load,
Full fallback and cancellation/admission accounting. Independent public-API tests.

Performance: preserve previous39scenario workload source/configuration as control
and do not overwrite artifacts. Separately label groupwidth/segment experiments;
a larger group is NOT the same old64record synchronization boundary. Compare
1/16/64MiB, groupwidth1/4, representative1rule1out/4rules1out/1rule16out,1KiB and
explicitly bounded datasets, three repeats. Report frames/sync, result/ref throughput,
full-process RSS and durable confirmation latency (queued/confirmed timestamps or
explicitly named per-group call latency; never divide batchp99). Use same machine,
Release and production sync, include environment, raw data, limitations. Run fmt,
strict Clippy, workspace and relevant crash/restart blackboxes before integration.

## Coordinated API clarification: borrowed group input

The new API borrows `&[Vec<NewResult>]` rather than consuming the payload vectors.
This lets the runtime safely retry validated per-frame subsets after a no-write
Full without cloning up to8MiB of raw bodies. Journal may allocate bounded prepared
metadata/encoded frames but does not own or retain caller payloads after return.
Existing owned append_results remains source compatible. All three owners must use
this signature; no second competing API or returned-payload error type is needed.

## Coordinated measurement accessor

Add `Journal::append_data_syncs(&self) -> u64`, a saturating process-local count of
successful data sync_all calls in append_result_groups (including its single-frame
append_results wrapper). Increment only after the syscall succeeds, including
rotation syncs. It excludes initialization, directory sync, state transactions and
checkpoint writer syncs; keep that exact name/definition in benchmark JSON. It is
not persisted and is not a replacement for QueueStats or a management endpoint.
Benchmarks subtract the pre-stage count and report real frames per append data
sync, never assume one multi-segment group is one syscall. No durable schema change.

A pure capacity preflight Full leaves the owner usable for smaller fallback frames.
If the pre-write checkpoint itself fails Full, existing checkpoint semantics poison
the owner: fallback immediately receives Io and stops. Do not promise every Full
allows continued writing; no group frames/IDs have been accepted in either case.

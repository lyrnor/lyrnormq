# Phase4b — partition hot-path bounds and proposals

Frozen before implementation. Supersedes Phase3a-optimization's "proposal still
fully deep-clones" and Phase4's implication that every installed transaction must
re-serialize the live projection, wherever they conflict with this document. All
Phase4 fan-out, QoS wait, Will, ACL, storage and identifier rules remain.

Motivation: 2026-10-30 closed-loop QoS0 at 2500 pairs saturated near 48k msg/s with
p99 ~270ms. Fixed-rate runs at 5k–40k msg/s kept p99 ≤ 3.6ms. `perf` on a
frame-pointer jemalloc build attributed the closed-loop CPU mainly to
`ensure_bounds` → `check_live_bounds` / `CountingWriter` / `serde_json` and to
`SessionEngine` clone/`memcpy`.

## B1 Live bounds

`ensure_bounds` still rejects orphan delivery metadata (unchanged). Exact live JSON
size via `check_live_bounds` remains the authority for snapshots and whenever a
cheap **upper bound** on that size would exceed `max_state_bytes`.

When the upper bound is ≤ `max_state_bytes`, the partition may set `live_state_bytes`
to that upper bound without running Serde. The upper bound must always be ≥ the
exact live JSON size; over-counting is allowed (capacity trips earlier), under-counting
is forbidden. Tests must compare upper vs exact on representative engines.

Broker-wide `max_state_bytes` accounting therefore remains safe and may be slightly
conservative under the fast path.

## B2 Proposals

Engine proposals remain deep clones (Phase3a). In-place mutation is deferred until
a rollback path can preserve broker-wide `max_state_bytes` accounting on install
failure without retaining a second copy.

## B3 Subscription index updates

`install` refreshes `SubscriptionIndex` only when the transaction may have changed
that client's filter set (CONNECT / SUBSCRIBE / UNSUBSCRIBE / session removal).
Delivery-only and publish-only installs must not rebuild filter maps.

## B5 Housekeeping fast paths

Authorization reconciliation re-validates bounds only when it changed state; every
other mutating path still ends in `ensure_bounds`. Shared-subscription pruning may
return "unchanged" without allocating when no session holds `$share/` filters,
shared queue/outgoing metadata, cursors or epochs. Encoders may preallocate; wire
bytes are unchanged.

## B4 Non-goals

No change to per-recipient fan-out, Will at-most-once, schema8 rows, or the
connection task's PollDelivery / BeginDelivery round trips (those are later work).
No claim of MQTT features beyond existing Phase4 scope.

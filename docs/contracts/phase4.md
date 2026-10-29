# Phase4 — per-session actors without the global worker

Frozen before implementation. Supersedes the single serialized worker, the global
cross-session proposal and the singleton snapshot container wherever they conflict
with this document. All earlier protocol, QoS, expiry, Will, ACL, alias, enhanced
AUTH, packet-size and identifier rules remain unless explicitly changed below.
Motivation: docs/performance and the 2026-10-29 private 10k-connection run showed
one worker thread saturated by per-command full-engine clone and full JSON
serialization (connect cost grows linearly with live sessions; PINGREQ starved).

## A1 Architecture

Every client identifier is owned by exactly one tokio session actor. The actor holds
a **partition**: a `SessionEngine` containing only that client's Session and/or Will
record. Partitions keep the existing propose/validate/commit/install discipline,
so clone, bounds and snapshot cost depend on one session, never on the broker size.
The connection task keeps socket IO (reader, complete-frame writer, credits, topic
aliases) and forwards requests to its session actor over a bounded channel.
Pre-session work (assigned identifier, enhanced AUTH initial exchange, connect
policy, credential verification) runs in the connection task; credential checks run
on the blocking pool behind a bounded semaphore. No lock is held across await.

Shared structures, accessed synchronously:
- ClientRegistry: sharded map client -> actor inbox, actor id and current online
  generation; enforces max_sessions on actor creation; replaces the global online map.
- SubscriptionIndex (mqtt-router): incremental filter trie of client filters and a
  shared-group table (members plus round-robin cursor). Actors publish their filter
  set after each installed transaction. It routes candidates only; every recipient
  re-checks ACL, No Local, RAP, identifiers, packet caps and quotas in its partition.
- ConcurrentRetained (mqtt-router): bounded retained store with per-topic metadata.
- StorageService (mqtt-storage): one OS thread owns SQLite, group-commits queued row
  writes in one IMMEDIATE transaction and acknowledges each write after commit.

## A2 Publication and fan-out semantics (changed)

A partition never routes to other sessions. publish_from emits a Publication
(topic, payload, QoS, retain, properties, absolute expiry, publisher, expired flag).
The publishing actor then: applies retained update/delete to ConcurrentRetained
(persisting the retained row), matches the index, and sends one Deliver per ordinary
recipient plus one per shared group to a selected member. A recipient applies the
delivery as its own transaction (`deliver`), committing before acknowledging.

- Per-recipient admission: a full queue, oversize copy, denied ACL or full inbox
  drops only that recipient's copy (counted), never the whole publication.
- Client QoS1/QoS2 publications (PUBLISH QoS1, PUBREL) wait for every recipient
  acknowledgement, then re-run the same request on a fresh proposal (discarding the
  regenerated publications), commit and only then send PUBACK/PUBCOMP. A crash in
  between can duplicate (at-least-once), never silently lose an acknowledged message.
- QoS0 publications, Wills and other maintenance publications commit first and fan
  out without waiting. A crash in that window may lose that fan-out (Will at-most-once).
- Shared groups: members are tried in round-robin order, online first, persistent
  offline members only for QoS>0; a rejection tries the next member.
- Retained capacity failure rejects a client QoS>0 publication with QUOTA_EXCEEDED
  before any delivery; for QoS0/Will it is counted and the delivery proceeds.
- Ordering per publisher->subscriber pair follows the bounded FIFO inbox; there is
  no global cross-publisher order. Cross-session atomic outbox transactions end.

## A3 Quotas, time and authorization (changed)

max_sessions counts actors. max_state_bytes bounds each partition's exact live JSON
and the sum of installed partition byte counts (atomic accounting, retained store
separately bounded). ClockFloor is per partition; restore raises every partition to
the maximum persisted floor. Partitions arm their own timer from next_expiry_deadline.
AccessControl is assumed immutable for a process run (StaticPolicy): reconciliation
runs per partition on restore and on each of its own transactions, plus recipient
checks at delivery and transmission; there is no global sweep.

## A4 Storage schema8 / container2 (changed)

Tables: `meta(key TEXT PRIMARY KEY, value BLOB NOT NULL)`, `sessions(client_id TEXT
PRIMARY KEY, data BLOB NOT NULL)`, `retained(topic TEXT PRIMARY KEY, data BLOB NOT
NULL)`; user_version 2, same application_id, WAL/FULL and `.broker-lock` sidecar.
Each row is a schema7 envelope fragment: a session row holds that client's
persistent session and/or Will; a retained row holds exactly one retained record.
Partitions without persistent state delete their row. Unknown objects fail closed.
A container1 database (`broker_snapshot`) is decoded with the existing schema1..7
chain, split into rows and replaced in ONE transaction before serving; failure
leaves the old database untouched. `rmqtt export-state --storage-path P` prints the
merged schema7 envelope for inspection and tests. Commit failure stops the broker.

## A5 Ownership, verification and limits

Session: partition API, publication/delivery split, row encode/decode, restore
split. Router: SubscriptionIndex, ConcurrentRetained. Storage: RowStore,
StorageService, migration. Transport: registry, actors, fan-out, startup, removal of
Operation worker/run_commands. Quality: tests use export-state helpers; fan-out
tests assert per-recipient semantics; connection-scale benchmark. lib.rs stays thin.
The transport RMQTT_PROFILE worker diagnostics retire with the worker; session
profiling remains. fmt, strict clippy, all Rust/Python suites and fuzz must pass.
Report only measured results; no MQTT compatibility claim beyond existing evidence.

# Engineering rules
Use Rust stable, edition 2024. Production code: no unsafe, unwrap or expect; typed errors, bounded resources, no blocking IO in async tasks, no locks across await. Run cargo fmt, clippy --all-targets -- -D warnings, and tests. Respect docs/contracts/phase1.md and superseding docs/contracts/phase2a.md and docs/contracts/phase2b.md and docs/contracts/phase2c.md and docs/contracts/phase2d.md and docs/contracts/phase2e.md and docs/contracts/phase2f.md and docs/contracts/phase2g.md and docs/contracts/phase2h.md and docs/contracts/phase2i.md and docs/contracts/phase2j.md and docs/contracts/phase2k.md and docs/contracts/phase2l.md and docs/contracts/phase2m.md and docs/contracts/phase2n.md and docs/contracts/phase2o.md and docs/contracts/phase3.md and docs/contracts/phase3a-profiling.md and docs/contracts/phase3a-optimization.md and docs/contracts/phase4.md and docs/contracts/phase4b.md; propose interface changes to coordinator first. Each agent writes only its owned paths in an explicitly assigned independent Git worktree and branch, commits tested changes, reports hash and limitations. Do not edit shared root Cargo.toml or other modules. Regression tests precede bug fixes. Never claim unimplemented MQTT compatibility.
Ownership: protocol crates/mqtt-protocol; transport crates/mqtt-transport; router crates/mqtt-router; session crates/mqtt-session; storage crates/mqtt-storage; quality tests, benches, fuzz, .github/workflows. Auth specialist owns crates/mqtt-auth. Coordinator owns bin/rmqtt, workspace and docs.

Structure: lib.rs contains crate documentation, module declarations and public re-exports only. Put data types, business functions, impl blocks and test bodies in responsibility-specific modules. Preserve existing public import paths and durable schema during structural refactors.

TLS listeners additionally follow docs/contracts/phase4c-tls.md. Cluster implementation
follows docs/contracts/phase7a-cluster.md; the historical partition-directory
proposal is superseded. Cluster owner owns crates/mqtt-cluster, coordinator owns
crates/mqtt-cluster-core and CLI; transport and quality retain their existing paths.
Durable forwarding and persistent-session failover additionally follow
docs/contracts/phase7b-durable-cluster.md. Runtime owns mqtt-cluster; transport owns
mqtt-transport; coordinator owns the neutral core API. Do not put session bodies in
Controller Raft or separate receipt persistence from recipient state transactions.
Replica repair additionally follows docs/contracts/phase7c-replica-repair.md.
Repair runtime owns mqtt-cluster except src/replica.rs; repair storage owns that
file only; coordinator owns Cargo/CLI/docs. Uncaught-up/replacement disks never
vote, including startup/restart windows; persistent generation fences survive thaw.
Role cohosting and topology measurement follow docs/contracts/phase7d-cluster-benchmark.md.
Coordinator owns CLI/loadgen integration; quality owns topology/probe tests and CI.
Failed warmup is not a zero-loss or zero-latency measurement; preserve raw failures.
Data-path diagnostics/backpressure follows docs/contracts/phase7e-data-path-diagnosis.md;
preserve execution/body/actor limits, finite cancellable waiting and generation fencing.
WS/WSS additionally follows docs/contracts/phase4d-websocket.md, including binary
stream framing, strict mqtt subprotocol, finite buffers and bounded closing.
Authentication admission and mandatory client certificates follow
docs/contracts/phase4e-admission-mtls.md. Metrics/health/reload/multi-listener and
performance acceptance are outside that phase's explicitly limited user scope.

Read-only management and React Dashboard additionally follow
`docs/contracts/phase4f-dashboard.md`. Dashboard owner owns dashboard/web/**;
coordinator owns management HTTP/CLI in bin/rmqtt. Do not reintroduce prototype
sample data into the real UI, expose credentials/payloads, or mutate sessions
through read endpoints. Preserve bounded metadata queries and MQTT hot paths.

SQL/Webhook additionally follows docs/contracts/phase5a-rules-webhook.md. Rules owner
owns crates/mqtt-rules; Webhook owner crates/mqtt-webhook; Dashboard owner frontend.
Rule admission is bounded best effort; durable outbound acceptance follows phase5f. Do not claim MQTT ACK/outbound transaction coupling.

Rule configuration SQLite additionally follows `docs/contracts/phase5b-rules-sqlite.md`.

Unified rules/outbounds follow `docs/contracts/phase5c-unified-outbounds.md`, which
supersedes the connector/action model. Egress owner owns crates/mqtt-egress and
crates/mqtt-egress-core; Webhook owns crates/mqtt-webhook as a protocol adapter only.
Generic scheduling must not depend on HTTP or import reqwest.

Bounded outbound concurrency additionally follows `docs/contracts/phase5d-outbound-concurrency.md`.

Client-ordered outbounds additionally follow `docs/contracts/phase5e-client-ordered-outbounds.md`.

Generic durable outbound queues additionally follow `docs/contracts/phase5f-durable-outbound-queue.md`. Storage owner for this phase owns crates/mqtt-egress-log; coordinator also owns the small Dashboard status integration.

Shared rule-result streams and per-destination consumers supersede per-delivery bodies via `docs/contracts/phase5g-rule-result-streams.md`; log owner owns crates/mqtt-egress-log, runtime owner crates/mqtt-egress, quality owns tests/benches/CI in separate worktrees.

Journal IO optimization additionally follows `docs/contracts/phase5h-journal-io-optimization.md`, including prevalidated owned-append cache retention, finite caches and unchanged durable frames/sync boundaries.

Multi-frame append Group Commit follows `docs/contracts/phase5i-group-commit.md`; retain per-frame atomicity, all-touched-segment sync before replies, borrowed payloads, FIFO barriers and bounded no-wait aggregation. Do not equate group atomic durability with a cross-frame transaction or logical group calls with sync syscalls.

Kafka outbounds follow `docs/contracts/phase5j-kafka-outbound.md`. Backend owns crates/mqtt-kafka, crates/mqtt-egress and the coordinated mqtt-egress-core timeout/shutdown extensions; UI owns dashboard/web, quality owns tests/CI. Reuse generic durable logs, wait for Kafka delivery reports, preserve native lifecycle fencing before generation replacement, and verify the locked amd64 musl build.

Kafka SDK replacement additionally follows docs/contracts/phase5k-krafka-migration.md.
Backend owns crates/mqtt-kafka only; quality owns Kafka tests/CI; coordinator owns
lock/docs/build integration. Preserve public and durable schemas, bounded lazy
connection, confirmed delivery, same-client FIFO and SDK lifecycle fencing.

HTTP access work additionally follows docs/contracts/phase6b-http-admission.md, which
supersedes phase6a receive/shared/recovery expansion for the narrowed user scope.

Subscribable client lifecycle system topics additionally follow
`docs/contracts/phase6d-system-client-events.md`. Transport owns bounded broker
lifecycle emission and dispatch; session owns unconditional client system-publication
and Will guards; quality owns independent wire tests. No schema_version field in
event JSON, no retained presence, no implied crash-complete event journal.

Privileged HTTP downlink publishing additionally follows
`docs/contracts/phase6e-http-publish.md`: same management Bearer token, bypass MQTT
source authentication/ACL; no Basic/cache or synthetic MQTT connection. Publisher
host handle is separate from read-only management. Preserve recipient semantics,
reserved system-topic guards and bounded admission.

Cluster data replicas additionally follow `docs/contracts/phase7f-memory-replicas.md`,
which supersedes Phase7b/7c disk guarantees for the data plane. Data replicas use
bounded typed RAM; Controller Raft/configuration SQLite remains. No SQLite session
mirror in replicated mode. A fresh boot is a new replica identity; catch up in the
same running process before voting. Whole-group RAM loss cannot recover from
control metadata. RocksDB is a future backend, not implemented.

Event-driven replicated forwarding additionally follows
`docs/contracts/phase7g-event-forwarding.md`. Actor commits notify bounded keyed
workers; no periodic full-directory message scan. Preserve FIFO per key, retry
state, caller-cancellation safety and metadata/readiness-triggered recovery.

Control-plane optimization additionally follows
`docs/contracts/phase7h-control-optimization.md`. Keep Raft WAL/vote/committed
watermarks durable, replay committed tails only, snapshot before log purge. Normal
metadata changes and watches are sparse; applied barriers use bounded revision
notifications and preserve all-live-node visibility. Coordinator additionally owns
this phase's tests/cluster_control_optimization* and CI/preflight integration.

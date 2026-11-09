# Phase5k — krafka Kafka outbound replacement

Frozen 2026-11-02 before implementation. Supersedes phase5j SDK/native build and
lifecycle details only. Preserve public KafkaConfig/KafkaSink imports, persisted
configuration and journal schemas, key selection, raw SQL JSON values, stable
headers/timestamps, Murmur2 including empty keys, size checks, generic bounded
scheduling, at-least-once restart semantics, and per(outbound,rule,Client) FIFO.
No UI schema changes. HTTP access-control work is independent and preserved.

## SDK and resource contract

Pin krafka =0.28.1, default-features=false, ring + zstd + tls-encrypted-keys + native-tls-roots.
No production rdkafka/librdkafka/OpenSSL/curl dependency. Independent test clients
may retain rdkafka as an interoperability oracle. Use configured inline CA exclusively, otherwise load platform-native trust roots
through the SDK blocking worker, preserving existing system CA behavior.
PEM remains in memory; retain PLAIN/SCRAM256/SCRAM512 and TLS/mTLS, mandatory
hostname verification and existing encrypted PKCS8 fixture compatibility. Reject
unsupported encrypted formats explicitly, never silently ignore key passwords.
Keep synchronous KafkaSink::new local/bounded and configuration validation usable
while Kafka is offline; async connection and InitProducerId are lazy and bounded.

Explicit acks=All, idempotent=true, same configured linger/batch/compression,
8MiB SDK buffer, <=512 admitted records, <=16 active/warming owners, <=8 active
outbounds, existing global512 attempts. Bound frame/metadata/connection resources.
The existing Sink interface is unchanged; manages_timeout stays true. Every send
has one absolute policy deadline including lazy connect, enqueue and final delivery.
Reserve delivery time: do not add enqueue + delivery windows beyond policy bound.
Do not cancel an enqueued record then retry it while the SDK still owns it. Keep
DeliveryHandle through final outcome, close/fence SDK when an outer deadline makes
outcome uncertain, retain journal delivery ID, and return retryable uncertainty.
Only confirmed Kafka append or Kafka deduplication acknowledgement completes the
journal; enqueue, possibly_written timeout, or acks=0 are never delivery success.

Configuration replacement/shutdown must fence old SDK network tasks before new
producer activation; concurrent close callers await the same completion. Cleanup
is bounded (5s existing contract); on failure report unavailable and do not claim
fenced. Dropping adapter must schedule teardown, not leave an orphan sender.
Do not log provider error strings, credentials, bodies or private keys. Map errors
to existing controlled auth/tls/topic/size/backpressure/delivery codes. No new
exactly-once or cross-restart Kafka deduplication claim.

## Ownership and integration

Backend owns crates/mqtt-kafka/** in /private/tmp/lyrnormq-krafka-backend,
branch codex/krafka-backend. Keep lib.rs docs/modules/reexports only.
Quality owns new/updated tests/kafka* and .github/workflows/kafka-acceptance.yml
in /private/tmp/lyrnormq-krafka-quality, branch codex/krafka-quality. Retain existing
real Apache Kafka tests; no weakening to accommodate SDK differences. Add actual
adapter benchmark with independent readback and precise counts/latency definitions.
Read-only reviewer audits pinned SDK and implementation, writes no source.
Coordinator owns root lock, docs, baseline/final builds, integration and acceptance.
Each writer commits tested owned paths only; no shared root manifest/lock commits.
Interface changes require coordinator contract update before implementation.

## Acceptance

1. Regression tests cover keys/headers/raw payload, bounds/backpressure, delivery
   unknown/timeout, idempotent ACK loss, shutdown/reconfiguration fence and races.
2. Existing real Kafka plaintext/TLS/mTLS/encrypted PKCS8, PLAIN/SCRAM256/512,
   MQTT311/5 QoS0/1/2, per-client FIFO, retry/outage, graceful restart/SIGKILL,
   stable IDs and shared durable rule streams pass with independent rdkafka reader.
3. Profile-equivalent before/after native binaries and Linux amd64 musl builds;
   validate ELF static/no dynamic dependencies and cargo tree no production openssl.
4. Same bounded direct-adapter load, <=1MiB messages, hotspot and spread keys;
   report attempted/acknowledged/readback counts, errors, exact latency boundary,
   throughput, p50/p95/p99, RSS and environment, no fabricated or cross-host claims.
5. fmt, strict workspace all-target Clippy/tests, appropriate existing regressions.
   Unit fake broker is fault injection, never called real Kafka interoperability.

## Reviewed timeout and offline TLS refinement

The absolute DeliveryPolicy deadline covers queueing, lazy init, enqueue and
confirmed delivery. Error reporting may take up to five additional seconds to
fence an uncertain SDK generation. Owner::send must bound reply waiting by this
combined budget. On cleanup overrun it marks the owner stopped and returns a
controlled retryable shutdown error, without claiming a successful fence. Retain
command byte/record permits and global owner slot until actual runtime destruction;
no fresh SDK work is accepted and generation replacement still requires real done.
SDK blocking DNS, native-root and compression tasks cannot be forcibly killed.
No false bounded physical teardown guarantee is made when those OS tasks stall.

Offline constructor validation checks strict certificate-only PEM, matching
certificate/private key and encrypted PKCS8 before lazy network IO. PBKDF2 is
bounded to100000 iterations, salt64B; scrypt N<=16384,r<=8,p<=1 (16MiB), key64B.
Traditional encrypted PEM or unsupported/expensive schemes reject explicitly.
No persisted TLS/configuration format changes. Error-code text remains controlled.

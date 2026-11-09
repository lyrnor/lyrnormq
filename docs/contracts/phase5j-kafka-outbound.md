# Phase5j — Kafka outbound adapter and Dashboard

Frozen after a successful rdkafka zigbuild spike, before parallel implementation.
Phase5c–i shared durable queue/Group Commit/cache/state contracts remain normative.
No new per-outbound log, SQL semantics, MQTT ACK coupling or rule-body duplication.

## Build gate and ownership

Pin rdkafka=0.39.0, default-features=false, features tokio,cmake-build,curl-static,
ssl-vendored,libz-static,zstd. Linked librdkafka2.12.1. CMake without curl fails on
an upstream OIDC macro/header condition; configure/make misdetects host compiler.
The complete vendored feature set built a static amd64 Linux musl ELF with no
PT_INTERP/DT_NEEDED. Same-feature host probe verified ssl,sasl,sasl_plain,sasl_scram,
lz4,zstd,gzip. Evidence docs/verification/kafka-outbound-20261102/build-probe.
No claim that the cross probe was executed on Linux. Final integrated Broker must
also zigbuild --locked successfully; no server-side compilation/system lib dependency.

Coordinator owns root Cargo/lock, bin/lyrnormq, docs/integration. Backend owns new
crates/mqtt-kafka/**, crates/mqtt-egress/** and crates/mqtt-egress-core/** for this
phase. UI owns dashboard/web/**. Quality owns tests/**,.github/workflows/** and
standalone test helper manifests. Each uses an assigned worktree+branch, commits
only owned sources; generated root lock changes are integrated by coordinator.
lib.rs remains docs/module declarations/reexports only; no own unsafe or blocking
native create/destroy on async workers. Transitive native FFI is the chosen library.

## Configuration wire contract

Extend schema_version2's existing outbounds union with type:"kafka". Old Webhook
shape, defaults, saved credentials and rules bindings remain compatible. SQLite
continues to store configuration, generic journal continues to store messages.
No rules.json or second connector/action model. Older binaries do not understand
Kafka configs and must fail closed rather than silently discard them.

Kafka config (deny unknown fields, defaults below; optional fields null/omitted):

```
{"brokers":["localhost:9092"],"topic":"mqtt-events","key_field":null,
 "security":{"protocol":"plaintext","sasl_mechanism":null,"username":null,
             "password":null,"ca_pem":null,"certificate_pem":null,
             "private_key_pem":null,"private_key_password":null},
 "compression":"lz4","linger_ms":1,"batch_bytes":65536}
```

- brokers1..16, each explicit hostname:port or [IPv6]:port, ASCII <=255bytes,
  no URL/userinfo/path/query/control/whitespace. Ports1..65535, list bounded.
- topic1..249bytes matching Kafka ASCII letters/digits/._-, excluding `.` and `..`.
  Fixed topic in this phase; dynamic Topic templates unsupported.
- key_field optional top-level SQL-result field name1..128UTF8bytes, no controls;
  absent means exact source MQTT Client ID from trusted envelope metadata.
  Field selection requires a string value, absent/null/nonstring is permanent error.
- protocol plaintext|ssl|sasl_plaintext|sasl_ssl. SASL requires mechanism exactly
  PLAIN|SCRAM-SHA-256|SCRAM-SHA-512, nonempty username/password <=1024bytes, no NUL.
  TLS supports optional CA PEM and paired certificate/private-key PEM <=16KiB each,
  optional key password <=1024bytes. Reject irrelevant fields for chosen protocol,
  incomplete cert/key pairs and malformed values. No server file paths or arbitrary
  native options. Server certificate and hostname verification are mandatory.
- compression none|lz4|zstd. linger_ms0..100; batch_bytes16384..1048576.
- Kafka DeliveryPolicy: concurrency default128 if omitted,1..512; batch_size=1,
  batch_wait_ms=0; queue_capacity remains system-managed256 for new UI objects;
  timeout_ms1000..30000(default30000 UI), max_attempts1..5(default3), retry_base_ms
  10..10000(default200). Preserve Webhook's default8/max16 and3000ms UI timeout.
  Non-Kafka omissions retain old serde behavior. Validate full64KiB config limit.
- At most8 enabled Kafka outbounds; max16 native producer owners process-wide to
  cover active+warming generations. All other existing config limits remain.
- Redact password/private_key_pem/private_key_password as [redacted], restore only
  matching existing outbound/type fields; unknown placeholders invalid. Debug,
  status and native log callbacks must never print payloads, secrets or raw errors.

## Kafka records and delivery

One shared rule result produces one Kafka record per Kafka consumer reference.
Value is raw JSON bytes of the SQL-projected data, NOT Webhook's batch envelope.
Reuse existing trusted WireEvent encoding for adapter input, parse its metadata
separately from SQL data; no persistent schema change. Key defaults to client_id;
optional key_field selects SQL data only for explicit partition policy.

Emit stable lyrnormq-delivery-id,lyrnormq-event-id,lyrnormq-rule-id,lyrnormq-client-id headers
(and original topic as lyrnormq-topic). Do not change IDs on retries/recovery. Use a
stable keyed partitioner (murmur2, including empty keys). Preserve timestamp_ms if
valid. One record's value/key/headers plus conservative protocol overhead must fit
1MiB; oversize permanently fails before enqueue. Same rule/client has one attempt
in flight through its delivery result; other rules/clients run independently.

Use one FutureProducer per enabled outbound, shared across clients. Fix acks=all,
enable.idempotence=true,max.in.flight.requests.per.connection=5. No user override
that disables guarantees; no transactional.id/exactly-once claim. Set bounded native
queue.buffering.max.messages=512 and queue.buffering.max.kbytes=8192; QueueFull is
backpressure/retryable, never success. SDK timeouts derive from DeliveryPolicy,
request timeout/linger validated to fit delivery timeout. SDK retries occur only
inside that bounded delivery deadline; generic retries start after final result.

Runtime Kafka attempt pool global512, separate from existing Webhook global16;
per-outbound policy and existing resident byte budget also bound jobs. SDK batches
across messages/partitions internally, but engine batches remain1 to avoid partial
batch ACK ambiguity. Do not make all clients wait for one HTTP-style serial worker.

Only Kafka successful delivery report permits journal Complete(Delivered).
Native enqueue success is insufficient. Network/timeouts retry current rule/client;
auth/topic/size/config failures classify redacted permanent/retryable explicitly.
Terminal retry exhaustion counts failed then continues next lane entry as before.
Delivery is at-least-once: Kafka-success/local-completion crash window may replay,
even with idempotence. Consumer can deduplicate stable delivery header. Broker acks
also depend on topic replication/min.insync.replicas; no claim of physical powerloss.

Ordering is per(outbound,rule,exactClient) attempt FIFO, default stable-key Kafka
partition order while mapping unchanged. Different rule streams can interleave.
Custom key changes/partition-count changes and uncertain remote outcomes cannot
promise global consumer order or exactly-once across crashes; phase5e remote-effect
limitation remains. No global order or cross-partition order claim.

## Lifecycle/adapter extensions

Add default methods to generic Sink:
`fn manages_timeout(&self)->bool {false}` and
`fn shutdown(&self)->Pin<Box<dyn Future<Output=Result<(),SendError>>+Send+'_>>`
(default immediate Ok). Kafka returns true: worker must NOT drop an enqueued native
send future on an independent outer timer and immediately retry; native delivery
report owns the bounded deadline. Webhook keeps current timeout behavior.

Generation cancellation joins jobs, then awaits sink shutdown before activating
replacement. Kafka enqueueing must stop, native pending requests must be purged/
settled and producer destroyed on a blocking/native owner thread; timeout/failure
stops outbound runtime rather than starting a competing generation. Shutdown is
idempotent and bounded (overall5s native cleanup deadline after existing job joins).
Do not hold locks across await or leave unbounded per-record tasks/threads.

Preferred native lifetime pattern: a bounded dedicated owner per producer retains
the last native Arc, while send_result borrows under a short mutex (no native clone
across await); shutdown/drop removes adapter handle and signals owner, which purges,
flushes with finite deadline and drops native off Tokio. Await completion via async
notification, do not block join on Tokio. Global owner slots retained until native
cleanup actually completes bound cancelled/failed-start/reconfiguration churn.
Equivalent implementations require coordinator review first.

## Dashboard and acceptance

Reuse shadcn components and existing unified outbound form/list. Add Kafka type,
Name/Brokers/Topic/security basic fields; conditional SASL credentials and TLS CA/
client cert/key fields. Key field selection belongs advanced settings, default
Client ID. Explain custom-key/partition ordering meaning, not SDK implementation.
Advanced collapsed: concurrency, delivery timeout/retries, compression, linger,
batch bytes. Hide queue sizing, fixed reliability knobs, SDK names/version/internal
logs/offsets. Auto-generate IDs, no credentials in browser storage. Existing redacted
secret edits retain values; type switch resets to matching defaults/validators.
List target column becomes generic, showing broker/topic or Webhook URL safely.

Regression-first tests: key/value/headers and boundaries, redaction/restoration,
independent protocol concurrency, same-client FIFO with failure/backoff while another
client progresses, QueueFull/timeout/permanent failures, stable IDs, native shutdown
and cancelled config/startup cleanup, unchanged Webhook behavior. Run actual Apache
Kafka integration (not only mocks) for MQTT→SQL→journal→Kafka, multi-rule/fanout,
plaintext/TLS/SASL, graceful restart and SIGKILL uncertain replay. Deterministic native
mock tests may supplement faults but must be labelled. No real Kafka server locally:
use checksum-verified temporary JRE/Kafka archives, no production-server installs.

Run fmt, strict workspace+helper Clippy, Rust/frontend tests, embedded Release,
real blackboxes and final locked Linux amd64 musl build. Record actual environment,
commands, results and limitations. Do not invent performance data or compatibility.

Internal producer client.id is derived as lyrnormq-{outbound_id}, not a new user-facing
SDK option. KafkaSink constructors validate explicit concurrency1..512 even when
called outside Config validation. Native authentication callbacks may retain only
structured error flags for classifying a final failed delivery; never log native
strings or turn pending enqueue into early successful/terminal completion.

Upgrade compatibility gate: existing Webhook Outbound serialization, including typed
DeliveryPolicy declaration field order, remains byte-for-byte unchanged because
durable Binding descriptors compare those bytes. Kafka-specific defaults may use
a read-only wire helper; never route existing serialization through sorted JSON
Value objects. Both legacy v1 fixture recovery and pending v2 queue restart must
pass unchanged. Do not weaken binding identity checks or change the durable schema.

# Phase5a — SQL rules and Webhook outbound

Frozen before parallel implementation on 2026-10-30; this consolidated version
includes the coordinated numeric, delivery identity, fail-closed and pool-lifetime
clarifications made during review. Earlier revisions remain in Git history.

## Scope and delivery boundary

Native SQL and Webhook only. No plugins, Kafka, PostgreSQL, aggregation or windows.
This phase implements a bounded **best-effort asynchronous side path**. MQTT ACKs,
QoS/session/outbox transactions and durable schemas are unchanged. Rule errors,
full queues and slow Webhooks do not block or modify ordinary MQTT routing.

Configuration persists; event queues, action tasks and counters do not. Crash,
overflow or configuration replacement may abandon work, with applicable drop/error
counters. There is no durable event admission, durable action outbox, persistent
dead-letter replay or external exactly-once guarantee.

## SQL core: mqtt-rules

Public types (serde configuration types reject unknown fields):

```text
Event { topic:String, payload:Vec<u8>, client_id:String,
        qos:u8, retain:bool, timestamp_ms:u64 }
Rule { id:String, name:String, sql:String, enabled:bool, action_ids:Vec<String> }
CompiledRule::compile(&str) -> Result<CompiledRule, RuleError>
CompiledRule::matches_topic(&self, &str) -> bool
CompiledRule::topic_filter(&self) -> &str
CompiledRule::evaluate(&self, &Event) -> Result<Option<serde_json::Value>, RuleError>
```

Use sqlparser AST parsing and strict whitelisting, never database execution. SQL
compiles at configuration/test time. Exactly one SELECT from one double-quoted MQTT
Topic Filter; reuse ordinary MQTT wildcard/system-Topic rules, not shared groups.
Support aliases, SELECT *, optional WHERE, metadata fields client_id/topic/qos/
retain/timestamp, payload JSON dot paths, comparison operators, AND/OR/NOT, IS NULL,
IS NOT NULL, parentheses, unary signs, + - * /, COALESCE/LOWER/UPPER. Reject joins,
subqueries, DML, ordering/grouping/windows, arbitrary functions and regex.

Limits: SQL8192 bytes,64 projections,256 expressions, depth32, input/output1MiB.
Duplicate output names are invalid. Metadata-only rules support binary payloads;
JSON is parsed lazily once within an evaluation. Invalid JSON/missing/incompatible
fields yield NULL; WHERE passes only boolean true. SELECT * returns metadata and
parsed payload (null for invalid JSON). Arithmetic errors/division by zero are
explicit. Numeric operands/results outside exact ±(2^53−1), or nonfinite numbers,
fail; direct JSON field projection does not convert values unnecessarily.

## Nonblocking transport observer

```text
pub trait PublicationObserver: Send + Sync + Debug {
    fn observe(&self, topic:&str, payload:&[u8], client_id:&str, qos:u8, retain:bool);
}
BrokerConfig.publication_observer: Option<Arc<dyn PublicationObserver>> = None
```

Observe accepted publication dispatch, including Will, after existing admission/
authorization. Skip expired flag and absolute-expiry-deadline inputs for rules
without altering preexisting MQTT routing. Subscription retained replay/outbound
retransmission do not trigger this hook. QoS1 input/recovery can duplicate; DUP is
not an application deduplication key. Hook performs bounded try-admission only:
no IO, await, SQL/JSON computation or unbounded payload allocation.

## Runtime: mqtt-webhook

```text
Runtime::open(PathBuf) -> async io::Result<Arc<Runtime>>
Runtime::observer(&self) -> Arc<dyn PublicationObserver>
Runtime::configuration(&self) -> async PublicConfig
Runtime::replace(&self, Config) -> async Result<PublicConfig, RuntimeError>
Runtime::test(&self, &str, Event) -> async Result<Option<Value>, RuntimeError>
Runtime::status(&self) -> async Status
Runtime::shutdown(&self) -> async ()
RuntimeError = InvalidConfig | InvalidSql | Busy | Storage | Stopped
```

Config contains connectors, actions and rules. Connector has id/name/url/headers.
Action has id/name/connector_id/enabled/batch_size/batch_wait_ms/queue_capacity/
timeout_ms/max_attempts/retry_base_ms. PublicConfig is the same shape with every
header value replaced by [redacted]. Round-trip replacement preserves old secret
values for that placeholder; new credentials cannot use it. References must exist,
IDs must be unique, duplicate rule-action bindings invalid. Disabled actions are
valid references but do not dispatch.

| Configuration limit | Value |
|---|---|
| Total serialized config |64KiB|
| Connectors / actions / rules / bindings |32 /64 /128 /512|
| ID / name |1..64 ASCII alphanumeric/_/-;1..128 UTF-8 bytes|
| Connector custom headers |16 entries,8192 total bytes|
| Batch size / wait |1..500 events;0..1000ms|
| Action queue |1..4096 copies|
| Request timeout / attempts / retry base |100..30000ms;1..5;10..10000ms|

URLs allow http/https only, with no userinfo/query/fragment. Credentials belong in
headers. Reject CRLF, invalid/duplicate case-insensitive header names and transport
headers (Host, Content-Length, Content-Type and hop-by-hop/proxy headers). Mark
custom HeaderValue values sensitive for debug/tracing. Errors never include headers,
SQL/Payload content, secret paths or credentials.

Load/create regular config files on the blocking pool, reject symlinks/oversize,
write0600 same-directory temporary file, sync file, rename and sync directory before
normal success. Compile/validate and construct new HTTP client before persistence.
Mutation admission is serialized and retained through caller cancellation. If rename
has published but directory sync fails, install the visible configuration, stop
rule execution, cancel generations, increment error status and return Storage.
Further mutation/test rejects Stopped until restart. Never claim file unchanged or
successful durability in that case; published config remains inspectable.

### Execution and resource bounds

Four dedicated OS-thread SQL shards,256 input slots each, hash by client_id to
preserve one publisher's processing order. Different shards execute in parallel.
Immutable compiled candidate index prefilters Topic before payload cloning; no
index rebuild or SQL parsing per message. Administrative compilation/tests also
have bounded jobs off Tokio network threads. Observer uses nonblocking snapshot
admission and records contention/queue/size rejection.

Global logical queued budget16MiB includes input and action copies, retained until
processing/send/retry/cancellation releases them. Each action owns one bounded
queue/worker, initially one in-flight request. Global HTTP concurrency16. Config
replacement cancels old generation and counts abandoned copies; serialized updates
bound coexistence to old+new workers. HTTP Client/pools belong to a generation,
not a process-global pool retaining arbitrarily many past endpoint hosts. Old
worker shutdown releases its clients. Keep finite per-host idle cap and timeout.
Shutdown is bounded to5s; any abandoned memory tasks are not called durable.

### HTTP delivery

POST application/json only, request body <=1MiB; split batches on count, byte or
wait limits. Envelope: `{action_id, events:[...]}`. Each event contains event_id,
delivery_id,rule_id,topic,client_id,qos,retain,timestamp_ms,data. event_id identifies
the source event and can be shared across fanout. delivery_id identifies source +
rule + action, distinct across fanout and stable on retries. Receiver deduplication
uses delivery_id. Multiple actions execute independently, without distributed
transactions or cross-action completion ordering.

HTTP2xx succeeds. Transport/timeout/408/429/5xx retry finitely with bounded
exponential backoff and jitter; other statuses fail permanently. Timeout may mean
the receiver already applied work. Redirects disabled, TLS verification enabled,
no implicit proxy, no unbounded response body collection. Configuration and SQL
test never perform an implicit endpoint request. Configured target addresses are
trusted administrator choices and may be private/internal.

Status has mode="best_effort",running,queued_events,queued_bytes,received,matched,
dropped,errors,actions. ActionStatus has id,queued,delivered,failed,retried,dropped,
last_error. Event/copy counts are not HTTP request counts. Error details are only
sanitized status/class; no payload history or arbitrary persistent dead letters.

## CLI, authenticated management and React

Rules are enabled by default for normal serving, using rules.json in the working
directory. --rules-config PATH overrides this location, must be nonempty/nonduplicate,
and is incompatible with export-state. Export-state never loads or creates rules
configuration. Missing file creates empty config; invalid existing file fails startup.
The resolved default/custom file identity is included in static sensitive aliases.
Admin opt-in/token/loopback/CSP remain unchanged. Rule configuration writes explicitly
extend Phase4f read-only scope; session controls remain read-only.

| Endpoint | Behavior |
|---|---|
| GET /api/v1/rules/config |Redacted PublicConfig|
| PUT /api/v1/rules/config |Atomic full-config replacement|
| GET /api/v1/rules/status |Real runtime counters/running state|
| POST /api/v1/rules/test |`{sql,event}` -> `{matched,output}`, no outbound side effects|

All routes authenticate before reading body, reject query parameters, require
application/json for PUT/POST and limit body64KiB. Errors are redacted structured
JSON:400 validation,409 busy,503 storage/stopped/disabled,413 body,415 media type.
Disabled runtime returns rules_disabled. Overview adds capabilities.rules=true only
when enabled; existing mutations=false continues to mean no session mutations.

React uses existing Tailwind/shadcn controls: separate Rules/Connectors/Actions tabs,
CRUD/enable/reference validation, explicit save, password headers, SQL sample test
and actual status polling. Token/secrets/sample data stay in page memory. Poll5s
when visible, abort on logout/route change, no overlapping writes. Show crash-loss,
overflow and replacement cancellation; stopped state disables all pending/new writes
and SQL tests but leaves config inspectable. Save uncertainty says unconfirmed,
not unchanged. No fake data or unsupported connector functionality.

## Ownership and acceptance

Isolated worktrees: core owns mqtt-rules and narrow transport hook; outbound owns
mqtt-webhook; frontend owns dashboard/web; quality owns rules_webhook_blackbox and
CI. Coordinator owns bin/rmqtt, workspace lockfile, docs and unified integration.
lib.rs remains module declarations/documentation/re-exports only.

Require SQL whitelist/NULL/type/precision/complexity regressions, real HTTP retry/
batch/capacity/secret/persistence/generation tests, actual dual-version QoS0/1/2
MQTT→SQL→HTTP while ordinary delivery remains correct, preview no side effects,
auth/config restart, mobile/desktop browser CRUD/status and embedded release tests.
Run fmt, strict all-target Clippy, workspace tests, frontend tests/build and relevant
existing MQTT/TLS/WS/mTLS acceptance. Retain first failures and fixture corrections.
Report measured functionality only; no durable-action or TPS/p99 claim.

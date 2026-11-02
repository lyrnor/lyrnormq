> Request scheduling and concurrency superseded by [Phase5d](phase5d-outbound-concurrency.md).

**Durability/lifecycle superseded by [Phase5f](phase5f-durable-outbound-queue.md).**

# Phase5c — Unified outbounds and extensible execution (frozen 2026-10-30)

Supersedes Phase5a connector/action model; Phase5b SQLite container and MQTT durable
schema stay unchanged. Only Webhook is implemented; no Kafka/PostgreSQL/Redis
feature or plugin claim. User manages Rules and Outbounds, never a connector plus
an action for the same destination. Runtime configuration stays pure SQLite.

## Public configuration/API (frozen JSON shape)

GET/PUT /api/v1/rules/config uses:

```json
{
  "schema_version": 2,
  "outbounds": [{
    "id": "alarm", "name": "Alarm service", "enabled": true,
    "type": "webhook",
    "config": {"url": "https://example.com/events", "headers": {}},
    "delivery": {
      "batch_size": 1, "batch_wait_ms": 0, "queue_capacity": 256,
      "timeout_ms": 3000, "max_attempts": 3, "retry_base_ms": 200
    }
  }],
  "rules": [{
    "id": "hot", "name": "High temperature", "enabled": true,
    "sql": "SELECT * FROM \"devices/#\"", "outbound_ids": ["alarm"]
  }]
}
```

No connectors/actions/connector_id/action_ids in new config, rule types or status.
GET /api/v1/rules/status keeps existing totals but replaces actions with outbounds,
whose items have id/type/queued/delivered/failed/retried/dropped/last_error.
POST /api/v1/rules/test unchanged. Existing authentication, 64KiB request/config,
redaction, reference validation, enabled controls and running=false write/test
prohibition stay. schema_version must equal2. Unknown type (including planned
Kafka/PostgreSQL/Redis) and unknown fields are rejected before persistence/swap.
Header placeholders preserved only for same outbound ID AND same type.
Max64 outbounds,128 rules,512 unique rule bindings; existing field/SQL/policy limits
remain. Deletion of referenced outbound requires editing rule references first.
Webhook body now {outbound_id, events:[...existing source/delivery/rule metadata...]};
delivery_id uses source+rule+outbound, stable across retries/distinct fanout.

## Rust/module boundary (backend owner)

- mqtt-rules owns SQL/events/Rule; rename Rule.action_ids to outbound_ids.
- Add mqtt-egress: configuration model, SQLite persistence, SQL dispatcher,
  snapshots/generations, bounded scheduling, backpressure, batching/retry pacing,
  per-outbound counters and lifecycle. Broker depends on mqtt-egress Runtime.
- Add mqtt-egress-core: transport-independent sink trait, batch bounds and typed
  retryable/permanent delivery result. Sink is async object-safe with Send futures.
  Generic worker calls the sink; it must not import reqwest or interpret HTTP
  statuses, build HTTP envelopes/headers, or retain endpoint-specific client code.
- mqtt-webhook becomes only the Webhook adapter: typed WebhookConfig, validation,
  header redaction/restore, HTTP client lifecycle, envelope encoding, TLS/redirect
  policy and HTTP-specific success/retry classification. Depends on egress-core,
  never mqtt-egress/runtime or mqtt-transport/mqtt-storage/mqtt-rules.
- Central type factory validates explicit supported type and constructs an erased
  sink. New native adapter adds a typed configuration variant/factory/UI form;
  scheduler and SQL evaluator do not require edits. Unknown types not arbitrary JSON
  accepted silently. It is fine to deserialize raw wire config at a strict boundary
  then convert to typed variants; runtime domain must use typed configuration.
- Shared delivery policy describes capacities/time budgets/retry ceiling. Adapter
  capabilities constrain batching sizes/bytes, declare fixed/record-separator
  framing overhead and classify errors. Shared timeout is enforced by the generic
  worker, including for non-HTTP sinks. Webhook's batch
  body max1MiB includes all envelope overhead, preserving current boundary tests.
  Tests with fake non-HTTP sinks prove retry/permanent handling and capability
  bounds without standing up Kafka/PostgreSQL/Redis or providing dummy support.

Keep SQLite max64KiB, process ownership, transaction-then-publish, cancellation-safe
mutation/shutdown, generation connection-pool release, metadata redaction, bounded
memory16MiB, client-hashed SQL shard order and nonblocking MQTT admission. Outbound
queues remain best-effort, no durable message delivery enhancement.

## Existing SQLite configuration (not JSON-file compatibility)

At startup, a strictly recognized old SQLite blob {connectors,actions,rules} is
converted once inside the existing ConfigStore transaction: each old action becomes
one outbound with the action ID/name/enabled/policy and referenced connector config;
rules' action_ids become outbound_ids. Preserve credentials and logical IDs; unused
connector records have no standalone counterpart. Validate old refs and the new
full config/SQL before committing. Existing format2 stays unchanged. Invalid or
unknown versions/shapes are rejected without overwrite. No JSON file IO/import.
Old config is never accepted via new PUT API. Failed migration fails startup;
SQLite process crash exposes old or new complete configuration, never partial.

## UI/acceptance

Two tabs: 规则 / 出站. Create outbound: choose available type (currently Webhook),
configure destination/headers and delivery in one editor. List type, destination,
status and operations; rules directly select outbounds. Unsupported types cannot
be saved or falsely advertised as available. No internal implementation banners.
Use existing React/Tailwind/shadcn components; preserve secret/password inputs,
no localstorage, error/stale state, stopped protections, polling and mobile layout.

Test SQL->outbound->real HTTP for both MQTT versions/QoS0/1/2; retries/error classes,
fanout identity,1MiB envelope limits, bounded slow target, redaction, references,
unknown type/fields/format versions, enable/delete, pure SQLite restart/SIGKILL,
old-SQLite transaction migration and no JSON-file backend. Run full Fmt/Clippy/tests,
frontend tests/build and real release blackbox; inspect desktop/mobile browser CRUD.

### Dashboard identity and controls clarification (2026-10-30)

New rule/outbound drafts generate bounded cryptographic internal IDs once; users
enter names rather than IDs. Preserve identities across edits, validation/save
retries and refresh. Configuration API/schema and existing SQLite identity remain
unchanged; API IDs and outbound_ids are still explicit for programmatic callers.
UI lists, bindings, execution status, delete confirmation and accessible action
labels display names rather than internal keys; duplicate destination names show
type/address context when selecting. MQTT Client IDs/Subscription IDs remain
meaningful protocol fields. Use shared Radix Switch/Checkbox/Tabs/Label and shared
Textarea/Field/Alert primitives; retain the tested Drawer/native-dialog top layer
and strict CSP, with no new dependency or inline-style permission.

### Webhook basic/advanced settings (2026-11-02)

Outbound basic authoring exposes only name, Webhook address and authentication/
headers. The existing Radix Collapsible component defaults closed and contains
enabled/type plus batch size/wait, timeout, maximum attempts and retry interval.
Rule editor enable controls are unchanged. Outbound list rows no longer repeat
tuning-policy summaries.

Queue capacity is not editable anywhere in this form: new outbounds use the
existing system default256; editing preserves the existing bounded capacity.
Configuration API/schema, SQLite data and backend admission limits are unchanged;
no adaptive-capacity behavior is implied. Collapsing never resets draft policy.
Closed panels unmount controls to avoid hidden invalid/required browser focus
errors; validation still checks the full policy and reopens invalid advanced
settings before any request. Existing non-default policies, headers, enabled flags
and IDs survive basic-only edits. No new dependency or CSP relaxation.

### Single-message authoring defaults (2026-11-02)

New Dashboard-created Webhook outbounds default to batch_size1 and batch_wait_ms0.
Users needing batching opt in via the collapsed advanced settings. Existing saved
policies, including100/50, remain unchanged on editing or basic-only saves. Other
limits/defaults and POST events-array encoding are unchanged; one message produces
one event per request under the new defaults. No SQLite rewrite or API migration.

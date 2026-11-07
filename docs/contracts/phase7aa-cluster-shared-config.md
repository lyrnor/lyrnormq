# Phase7aa — cluster-shared rules and access configuration

Rules/outbound and HTTP access-control configuration are one cluster document
each. Standalone brokers keep local SQLite as the source of truth. Session
replicas, egress durable queues, Controller Raft WAL, and bbolt checkpoints
outside these documents stay unchanged.

## Source of truth

With `[cluster]`, Controller Raft stores:

- `Metadata.rules_config`: `{ generation, document }`
- `Metadata.access_config`: `{ generation, document }`

`document` is the validated JSON text, at most 64 KiB, including secrets needed
to rebuild webhook/HTTP/Kafka clients. Dashboard GET still returns the existing
redacted public shape. This is an explicit exception to the Phase7a rule that
ordinary client credentials must not enter metadata: these are administrator
configuration documents, not MQTT CONNECT material.

Standalone mode continues to use `data_dir/rules.sqlite` and
`data_dir/access.sqlite` only.

## Write and apply

Dashboard PUT runs only on broker or `controller,broker` processes. The local
runtime restores `[redacted]` secrets, validates, and probes egress bindings for
rules. It then submits `Command::PutClusterConfig` with CAS
`expected_generation` (zero when absent). Controllers commit through the existing
mutate path. After commit, every broker applies the document through the metadata
watch. Apply is idempotent on generation: equal or older generations are ignored.
Apply does not treat local SQLite as authority.

PUT returns after Raft acceptance and the submitting broker's applied barrier.
It does not wait for every live broker. Status, SQL/test probes, and cache clear
remain process-local.

## Migration

Empty cluster documents do not auto-merge divergent per-node SQLite files. The
operator publishes once through Dashboard (or an explicit import later). Until
generation ≥ 1 exists, a broker may keep serving its local file contents. After
the first committed document, local SQLite is no longer authoritative in cluster
mode.

Older Controllers that cannot decode `PutClusterConfig` must be upgraded before
any broker writes shared configuration. Brokers that fail to apply a committed
document fail closed for that subsystem and must not silently fall back to a
stale local file as a competing source of truth.

# Embedded cluster: initial gate

Build with `cargo build --release --locked`. Building the peer RPC adapter also
requires `protoc` on PATH. This gate is for development acceptance, not a complete
production MQTT cluster. See [contract](../contracts/phase7a-cluster.md).

Each process reads one TOML file. The complete field list and the matching
`RMQTT_*` environment variables are in `rmqtt.toml.example` at the repository
root. Three, five, or seven machines share the member list and differ in `node.id`, `node.role`, and
`node.data_dir`. Omitting `[cluster]` starts a standalone broker. With
`[cluster]`, `node.role` lists `controller`, `broker`, or `controller,broker`.
A controller process uses member `id`. A broker process uses `100 + id`. One
process listing both roles starts them together.

```sh
./target/release/rmqtt --config /opt/rmqtt/rmqtt.toml
./target/release/rmqtt cluster-status --config /opt/rmqtt/rmqtt.toml
```

`--config` defaults to `rmqtt.toml` in the current directory. Environment
variables override the same field. `RMQTT_CLUSTER_MEMBERS` replaces the whole
member list and uses comma-separated `id:host` pairs. There is no setting flag for
listen addresses, limits, TLS, Dashboard, or rules paths.

Member `id` is the stable identity and does not change. `host` is the current
address. Controller peers listen on `host:controller_port`. Broker peers listen
on `host:broker_port`. The smallest id creates the Raft group only when it is
not already initialized. The lease is 3000 ms. Every member is both a
controller and a memory replica. The member count must be 3, 5, or 7, the odd
sizes etcd documents up to its practical maximum of seven voting members. Data for the two roles is
`data_dir/controller` and `data_dir/broker`. Rules, access control, and the
outbound queue are `data_dir/rules.sqlite`, `data_dir/access.sqlite`, and
`data_dir/egress`. Standalone sessions are `data_dir/sessions.sqlite`. Cluster
mode does not keep a session SQLite mirror.

Create a private peer token file containing 32–256 visible ASCII characters.
All members share this secret; use a separate secret from the Dashboard token.
Start every member before testing readiness. Use clean sessions without
Wills; retained publications and shared subscriptions are refused in this gate.
Existing Dashboard Bearer HTTP publishing routes to remote subscribers as well.

A public MQTT address requires `[mqtt.tls]`. Loopback and RFC1918 addresses may
stay plaintext. Cluster member hosts must be loopback or private addresses.

Public peer endpoints require HTTPS and mutual TLS. Loopback and RFC1918
private addresses may use plaintext HTTP. When TLS is used, add tls with
cert_file, key_file, ca_file and domain_name, and use numeric HTTPS endpoint
addresses with a certificate matching domain_name. Controller membership is
fixed at three nodes.
Do not share node IDs or data directories. Nodes holding the peer credentials are
trusted cluster members; this is not per-node permission isolation.

Reproduce the independent real-process checks with:

```sh
RMQTT_BIN="$PWD/target/debug/rmqtt" python3 tests/cluster_blackbox.py -v
```

The harness allocates its own temporary ports and files and does not use an
existing MQTT service. Process crashes exercise
journal recovery, not physical power-loss certification. Durable forwarding receipts,
session relocation, replicated retained/Will/shared state and performance acceptance
remain later gates.

Current fixed implementation limits (acceptance bounds, not capacity measurements):

| Resource | Limit |
| --- | --- |
| Metadata nodes / active watch streams per Controller | 1024 / 128 |
| Clients / encoded metadata | 100,000 / 8 MiB |
| One metadata command / RPC envelope | 512 KiB / 64 MiB |
| Forwarded payload | 1 MiB |
| Concurrent forwarding / payload budget | 32 / 16 MiB |
| Volatile duplicate receipts | 8192 |

The byte limit can be reached before the count limit. Whole metadata serialization,
route rebuilding and per-watch delta calculation need profiling and optimization
before making a large-scale performance promise.

## Durable forwarding and session failover

The Phase7b mode uses three fixed Broker data replicas, separate from the three
Controllers. Add this identical block to **each Broker** configuration (not the
Controller configurations); their existing node IDs, listen and advertise must
match the group:

```json
"durable": {
  "replicas": [
    {"id": 101, "address": "http://127.0.0.1:7101"},
    {"id": 102, "address": "http://127.0.0.1:7102"},
    {"id": 103, "address": "http://127.0.0.1:7103"}
  ]
}
```

Initialize a separate cluster ID and fresh, separate Controller/Broker directories
for this mode. The first Broker registration freezes the cluster's mode and full
replica group. Existing legacy Controller data stays legacy; in-place migration is
not implemented. Preserve old data. Do not delete a journal or change the replica
list to bypass this protection. A different group or mixed legacy registration is
rejected. Public addresses still require mutual TLS. RFC1918 private addresses
may stay on plaintext HTTP.

Current [Phase7f](../contracts/phase7f-memory-replicas.md) uses **typed RAM** for
sessions, outbox, receipts and repair staging. `durable.replicas` is the historical
configuration name for replication, not a disk guarantee. Controller Raft remains
SQLite. Rules/access configuration databases and generic outbound append logs are
separate and unchanged. Replicated mode does not keep a session SQLite mirror.

Start all three processes before admitting traffic. ACKs require two live replicas;
subscriber receipt is a separate condition. Surviving-majority failover restores
sessions and packet identifiers. Every data-process restart creates a new identity,
even with the same directory: it must complete explicit [repair](replica-repair.md)
before voting. Whole-group RAM loss cannot be recovered from Controller metadata.
Do not erase old directories or controller anchors to pretend to recover data.

New data directories contain only data.lock and memory.identity. Existing
`data.sqlite` is refused untouched; automatic import is not implemented. Fresh
clusters require fresh directories and a distinct cluster ID. Mixed old/new rolling
upgrades are unsupported and capability checks fail closed.

Bounds: 100,000 records, 256 MiB accounted canonical bytes plus keys, 8 MiB per
record, 2 MiB opaque snapshot, 256 pending intents per source and 8192 recipient
source-stream watermarks. These are admission bounds, not a physical RSS cap;
typed objects, cached canonical bytes and containers add memory overhead. Repair
staging has an independent finite 768 MiB budget. Full capacity rejects admission.

Clean Start/expiry creates a new logical session generation; queued work cannot
leak into a recreated session. Retained, Will and shared subscriptions remain
unsupported in cluster mode. Automatic incremental repair, membership changes,
tombstone cleanup, whole-group restart recovery and RocksDB remain future work.

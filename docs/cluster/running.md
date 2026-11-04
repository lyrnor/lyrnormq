# Embedded cluster: initial gate

Build with `cargo build --release --locked`. Building the peer RPC adapter also
requires `protoc` on PATH. This gate is for development acceptance, not a complete
production MQTT cluster. See [contract](../contracts/phase7a-cluster.md).

Run three Controllers with separate configuration files and data directories.
Example Controller 1 configuration (change node_id, listen and data_dir for 2/3;
only the first Controller sets bootstrap true):

```json
{
  "role": "controller",
  "cluster_id": "rmqtt-development",
  "node_id": 1,
  "listen": "127.0.0.1:7001",
  "data_dir": "/tmp/rmqtt-cluster/controller-1",
  "token_file": "/tmp/rmqtt-cluster/peer.token",
  "controllers": [
    {"id": 1, "address": "http://127.0.0.1:7001"},
    {"id": 2, "address": "http://127.0.0.1:7002"},
    {"id": 3, "address": "http://127.0.0.1:7003"}
  ],
  "bootstrap": true,
  "lease_ms": 3000
}
```

Create a private peer token file containing 32–256 visible ASCII characters. All
members share this secret; use a separate secret from the Dashboard token. Start
each Controller in its own terminal:

```sh
./target/release/rmqtt --cluster-config /path/to/controller-1.json
```

For a Broker, use the same cluster_id, token_file and controllers; change role to
`broker`, node_id to 101 (102 for another Broker), listen to `127.0.0.1:7101`,
data_dir to its own directory, and set bootstrap false. Set advertise to
`http://127.0.0.1:7101`. This listen address is the internal peer RPC endpoint;
the existing command line `--listen` is the MQTT listener:

```sh
./target/release/rmqtt --cluster-config /path/to/broker-101.json --listen 127.0.0.1:1884
./target/release/rmqtt cluster-status --cluster-config /path/to/controller-1.json
```

The second Broker needs separate peer/MQTT ports and state/configuration files.
Use clean sessions without Wills; retained publications and shared subscriptions
are refused in this gate. Existing Dashboard Bearer HTTP publishing routes to
remote subscribers as well. Standalone startup is unchanged.

To host a Controller and Broker in **one process**, supply both existing role
configurations. Each role keeps its own node ID, peer endpoint and data directory;
the process shares one Tokio runtime and exits if either service fails:

```sh
./target/release/rmqtt --cluster-config /path/to/broker-101.json \
  --cluster-controller-config /path/to/controller-1.json --listen 127.0.0.1:1884
```

Repeat for Broker102/Controller2 and Broker103/Controller3, starting all three
processes before testing readiness. The two configurations must have identical
cluster/membership, token path, TLS and lease settings, distinct IDs/listeners, and
non-overlapping canonical data directories. The additional option is only for
normal Broker startup, not status/export/repair commands. Existing standalone and
single-role commands remain valid. This is role cohosting, not additional physical
failure domains or a new Raft/data storage schema. See the
[comparison contract](../contracts/phase7d-cluster-benchmark.md).

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

The harness allocates its own temporary ports/files and starts three Controllers
and two Brokers; it does not use an existing MQTT service. Process crashes exercise
journal recovery, not physical power-loss certification. Durable forwarding receipts,
session relocation, replicated retained/Will/shared state and performance acceptance
remain later gates.

Current fixed implementation limits (acceptance bounds, not capacity measurements):

| Resource | Limit |
| --- | --- |
| Metadata nodes / active watch streams per Controller | 1024 / 128 |
| Clients / encoded metadata | 100,000 / 8 MiB |
| One metadata command / RPC envelope | 512 KiB / 16 MiB |
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
separate and unchanged. `--storage-path` is refused in this replicated mode.

Start all six processes before admitting traffic. ACKs require two live replicas;
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

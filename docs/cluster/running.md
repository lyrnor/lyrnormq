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

Non-loopback peer endpoints require HTTPS and mutual TLS: add tls with cert_file,
key_file, ca_file and domain_name; use numeric HTTPS endpoint addresses with a
certificate matching domain_name. Controller membership is fixed at three nodes.
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
rejected. Non-loopback addresses still require mutual TLS.

Each data journal also has an automatically persisted disk identity anchored by
the Controllers. Restarting with the original disk is supported. A new empty disk
under an old node ID is refused, including during the RPC startup window; it must
not count as an empty quorum vote. Phase7c provides explicit maintenance-window
[repair/replacement](replica-repair.md) using two original donors and an excluded
learner. Losing one node leaves the other two able to continue; do not wipe its
directory as a recovery procedure.

Start all six processes before admitting traffic. Durable operations require two
data replicas: a successful ACK means replicated session/forwarding responsibility,
not that a subscriber has already received the message. One failed Broker can be
recovered on another disk; with fewer than two replicas, new durable operations
fail. Startup/listener availability alone is not a data-quorum health guarantee.

The authoritative Broker journal is data.sqlite under config.data_dir. Optional
--storage-path is a local compatibility mirror, not a failover backup. Controllers
hold ownership epochs/filter metadata only. Cross-node recovery restores existing
MQTT snapshots and packet identifiers; acknowledged forwarding intents survive
source client disconnect and do not require that client to reconnect for draining.
QoS0 subscriber delivery remains best effort. Retained/Will/shared cluster features
remain refused. See [Phase7b contract](../contracts/phase7b-durable-cluster.md) and
the corresponding verification report for actual accepted gates and limits.

Durable-mode bounds currently include 100,000 journal records, 256 MiB aggregate
encoded record data (SQLite pages/WAL add overhead), 8 MiB per encoded record,
2 MiB per opaque snapshot, 256 pending intents per source record and 8192 distinct
source-stream watermarks per recipient. Earlier byte/SessionEngine limits can
bind first. Full capacity fails admission; it never falls back to volatile success.
The source watermark does not grow once per message for a fixed producer.

Clean Start or expiry/recreation gets a new logical session generation. Pending
deliveries freeze that generation, so reusing a Client ID and topic cannot admit
old queued work into the new session. Ordinary persistent owner failover preserves
the generation and can resume its pending deliveries.

This is a correctness gate, not large-cluster performance acceptance: directory
scans, journal quota scans and whole-record serialization still need profiling.
General tombstone/abandoned-host cleanup, membership reconfiguration and rolling-version
migration remain unsupported. Controlled disk repair follows the Phase7c guide.

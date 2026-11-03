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

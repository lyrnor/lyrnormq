# rmqtt

Rust 2024 / Tokio TCP MQTT3.1.1 broker with an initial MQTT5.0 implementation, supporting QoS0/1/2,
Retained Messages, Last Will and local authentication/ACL, with optional SQLite-backed persistent sessions.
**Full MQTT conformance and production-readiness are not yet claimed.**

```sh
cargo build --workspace
cargo run -p rmqtt -- --listen 127.0.0.1:1883
# Enable durable sessions, retained records and active/pending Wills:
cargo run -p rmqtt -- --storage-path ./sessions.sqlite --max-retained 10000
```

Both modes use the existing bounded SessionEngine/outbox transaction pipeline.
Without SQLite, clean sessions and global retained/Will state are volatile across
process death. With SQLite, Clean Session=false retains subscriptions, offline
QoS1/2 and inflight stages/IDs. Clean Session=true discards the prior session;
retained messages remain independent of that reset. MQTT5 Clean Start and Session
Expiry are independent, as described below. TLS and WebSocket remain unsupported.
Default bind is loopback.

## MQTT5 foundation and version interoperability

The same TCP listener negotiates protocol level4 or5. The normalized Packet/QoS
state machines and transactional outboxes are shared; a versioned Frame adds
properties, reasons and subscription wire options. The codec recognizes all standard
property identifiers/types, rejects wrong contexts/duplicates/illegal values and
preserves repeated UserProperty order. This is an explicit subset of MQTT5 semantics.

| Implemented semantic | Behavior |
| --- | --- |
| Clean Start | Resets old session state independently of the requested expiry |
| Session Expiry | Default0, finite seconds or0xFFFFFFFF infinite; DISCONNECT may change it except0→positive |
| Receive Maximum | Per connection; QoS1/2 quota is held through PUBACK/PUBCOMP or negative PUBREC |
| Reconnect flow | New quota applies to old replay; PUBREL/control progress bypasses blocked PUBLISH |
| Application properties | UserProperty, ContentType, PayloadFormatIndicator, ResponseTopic and CorrelationData survive retained/Will/QoS/outbox recovery |
| Message Expiry | Finite TTL for offline/retained/QoS2 data; expired unstarted writes are suppressed |
| Will Delay | Durable delay capped by session end; surviving-session reconnect cancels before due |
| Reasons | Context-validated ACK/SUBACK/UNSUBACK/CONNACK/DISCONNECT reasons, negative ACK cleanup |
| Mixed versions | Publish/subscribe QoS0/1/2 routes across versions; properties are stripped only from MQTT311 wire output |

MQTT5 CleanStart=false may resume an existing session and then choose expiry0,
which removes it at that connection's end. CleanStart=true with positive expiry
creates a fresh persistent session. MQTT311 wrappers preserve the old CleanSession
mapping (true→0,false→infinite). MQTT5 positive expiry can survive disconnects in
volatile mode while the process lives; process durability requires SQLite. MQTT311
non-clean connections still require SQLite under the existing broker policy.

Offline finite deadlines are persisted in Unix milliseconds and include downtime.
Online sessions do not expire. If an active finite session is loaded after a crash,
the exact failure instant is unknown: its deadline starts conservatively at restart
plus the interval. A known offline deadline is never extended by reboot. An expiry
timer removes offline subscription/queued/inflight state and frees IDs independently
of traffic. Retained globals and pending Will intent remain separate. Schema3 owners
are preserved in schema5; schema1/2 still migrate to anonymous ownership.

The server advertises bounded ReceiveMaximum/MaximumPacketSize, TopicAliasMaximum0,
SharedSubscriptionAvailable0 and SubscriptionIdentifierAvailable0. Unsupported
semantics are refused explicitly rather than accepted and discarded:

- Client MaximumPacketSize negotiation: CONNECT refusal0x83; if even the five-byte
  refusal cannot fit the supplied limit, the connection closes without an oversized reply.
- Enhanced AUTH/methods: CONNECT0x8C; unsolicited AUTH after basic auth is a protocol error.
- Topic Alias:0x94, including zero; Subscription Identifier: DISCONNECT0xA1 (client PUBLISH0x82).
- Shared filters: DISCONNECT0x9E; nondefault NoLocal/RAP/RetainHandling: SUBACK0x83.
- Server-assigned Client IDs: not implemented; empty MQTT5 Client ID gets CONNACK0x85.

Complete output frames obey the packet cap too: legacy311 messages can grow when
bridged to5. An unclean5 resume whose pending/queued output would exceed that cap
is refused0x83 without changing old state; with SQLite, the owner can reconnect311
to drain it. Known offline5 recipients are preflighted too, preventing new undrainable
volatile queues. Routing/retained replay preflights the recipient wire version and atomically rejects
a publication/replay that cannot fit, rather than poisoning a durable outbox.

These exclusions, full direction/conformance matrices and operational hardening
remain work. Full MQTT5 compatibility and production-readiness are not claimed.

## Authentication and ACL

No policy file keeps open anonymous access for compatibility. With `--auth-policy
policy.json`, unlisted users and actions are denied. CONNECT supports UTF8 usernames
and binary passwords; the local provider verifies Argon2id version19 PHC hashes.
Passwords are redacted in Debug and are never stored in broker snapshots.

Generate a fresh PHC by piping exact password bytes to the owned helper (no newline
trimming). For example, in a shell supporting silent `read`:

```sh
read -r -s MQTT_PASSWORD
printf '%s' "$MQTT_PASSWORD" | cargo run -q -p mqtt-auth --example hash_password
unset MQTT_PASSWORD
```

Paste its output into `password_hash` below; do not store plaintext in the policy:

```json
{
  "allow_anonymous": false,
  "users": [{
    "username": "alice",
    "password_hash": "<generated Argon2id PHC>",
    "client_id_prefix": "alice-",
    "publish": ["tenant/alice/#"],
    "subscribe": ["tenant/alice/#"]
  }]
}
```

```sh
cargo run -p rmqtt -- --auth-policy policy.json --storage-path sessions.sqlite
```

`allow_anonymous:true` additionally needs an `anonymous` object with the same
`client_id_prefix`, `publish` and `subscribe` fields to grant actions; omission
means empty deny lists. Each user must specify all five fields. Prefixes are literal.
Unknown fields, duplicate users, invalid filters and unsupported/unbounded hash
parameters fail startup. Policy file<=1MiB, users<=1024, username<=256 bytes, each
publish/subscribe list<=128. Hash work is bounded to m<=65536KiB,t<=10,p<=4;
the helper generates standard library defaults (m19456,t2,p1).

Publishing uses exact-topic permission; a subscription filter must fit within one
allowed filter. This conservative containment can reject some equivalent or union
coverage. Root wildcards do not grant `$SYS` access; add explicit system filters.
MQTT311 mixed SUBSCRIBE returns0x80 for denied filters; MQTT5 uses specific reasons
such as0x87. Denied PUBLISH closes without success acknowledgement; authentication
refusals use CONNACK4/5 in MQTT311 or0x86/0x87 in MQTT5 before close.
A forbidden Will rejects CONNECT before takeover or registration.

Session and Will records bind a stable identity. Another user cannot take over or
clear that Client ID, including with Clean Session=true. Anonymous clients share
one identity; disable them when distinct ownership is required. Existing schema1/2
records migrate to anonymous, never to the first authenticated claimant. Enabling
an authenticated-only policy may therefore make old anonymous sessions inaccessible;
explicit offline identity conversion/management remains future work. Owner labels
include the username. Migration metadata counts toward --max-state-bytes; a legacy
snapshot near that limit may need additional configured headroom.

Policy is loaded once before listening. After a restart with reduced rights,
unauthorized subscriptions, queued messages and payload-bearing retransmissions are
removed transactionally; Packet IDs are released. Pending inbound QoS2 checks its
original topic again on PUBREL. Already accepted outgoing AwaitComp may finish its
payload-free handshake. Retained reads and Will publication are reauthorized;
revoked Wills are suppressed atomically. `BrokerConfig.access_control` and
`mqtt_auth::AccessControl` support embedded custom bounded policies.

TCP is currently unencrypted: authentication does not provide wire confidentiality.
TLS, remote providers, reload and authentication rate limiting remain unimplemented.
Argon2 runs on the bounded blocking worker; authentication bursts and global ACL
reconciliation can delay other traffic. No security/performance certification is claimed.

## Retained Messages

A retained publication replaces the exact topic's stored payload and QoS. Empty
payload deletes that record while still forwarding the empty live message.
Non-retained publications never alter retained state. Existing subscribers receive
RETAIN=0; an accepted new or identical replacement subscription receives matching
retained records with RETAIN=1 and QoS=min(stored,granted). Wildcards and system-topic
isolation reuse the existing matcher. Multiple filters in one SUBSCRIBE are handled
sequentially and may each replay a copy; ordinary live routing remains deduplicated.
Incoming retained QoS2 changes state only on PUBREL, atomically with source completion
and recipient outboxes. Retained replay flags survive outgoing retries/restarts.

## Last Will

CONNECT accepts binary Will payloads at QoS0/1/2, with optional retain. A reason00
DISCONNECT commits cancellation before closing. Other valid nonzero reasons, EOF/RST,
protocol error and Keep Alive timeout schedule the Will through the same outbox path.
MQTT311 and delay0 Wills are immediate; MQTT5 positive delay waits until the earlier
of its deadline or Session Expiry. A pre-due CleanStart=false resume of a surviving
session cancels its delayed Will, including takeover; CleanStart=true or expiry0 ends
the old session and forces publication. Reason04 also respects the configured delay. Replacement registration/cancellation and old-Will effects share
a transaction; stale connection cleanup cannot fire the replacement's Will.
A retained Will updates/deletes retained state using the same empty-payload rule.

Capacity-blocked Wills remain bounded pending intents and retry after progress,
without partial delivery or silent loss. A same-ID CONNECT carrying a new Will is
rejected while the old one cannot drain; reconnecting with no new Will can resume
the session without overwriting the pending intent, including draining a full
self-subscription outbox. Graceful close of that new connection preserves the old
pending intent. SQLite stores accepted Wills even for clean-session origins, and
recovers them before serving clients; publication follows the durable due time. Without SQLite, process
failure loses volatile state. Server shutdown closes connections as unexpected
ends unless their DISCONNECT was already committed.

## Message Expiry and delayed Will recovery

MQTT5 MessageExpiryInterval is stored as one absolute deadline shared by all routed
copies. Zero suppresses delivery immediately; 0xFFFFFFFF is finite, while absence
means no expiry. Offline queues and retained records expire without MQTT traffic;
an expired retained replacement removes the previous value. Incoming QoS2 keeps
its handshake through PUBREL even when its payload expires. Mixed-version delivery
keeps internal deadlines and omits properties on MQTT311 wire output.

The writer reserves a delivery, then validates its generation/deadline and commits
start intent before socket output. A never-started expired QoS1/2 exchange retires
its ID and quota. Already-started QoS1/2 exchanges still finish/retransmit with their
original IDs (remaining TTL may be0); AwaitComp sends only PUBREL. Remaining TTL is
rounded up and refreshed after storage commit immediately before writing.

Will Message Expiry starts when publication succeeds, after any delay/capacity wait.
Known delayed-Will deadlines survive restart. For an active-at-crash connection the
failure instant is unknown: delay starts conservatively at recovery and is capped
by the recovered session deadline. Capacity-blocked intents remain bounded and
retry after progress without a busy timer.

Schema5 requires deadlines, started flags, Will delay/due fields and a persisted
logical clock floor. Clock rollback cannot extend deadlines beyond a committed
observation. A later socket-only TTL refresh is not itself another transaction:
a crash plus clock rollback may restore a TTL above that last advertised value,
bounded by committed time. Start intent and TCP output also have a conservative
may-have-sent gap; no atomic database/socket guarantee is claimed. Extra first-write
transactions and full snapshots have unmeasured throughput/latency costs.

## Persistence and limits

SQLite uses WAL/FULL and schema5 JSON whole-registry transactions. Valid schema1/2/3/4
snapshots are explicitly migrated, preserving IDs/order/subscriptions; the opaque
SQLite container remains version1. Unknown/corrupt schemas or incompatible lowered
quotas reject startup instead of dropping data. Atomic state/outbox commits precede
ACKs and first outgoing transmission. QoS2 AwaitComp reconnects retransmit PUBREL,
never PUBLISH; other unresolved publishes keep their IDs and set DUP.

| CLI flag | Default | Meaning |
| --- | --- | --- |
| `--auth-policy` | disabled | Bounded JSON authentication/ACL policy; invalid policy fails startup |
| `--listen` | `127.0.0.1:1883` | TCP bind address |
| `--storage-path` | disabled | SQLite path; parent must exist |
| `--max-packet-size` | 1048576 | Maximum complete packet bytes |
| `--max-connections` | 1024 | Accepted concurrent connections |
| `--max-sessions` | 1024 | Online/offline session count and bounded Will records |
| `--outbound-capacity` | 64 | Per-session pending outbox / per-connection write queue |
| `--max-subscriptions` | 256 | Distinct filters per session |
| `--max-inflight` | 32 | Each incoming QoS2 and outgoing QoS1/2 window |
| `--max-retained` | 10000 | Global retained topic count |
| `--max-state-bytes` | 67108864 | Serialized live registry and durable snapshot limit |
| `--handshake-timeout-ms` | 10000 | CONNECT deadline |
| `--write-timeout-ms` | 5000 | Per-frame write deadline |

All numeric limits are positive; packet size is at least2 and inflight<=65535.
`--help` and BrokerConfig expose the controls. Count and logical-byte limits do not
constitute an aggregate process-memory or disk budget. Full retained replay/outboxes
abort a proposal without a success ACK; pending Will publication waits for capacity.

A permanent canonical-path `.broker-lock` sidecar holds a lifetime OS lock; it is
not an existence lock and is not deleted. A second cooperating broker is rejected.
Existing symlinks share the canonical lock; dangling symlinks are rejected. Hard-link
aliases, externally replacing/renaming/modifying an active DB are unsupported.

Whole-registry cloning/JSON and per-proposal routing-index rebuilding have costs
that are not benchmarked. Worker transaction waits can delay controls; operational
fault/stress testing, disk quotas, rolling-version migrations beyond schema1/2/3/4->5, backup/admin and
observability remain work. SIGKILL and SQLITE_FULL tests do not establish physical
power-loss behavior or application-side exactly-once processing.

## Verification

```sh
python3 -m venv /tmp/rmqtt-client-venv
/tmp/rmqtt-client-venv/bin/pip install paho-mqtt==2.1.0
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
python3 tests/blackbox.py
python3 tests/qos_blackbox.py
python3 tests/auth_acl_blackbox.py
/tmp/rmqtt-client-venv/bin/python tests/auth_acl_paho.py
python3 tests/mqtt5_blackbox.py
/tmp/rmqtt-client-venv/bin/python tests/mqtt5_paho.py
python3 tests/expiry_will_blackbox.py
/tmp/rmqtt-client-venv/bin/python tests/expiry_will_paho.py
python3 tests/persistence_blackbox.py
python3 tests/retained_will_blackbox.py
/tmp/rmqtt-client-venv/bin/python tests/paho_interop.py
/tmp/rmqtt-client-venv/bin/python tests/persistence_paho.py
/tmp/rmqtt-client-venv/bin/python tests/retained_will_paho.py
```

Set `RMQTT_TEST_STORAGE_PATH` to a fresh temporary SQLite path to run the reused
basic/QoS/Retained/Will/Paho suites in SQLite mode. Tests needing process persistence
are explicitly skipped in volatile mode; CI runs both. Each lib.rs contains only
documentation, module declarations and re-exports.

See [architecture](docs/architecture.md), [current contracts](docs/contracts/phase2f.md),
[actual results](docs/progress.md) and [remaining dependencies](docs/tasks.md).

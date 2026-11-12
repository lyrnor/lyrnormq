# Redis authentication and authorization

The Dashboard's **认证 / 授权 → 后端类型 → Redis** selects each backend
independently. HTTP, PostgreSQL and Redis may be mixed. The broker reads fixed
HASH fields and does not create or write business keys. SQLite still stores
private access configuration. Existing HTTP and PostgreSQL configurations remain
valid.

## Keys and fields

The key prefix defaults to `lyrnormq:` and must be 1..64 bytes, end with `:`,
and contain only ASCII letters, digits, `:`, `_` and `-`. Logical database 0
is always used.

Authentication:

```text
HSET lyrnormq:auth:device-001 password_hash '$argon2id$v=19$m=19456,t=2,p=1$...'
```

The only result field is `password_hash`, the same alias PostgreSQL queries
return. A missing key or field denies authentication. An empty, oversized or
malformed PHC makes the attempt unavailable. Only Argon2id v19 PHC is
supported. Limits match local authentication: m<=65536 KiB, 1<=t<=10, 1<=p<=4
and m>=8*p. Hash length<=512 bytes. MQTT passwords are exact bytes and are
verified in the broker; they are never written to Redis. At most four blocking
hash tasks run across PostgreSQL and Redis providers. Authentication results
are not cached. Accounts that should not authenticate are represented by not
writing the key.

Authorization:

```text
HSET lyrnormq:acl:device-001 \
  publish '[{"effect":"allow","topic_filter":"devices/device-001/#","priority":10}]' \
  subscribe '[{"effect":"allow","topic_filter":"devices/device-001/#","priority":10},{"effect":"deny","topic_filter":"devices/device-001/private/#","priority":10}]'
```

Each JSON object uses the PostgreSQL result columns: non-null `effect`
(`allow`/`deny`), non-null `topic_filter`, and integer `priority`. Extra JSON
members are ignored. Higher priority wins. Equal priority deny wins regardless
of array order. No match denies. Publish matches the exact topic. Subscribe
requires one allow filter to cover the requested filter. Deny filters match any
intersection. The broker loads at most 256 rules after merging the general
field and the Client ID field; one more rule makes the operation unavailable.
Each filter<=65535 bytes and aggregate filter bytes<=1MiB.

Rules for every Client ID live in `publish` or `subscribe`. Rules for one
Client ID live in a field whose name is that action, one ASCII unit separator
(`0x1F`), then the Client ID. Both fields are read and merged before the
priority decision. This is the Redis form of
`device_id IS NULL OR device_id = $2` together with `operation = $3`.

## Connections, TLS and limits

Configure host, port, optional username, Redis password, TLS mode, optional CA
PEM, key prefix and maximum connections per stage (1..32, default 8). TLS
defaults to verify-full with the bundled web trust anchors; a private CA PEM
replaces those anchors. Disable TLS only through the explicit plaintext
selection on a trusted network. API reads show `[redacted]`; sending that
placeholder back preserves the previous password for the same backend. An empty
password connects without AUTH.

Pools are lazy. Saving configuration does not prove connectivity. **测试 Redis
连接** sends `PING`. Authentication and authorization test drawers read the
configured HASH fields without creating sessions or changing live counters.
Status shows pool total and idle connections.

The existing timeout 100..5000 ms includes connection acquisition and the Redis
commands. Hash work is bounded separately and cannot be forcibly stopped.
Requests also retain existing per-stage and global concurrency caps. Use a
Redis ACL that grants only `PING`, `HGET`, `HSTRLEN` and `HEXISTS` on the
configured prefix. Connection failures, timeouts, `WRONGTYPE` and invalid
values are unavailable, distinct from a missing record. No fail-open or
secondary-backend retry. Authorization allow/deny caching reuses TTL 0..300 s.

New CONNECT (including Will topic), PUBLISH and each SUBSCRIBE filter check
admission before effects. Existing accepted QoS handshakes, queued deliveries,
retained replay and delayed Will do not query Redis again.

## Build and verify

Normal builds include `redis-auth`. To omit Redis from the broker binary, use
`cargo build -p lyrnormq --release --no-default-features`. That build does not
accept Redis configuration and the Dashboard hides the backend option.

```sh
docker run -d --name lyrnormq-redis-access-test -p 127.0.0.1:16379:6379 redis:7
cargo test -p mqtt-auth-redis -- --include-ignored
```

The disposable Redis instance is for isolated local tests only. Production
deployments must use a restricted ACL user and actual credentials.

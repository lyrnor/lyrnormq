# PostgreSQL authentication and authorization

The Dashboard's **认证 / 授权 → 后端类型 → PostgreSQL** selects each backend
independently. HTTP and PostgreSQL may be mixed. Database connections use SQLx.
The broker does not create, migrate or write business tables. SQLite still stores
private access configuration via `--access-control-config`, or the existing
rules-configuration integration. Existing HTTP configurations remain valid.

## Queries and aliases

Enter the SQL in the Dashboard. Table names, joins, predicates and source column
names are yours. Fixed output aliases give the broker an explicit result contract.
Queries must be a single SELECT-compatible subquery (WITH/joins are supported),
with no extra statements; the broker adds an outer projection and limit.
A final semicolon is accepted. Bind parameters are never interpolated into SQL.

Authentication binds **$1 = MQTT username** and returns one text `password_hash`:

```sql
SELECT passwd AS password_hash
FROM business_accounts
WHERE account = $1 AND active = TRUE
```

Zero rows or a mismatched/missing password denies authentication. Multiple rows,
NULL/missing/wrong-type columns or malformed/unbounded PHC values fail unavailable.
Only Argon2id v19 PHC is supported; each PHC contains salt and m/t/p parameters.
Limits match local authentication: m<=65536 KiB, 1<=t<=10, 1<=p<=4 and m>=8*p.
Hash length<=512 bytes. The existing `mqtt-auth` hash_password example generates
suitable defaults. MQTT passwords are exact bytes, including NUL/non-UTF8; they are
never bound into SQL. At most four blocking hash tasks run across all PG providers,
and a cancelled request keeps its permit until the hash job actually finishes.
Authentication results are not cached.

Authorization binds **$1 = username, $2 = Client ID, $3 = publish/subscribe**:

```sql
SELECT permission AS effect, topic AS topic_filter, rank AS priority
FROM business_permissions
WHERE account = $1
  AND (device_id IS NULL OR device_id = $2)
  AND operation = $3 AND active = TRUE
```

Required output: non-null text `effect` (`allow`/`deny`), non-null text
`topic_filter`, and integer-compatible `priority` (converted to bigint).
The broker loads at most 256 rules; a 257th row makes the operation unavailable,
never silently truncates. Each filter<=65535 bytes, aggregate filter bytes<=1MiB.
Invalid rows fail the complete operation rather than being skipped.

Higher priority wins. Equal priority deny wins regardless of database row order.
No match denies. Publish matches the exact topic; Subscribe requires one allow
filter to cover the requested filter. Deny filters match any intersection with a
requested subscription: `allow tenant/#` plus same-priority `deny tenant/private/#`
therefore rejects subscribing to `tenant/#`, but permits `tenant/public/#`.
This is conservative; unions of partial allow coverage are not inferred.
Root wildcard `$SYS` isolation and shared subscription normalization reuse MQTT
core semantics. SQL LIKE is not an MQTT filter matcher.

## Connections, TLS and limits

Configure host, port, database, username, database password, TLS mode, optional
CA PEM and maximum connections per stage (1..32, default8). TLS defaults to
verify-full with trusted roots; a private CA can be supplied in PEM. Disable TLS
only through the explicit plaintext selection on a trusted network. The database
password is independent of a device's MQTT password. API reads show `[redacted]`;
sending that placeholder back preserves the previous password for the same backend.
Enter an empty string to replace it with an empty password. Protect the SQLite
configuration file: stored provider credentials are private, not encrypted at rest.

Pools are lazy (min0), idle timeout30s, bounded by existing configuration-generation
limits. Authentication and authorization have independent pools. Saving configuration
does not prove connectivity; **测试数据库连接** performs SELECT1. Authentication and
authorization test drawers also execute the configured SQL without creating sessions
or changing live counters/cache. Status shows pool total/idle connections.

The existing timeout100..5000ms includes pool acquisition, SQL and hash verification.
PostgreSQL statement/lock timeouts bound abandoned SQL execution; CPU hash work
cannot be forcibly stopped but is bounded. Requests also retain existing per-stage
and global concurrency caps. Queries run in read-only transactions and SQLx statement
logging is disabled. Use a database role with SELECT only on the intended tables;
read-only transactions do not sandbox arbitrary administrator-supplied functions.
Do not grant function execution or other privileges beyond the intended query.

DB failures/timeouts/pool errors are unavailable, distinct from business denial.
No fail-open or secondary-backend retry. Authorization allow/deny caching reuses
TTL0..300s, exact-resource keys, configuration generation and manual clear controls;
unavailable results are not cached.

New CONNECT (including Will topic), PUBLISH and each SUBSCRIBE filter check admission
before effects. Existing accepted QoS handshakes/retransmissions, queued deliveries,
retained replay, delayed Will/recovery do not query PostgreSQL again. Permission
changes affect later admissions subject to cache TTL; they do not retroactively
remove old grants. Phase6b remains the MQTT semantic contract.

## Build and verify

Normal builds include `postgres-auth`. To omit PostgreSQL from the broker binary,
use `cargo build -p lyrnormq --release --no-default-features`. That build does not
accept PostgreSQL configuration and the Dashboard hides the backend option.

For disposable local verification (loopback-only, no business data):

```sh
docker run -d --name lyrnormq-postgres-access-test \
  -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1:15432:5432 postgres:17
cargo test -p mqtt-auth-postgres -- --include-ignored
LYRNORMQ_BIN=/absolute/path/lyrnormq python3 tests/postgres_access_blackbox.py -v
```

The trust-auth test fixture is for isolated local tests only. Production deployments
must configure a restricted role and actual database credentials. No specific tables
are required by these tests; custom SQL uses synthetic VALUES data.

For the optional live TLS test, configure the disposable PostgreSQL fixture with a
CA-signed server certificate for `localhost` (the existing `tls_blackbox.Identities`
fixture can generate one), then supply `LYRNORMQ_POSTGRES_TEST_CA=/path/to/ca.pem`.
The test verifies both trusted success and hostname/untrusted-CA failures.

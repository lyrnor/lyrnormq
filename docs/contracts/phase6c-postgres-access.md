# Phase6c — PostgreSQL authentication and operation authorization

Adds PostgreSQL as an independently selectable authentication/authorization backend
under the existing access-control configuration/API. SQL is administrator-configured;
no business tables are created or migrated. SQLx uses PostgreSQL/Tokio/Rustls only.
Existing HTTP v1 configurations and the Phase6b MQTT admission semantics remain valid.

Authentication SQL binds username as $1, returns one non-null text password_hash
(Argon2id v19 PHC). Zero rows denies, multiple rows/invalid hash are unavailable.
Authorization SQL binds username/client_id/action as $1/$2/$3 (publish/subscribe),
returns text effect (allow/deny), text topic_filter and bigint-compatible priority.
No rows denies. Higher priority wins; deny wins ties. Invalid/oversized rows or more
than 256 rules fail unavailable, never partially authorize. Subscribe allow requires
single-filter containment; deny applies on any intersection at equal/higher priority.
System-topic isolation follows the existing matcher. SQL is parameterized and wrapped
as a bounded subquery, executed in a read-only transaction with statement_timeout.
Only a minimally privileged database account should be used; no remote SQL can run
on the broker's serialized state worker.

Pools, requests, hash tasks, rows, field bytes and deadlines are bounded. Hash jobs
retain a global CPU permit until blocking work finishes, even after cancellation.
No authentication caching; authorization cache retains existing generation/TTL/clear
behavior. Passwords, PHC values and SQLx errors are not logged or returned in APIs.
TLS defaults to verify-full; plaintext requires explicit selection. SQLite continues
to hold backend configuration. PostgreSQL holds business data only.

Existing RemoteAccess/AccessAttempt contracts are reused. Keep the mqtt-auth-http
public crate/API for compatibility and introduce a backend-neutral dispatch module
inside it; HTTP adapter and PostgreSQL adapter remain separate. Pool lifecycle is
bounded by existing configuration generations and no installed admitted policy may
retain pools. postgres-auth is a default-enabled feature with a disabled-build path.

Acceptance: unit rules/hash/config regression; actual PostgreSQL authentication,
custom SQL/aliases, binary password, deny and unavailable, read-only/timeout/row limits;
management save/redaction/restart and real MQTT311/5 admission; HTTP/local regressions;
fmt, clippy, workspace/frontend tests and release build. Record actual limitations.

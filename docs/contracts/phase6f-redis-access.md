# Phase6f — Redis authentication and operation authorization

Adds Redis as an independently selectable authentication/authorization backend
under the existing access-control configuration/API. Keys and HASH fields are
fixed; the broker does not create, migrate or write business keys. SQLite
continues to hold backend configuration. Existing HTTP and PostgreSQL
configurations and the Phase6b MQTT admission semantics remain valid.

Authentication key is `{prefix}auth:{username}`. The broker reads HASH field
`password_hash` (Argon2id v19 PHC, at most 512 bytes). A missing key or field
denies. An empty, oversized, non-UTF-8 or malformed PHC is unavailable. Other
HASH fields are ignored. MQTT passwords are verified locally and are never
written to Redis. Authentication results are not cached.

Authorization key is `{prefix}acl:{username}`. HASH field `publish` or
`subscribe` holds a JSON array of `effect` (`allow`/`deny`), `topic_filter`
and integer `priority`. Rules for one Client ID use the same JSON in the field
whose name is that action, ASCII `0x1F`, then the Client ID. The two fields
are merged. Higher priority wins; deny wins ties. No match denies. More than
256 rules, invalid JSON, an illegal filter or more than 1 MiB of filter bytes
fails unavailable. Missing key or field denies. Subscribe allow requires
single-filter containment; deny applies on any intersection. System-topic
isolation follows the existing matcher.

The key prefix is a literal 1..64 byte string ending in `:`, default
`lyrnormq:`. Usernames are copied into keys as raw bytes; Redis commands are
not templates. Logical database 0 only. No `KEYS`, `SCAN`, `EVAL` or cross-key
reads on the admission path. Each HASH field is length-checked before its body
is read, with a 1 MiB plus 64 KiB cap.

Pools, requests, hash tasks and deadlines are bounded. Password hashing shares
the existing process-wide limit of four Argon2 tasks and keeps the permit until
the blocking job finishes. TLS defaults to verify-full; plaintext requires
explicit selection. A private CA PEM replaces the default trust anchors for
that backend. Authorization cache retains existing generation, TTL and clear
behavior. Passwords, PHC values and Redis errors are not logged or returned.

`redis-auth` is a default-enabled feature with a disabled-build path. Unknown
backend kinds and Redis configuration on a build without the feature are
rejected. Admission still reuses RemoteAccess: new CONNECT (including Will
topic), PUBLISH and each SUBSCRIBE filter check before effects. Accepted QoS
handshakes, queued delivery, retained replay and delayed Will do not query
Redis again.

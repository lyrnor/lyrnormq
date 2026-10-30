# Phase4e — bounded admission and mandatory client certificates

Frozen before implementation, 2026-10-30. User scope is only authentication rate /
handshake concurrency protection and mTLS. No metrics, health endpoints, reload,
multi-listener work, deployment, soak or performance acceptance. Reuse Phase4/4b/4c/4d
protocol, session, outbox, storage, WS framing and closing; lib.rs remains thin.

## A. Admission configuration/API

Add these public `BrokerConfig` fields (no new root manifest dependencies):

| Field | Type | Default | Meaning |
|---|---|---|---|
| max_handshakes | usize |128| concurrent pre-CONNACK connections |
| max_auth_tasks | usize |4| outstanding credential/provider blocking jobs |
| auth_rate_per_second | u32 |1000| global authentication attempt refill;0 disables rate only |
| auth_burst | u32 |1000| token capacity when rate enabled |

Positive semaphore limits must fit Semaphore::MAX_PERMITS; auth_burst is always
positive. Limits belong to the existing single broker, not each Worker clone.
No per-IP/user maps, IP bans or TLS certificate-to-MQTT principal mapping.

- Try-acquire handshake slot immediately after accept and before TLS/mTLS/HTTP;
  overload drops the new socket without spawning work or touching MQTT state.
  Hold through all transport stages and initial enhanced AUTH until successful
  CONNACK has actually flushed. Established sessions consume only max_connections;
  all failure/cancellation/shutdown paths release the handshake slot.
- One global monotonic token bucket charges initial CONNECT and each received
  enhanced AUTH/provider step, including reauthentication. Failed credentials
  consume tokens; PING/PUBLISH/ACK/ordinary delivery and cancellation do not.
  Fixed-size bucket, overflow-safe integer arithmetic, no lock across await.
- Admission is fail-fast: no waiting queue for authentication rate or CPU slots.
  Acquire CPU slot before spawn_blocking; move its permit inside the blocking job
  so cancellation/timeout of the caller cannot release a still-running job's slot.
  Outstanding jobs remain <=max_auth_tasks even after clients disconnect.
- Initial refusal: MQTT311 CONNACK3, MQTT5 CONNACK89(Server Busy), if negotiated
  version/packet cap permits. Established reauth exhaustion uses DISCONNECT97
  (Quota Exceeded), never an invalid AUTH reason. No session takeover, Will or
  durable mutation before initial authentication success.
- CancelAuthentication cleanup bypasses rate and cannot enqueue an unbounded
  blocking job or wait behind a provider-held mutex on an async runtime thread.
  Preserve generation isolation and connection-local enhanced auth cleanup.
  Running blocking hooks cannot be forcibly aborted; built-in/bounded custom
  hooks must eventually return. Do not claim preemptive provider cancellation.
- Credential cancellation before actor admission must not create or take over a
  session. Once an authenticated CONNECT transaction is already committing, a
  caller timeout is not an atomic rollback: retain a generation-scoped cleanup
  responsibility so a late commit cannot leave an orphan online session/Will.
  Any asynchronous cleanup must retain the handshake admission slot until the
  transaction/cleanup resolves, remain bounded, and never disconnect a newer
  generation. Previously committed takeover is not falsely reported as undone;
  an accepted connection lost before CONNACK is cleaned as a network disconnect.

CLI: `--max-handshakes`, `--max-auth-tasks`, `--auth-rate-per-second` (0..u32MAX),
`--auth-burst` (1..u32MAX). Reject invalid/missing/duplicate new numeric options.
Default limits apply to TCP/TLS/WS/WSS; rate0 explicitly disables token throttling
without disabling handshake or CPU limits. No initial queue intended to absorb
unlimited login bursts. Existing authenticated traffic must remain responsive.

## B. mTLS identity API

Keep TlsConfig::from_pem and existing TLS/WSS behavior unchanged when no client CA.
Add `TlsConfig::from_pem_with_client_ca(cert_chain: &[u8], private_key: &[u8],
client_ca: &[u8]) -> io::Result<Self>` using the existing explicit ring provider,
TLS1.2/1.3, strict server identity parsing, and mandatory WebPkiClientVerifier.

- Client CA input is a nonempty PEM certificate bundle, bounded to1MiB; reject
  malformed/noncertificate/trailing junk input before listening. No anonymous
  client-auth fallback or custom insecure verifier. Trust roots are startup-only.
- Require a trusted, currently valid certificate chain with valid client-auth
  purpose and proof of private-key possession. Wrong CA, absent/expired/wrong-EKU
  certificate or bad handshake must fail before MQTT state/authentication/takeover.
- The certificate is a transport trust gate. MQTT username/password/enhanced AUTH
  and ACL are still enforced independently; no CN/SAN-to-username mapping.
- CLI `--tls-client-ca PATH` requires the existing complete --tls-cert/--tls-key
  pair. Reject empty/duplicate path and export-state combination. Read on blocking
  pool using the existing regular-file/1MiB limits before bind. Applies identically
  to TLS and WSS. No optional client auth, CRL/OCSP or certificate reload this phase.

## C. Isolation and acceptance

Admission Agent owns crates/mqtt-transport/** EXCEPT tls.rs and tls/** in an
assigned independent worktree. TLS Agent owns ONLY crates/mqtt-transport/src/tls.rs
and tls/** in another independent worktree. No overlapping file modifications;
TLS has no BrokerConfig changes and needs no public lib export changes. Quality
owns tests/** and CI in a third worktree; coordinator owns bin/rmqtt, docs and
root lockfile/integration. Every writer commits tested changes and reports hashes.

Admission tests: deterministic token refill/overflow, bounded blocking fake provider,
caller cancellation retaining permits, fail-fast overload and generation/state
invariants; independently exercise initial311/5 refusal, rate refill, established
liveness, TCP/TLS/WS/WSS handshake slot recovery, pending shutdown, enhanced auth
and reauth quota. No production test delay flags or weakened protocol assertions.
Include a controlled SQLite write-lock case spanning CONNECT's commit and caller
timeout; verify eventual generation cleanup, slot recovery and no orphan state.

mTLS tests: TLS12/13 × TLS/WSS × MQTT311/5 QoS0/1/2; strict server/client identity,
required/missing/wrong-CA/expired/wrong-EKU client cert, packet/state isolation and
good-certificate plus wrong MQTT credentials. Verify source timing: TLS13 clients
may observe a server certificate before failure, so attempt MQTT IO and require
no accepted CONNACK rather than relying solely on client connect() error.
Reuse persistent/offline/outbox recovery and existing Will behavior across mTLS
connections. No-cert TLS regressions must remain valid.

Freeze first, add failing regressions for confirmed bugs, then fmt, strict workspace
Clippy, Rust tests and independent raw/client suites. Record actual logs and skips;
do not claim complete security/capacity or add metrics/reload from the abandoned scope.

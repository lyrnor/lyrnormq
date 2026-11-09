# Phase4c — single-node TLS listener

Frozen before implementation, 2026-10-30. Cluster work is deferred. This contract
adds server-authenticated TLS to the existing single-node broker; Phase4/4b MQTT,
session, outbox, admission, expiry, Will and storage semantics remain unchanged.

## API and deployment

- Preserve `serve(TcpListener, BrokerConfig, watch::Receiver<bool>)` unchanged.
- Export `TlsConfig` from mqtt-transport: cloneable, immutable configuration;
  `TlsConfig::from_pem(cert_chain: &[u8], private_key: &[u8]) -> io::Result<Self>`.
  Parse and validate certificate/key pair before listening. Never log key bytes.
- Export `serve_tls(TcpListener, BrokerConfig, TlsConfig, watch::Receiver<bool>)`.
  One invocation owns one broker, as existing serve does. No concurrent plain/TLS
  listeners sharing one SQLite database; dual-listener state sharing is deferred.
- Rustls 0.23 / tokio-rustls 0.26 with explicit ring provider, TLS1.2 and TLS1.3;
  no global provider installation, insecure fallback, client certificate auth,
  certificate reload, SNI routing, WebSocket or ALPN requirement in this phase.
- CLI `--tls-cert PATH --tls-key PATH` is an all-or-nothing pair, selecting TLS
  instead of plaintext on `--listen`. Default address remains 127.0.0.1:1883;
  documentation uses explicit port 8883. Empty/duplicate paths fail; export-state
  with TLS options fails. Certificate/key reads run off the async executor and
  are bounded to 1 MiB per file; from_pem independently enforces the same bound.
- PEM supports a nonempty certificate chain and exactly one unencrypted supported
  private key. Empty, oversized, malformed or mismatched identities fail closed.

## Lifecycle and resource bounds

- Acquire existing max_connections permit before TLS handshake and hold through
  connection cleanup. Do not create MQTT session/auth state before TLS succeeds.
- Each TLS handshake has `handshake_timeout`, then existing MQTT CONNECT handshake
  has its own `handshake_timeout`: the two stages are independently bounded.
- Select shutdown while TLS handshake is pending; invalid/idle/raw TCP peers do
  not block accepting other clients or shutdown. Failure releases the permit.
- Set TCP_NODELAY on the underlying socket. Plaintext listener keeps its concrete
  TcpStream path; generic IO factoring must not introduce boxed IO for plaintext.
- TLS writes require flush under the existing write deadline: write_all alone
  does not guarantee encrypted bytes reached the socket. Handle TLS EOF/errors
  through existing ungraceful MQTT cleanup; TLS close_notify without MQTT
  DISCONNECT is still ungraceful for Will semantics.
- Normal MQTT DISCONNECT over TLS completes a bounded TLS close_notify without
  draining queued application messages. Keep plaintext writer-abort behavior and
  MQTT terminal-frame ordering unchanged. Abrupt socket/IO failures may abort.
- Keep packet limits on decoded MQTT bytes, independent of TLS record sizes.
  Existing bounded queues, write deadlines, receive maximum and Keep Alive apply.

## Acceptance and ownership

Transport Agent owns crates/mqtt-transport only, including its manifest and unit
tests, in an assigned independent worktree. Quality Agent owns tests and CI only
in another worktree. Coordinator owns bin/lyrnormq, root lockfile, docs and integration.
No Agent changes root Cargo.toml/Cargo.lock or another owner's files.

Quality adds independent Python ssl/raw MQTT tests using ephemeral OpenSSL test
identities: both MQTT versions and QoS0/1/2, verified CA/hostname, untrusted and
wrong hostname refusal, TLS1.2/1.3, invalid identity, plaintext rejection, timeout
and permit recovery, shutdown while handshaking, persistent resume and Will.
Run fmt, strict workspace Clippy, workspace tests, existing wire regression suites
and new TLS blackbox; report actual counts. No throughput/production claim from
correctness tests and no server deployment in this phase. lib.rs remains module
declarations and re-exports only. Durable schema remains unchanged.

# Authentication admission and mTLS — 2026-10-30

Validated code `37c872d` on `codex/admission-mtls`. Environment, exact binary hash,
source commits and compressed artifact hashes: [manifest](manifest.json).
Scope is only authentication/handshake admission and mTLS. No metrics, health,
reload, shared listeners, deployment, soak or performance acceptance was added.

## Actual results

| Gate | Result |
|---|---|
| Workspace build, fmt, strict all-target Clippy | passed |
| Workspace Rust tests |556 passed, no failures |
| New independent raw SSL/MQTT/WS acceptance |12 methods passed, no skips |
| Mandatory mTLS positive profiles |24: TLS12/13 × TLS/WSS × MQTT311/5 × QoS0/1/2 |
| Missing/wrong-CA/expired/wrong-EKU certificates |16 profiles rejected before MQTT acceptance |
| Valid certificate with wrong MQTT password |4 profiles rejected; certificate does not replace credentials |
| CA/CLI invalid configuration |15 profiles fail closed |
| Original server-only TLS suite |12 passed |
| Original WS/WSS suite |18 passed |
| Existing Paho2.1 WS/WSS interoperability |1 method /12 protocol-QoS profiles, passed |
| Original plaintext suites, both stores |18 × memory/SQLite =36 executions passed |
| Source/matrix guards and6 negative metadata tests |passed; prior clause dispositions unchanged |

Rate tests verify CONNACK3/89, failed credentials consume tokens, refill, existing
connection liveness and no denied-client takeover/Will. Enhanced reauth exhausts
with DISCONNECT97, while initial AUTH retains its handshake slot. All four
transports exercise handshake refusal, slot release, timeout and pending shutdown.

Rust controlled-provider tests verify cancellation never frees a still-running
blocking job's permit and cleanup never waits on the provider-held manager lock.
Monotonic integer bucket tests include burst, refill, disabled rate, rewind and
large-value saturation. Existing2000-actor concurrency tests explicitly raise
their configured admission limits and disable rate to retain their original
actor-concurrency purpose; low-limit gates are tested independently.

mTLS validates trusted chain, validity, client-auth purpose and key proof through
Rustls mandatory verification. TLS13 negative tests attempt MQTT IO and require
no accepted CONNACK; client connect() alone is not used as proof. Positive server
certificate validation is never disabled. Independent client credentials and ACL
remain in force. Certificate names are not mapped to MQTT principals.

SQLite offline/QoS2 recovery switches between mTLS TLS and WSS on the same stopped
database. The same packet identifier resumes with PUBREL in AwaitComp. The added
real SQLite BEGIN IMMEDIATE test also holds CONNECT commit across the network
deadline, observes EOF, unlocks, then requires late-generation Will BEFORE any
reconnect and subsequent Session Present/handshake-slot recovery in both versions.

## Cancellation failures and ownership fix

Three saved red artifacts are explicitly labeled **captured pre-fix tool
transcripts**, not reruns against final source:

1. Cancellation cleanup waited behind a provider-held mutex.
2. Socket handshake deadline did not interrupt actual provider waiting.
3. Cancelling CONNECT during SQLite commit released its cleanup admission bound
   before the transaction completed; source review identified the orphan-generation
   window. The independent raw SQLite case was run green only, not claimed red.

The fix does not roll back or conceal an accepted transaction. A bounded tracked
handoff retains actor reply and handshake-slot ownership; the network records its
resolved identity before adopting cleanup responsibility. Without adoption the
task waits for same-generation Disconnect completion before releasing its slot.
Newer generations are fenced, and shutdown joins responsibility tasks before
stopping actors/storage. An authenticated transaction that already started may
commit after timeout, then is cleaned as a network disconnect and may publish Will.
Initial rate/CPU/certificate denials still cannot mutate or take over a session.

The old binary feature-red log is separate; it shows unsupported new CLI options,
not a protocol failure. Module, raw and original regression logs retain their
actual commands/results. No skipped or failed run is relabeled passed.

## Reproduction and limits

```sh
cargo build --workspace --offline
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --offline -- -D warnings
cargo test --workspace --offline
python3 tests/admission_mtls_blackbox.py
python3 tests/tls_blackbox.py
python3 tests/websocket_blackbox.py
# With paho-mqtt==2.1.0 installed:
python3 tests/websocket_paho.py
python3 tests/check_conformance_matrix.py
python3 tests/acceptance_guard_negative.py
```

Offline builds require cached locked dependencies. Python/OpenSSL generate strict
ephemeral identities. The original plaintext suite list and results are archived;
run each with a fresh LYRNORMQ_TEST_STORAGE_PATH for SQLite. Every test owns temporary
listeners/process cleanup; no remote hosts or production files are touched.

Three independent worktrees separated admission core, TLS identity and quality;
read-only review confirmed the cancelled-commit fix after finding the gap.
Coordinator integrated CLI/docs. lib.rs stays declarations/reexports; durable
schema and MQTT import paths remain unchanged. No whole-protocol/security or
hardware-power-loss certification follows from these finite traces.

Admission is global, not per-IP. Running blocking providers cannot be forcibly
preempted; their permits remain held until they return. Default rate1000/burst1000
and concurrency128/4 are configured protections, not measured capacity. Client CA
reload, CRL/OCSP, certificate-principal mapping and other abandoned scope remain
unimplemented.

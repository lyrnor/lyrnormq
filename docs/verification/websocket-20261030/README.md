# WS/WSS verification — 2026-10-30

Code revision `80b478f`, branch `codex/single-node-websocket`. Environment, binary
SHA256, source commits and artifact hashes: [manifest](manifest.json). No remote
deployment, throughput test, memory benchmark or production readiness claim.

## Actual acceptance

| Gate | Result |
|---|---|
| Workspace build/fmt/strict all-target Clippy | passed |
| Final workspace Rust tests |540 passed, no failures |
| Independent raw WS/WSS Python methods |18 passed:15 network and3 byte/oracle tests |
| Paho2.1.0 |1 method,12 WS/WSS × MQTT311/5 × QoS0/1/2 profiles; no skips |
| Strict delayed normal TLS close |40/40 connections, TLS12/13 × both MQTT versions ×10 |
| Existing independent TLS suite |12 methods passed on final closing fix |
| Original plaintext integration |18 suites × memory/SQLite =36 executions, all passed |
| After final WS closing patch |MQTT5/QoS/Will suites × both stores =6 executions, all passed |
| Matrix/source guards |354 rows/1749 links;343 authoritative dispositions/1451 links |
| Negative acceptance-metadata guards |6 tests passed |

Raw framing covers upgrade pipeline bytes, exact path/query and subprotocol,
combined MQTT packets, split packets across independent messages, fragmented
binary with interleaved controls, empty messages, finite data bounds including
1-byte messages with125-byte Ping/Pong, invalid frames, header cap, timeout,
connection permit recovery and shutdown. WS control traffic cannot extend MQTT
Keep Alive. WSS verifies CA and hostname; no certificate verification is disabled.

Recovery covers retained replay, normal/WS-close/drop Will behavior and SQLite
SIGKILL while offline or in Q1 AwaitAck / Q2 AwaitRec / Q2 AwaitComp. Restarts switch
WS↔WSS sequentially on the same database; checks retain IDs, correct DUP and only
PUBREL during AwaitComp. No simultaneous processes share a live database.

The36 original plaintext runs completed before the last WS closing patch. After
that patch, the final540-test workspace, TLS suite and6 affected plaintext suites
were rerun; logs retain that distinction. This is reuse of unaffected integration
evidence, not a claim that every artifact used one identical binary.

## Failures retained and fixes

- HTTP pipeline: maintained HTTP handshake parsing initially treated trailing WS
  bytes as HTTP junk. Module regression reproduced it; upgrade now supplies only
  the bounded HTTP header, then restores the buffered bytes after101.
- Text fragment: complete-message decoding delayed rejection of a FIN=0 text
  frame. A failed module regression preceded a finite14-byte raw-header policy
  guard. It also rejects nonminimal lengths as an explicit strict-input policy;
  this sender encoding requirement is not mislabeled a receiver MUST.
- WSS normal close: immediate socket release left peer closing records unread.
  Independent strict `SSL.unwrap()` failed;10ms peer delay reproduced20/20 failures,
  and all four TLS/MQTT combinations failed the dedicated red regression.
  The fix waits for WS acknowledgement and TLS close_notify/EOF under one deadline
  only after a valid MQTT DISCONNECT. Strict final40-connection repetition passed.
  Other WS terminations remain abnormal and do not wait for peer EOF before Will.
- Two oracle assumptions were corrected without relaxing runtime requirements:
  echoing an already received error Close may hit BrokenPipe after peer closure;
  MQTT pre-session timeout may terminate by bounded EOF. Valid error close codes,
  permit recovery, normal1000 echo and normal WSS unwrap remain strict.

First failed raw rounds, deterministic delayed-close repro/red and final passing
logs are separate compressed artifacts. No original failed run is relabeled green.

## Reproduction

```sh
cargo build --workspace --offline
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --offline -- -D warnings
cargo test --workspace --offline
python3 tests/websocket_blackbox.py
python3 tests/tls_blackbox.py
# With paho-mqtt==2.1.0 installed in your environment:
python3 tests/websocket_paho.py
python3 docs/conformance/build_matrix.py --check
python3 tests/check_conformance_matrix.py
python3 tests/acceptance_guard_negative.py
```

Offline build assumes the locked dependencies are cached. Raw peers use Python
stdlib socket/ssl and OpenSSL-generated ephemeral strict identities. SDK evidence
used the existing `/private/tmp/lyrnormq-client-venv/bin/python`, Paho2.1.0. The delayed
regression is `WebSocketBlackbox.test_wss_normal_close_survives_delayed_reciprocal_tls_notify`;
run that method ten times for the40-connection repetition. Exact existing suite
lists and exit statuses are in the plaintext result JSON/archives.

Transport and quality changes came from independent worktrees/branches. A third
read-only specialist reviewed endpoint roles, frame guard and closing policy;
coordinator reconciled the contract before integration. lib.rs remains module
declarations/re-exports, public old imports and durable schema are unchanged.

MQTT5 matrix gains only applicable server rows6.0.0-1/-2/-4; client-only -3 stays
N/A. Historical TCP G1..G6 is not expanded into full WS certification. mTLS,
Origin ACL, trusted proxy identity, compression, certificate reload, shared multiple
listeners, long WS soak/Fuzz and WS-enabled Rust loadgen remain outside this phase.

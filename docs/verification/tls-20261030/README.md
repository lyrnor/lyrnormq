# Single-node TLS verification — 2026-10-30

Validated code revision `827073d`, business base `89293dc`. Actual local debug
binary hash, host/runtime environment and compressed-log digests are in
[manifest.json](manifest.json). No remote deployment or performance test occurred.

## Results

- `cargo build --workspace --offline`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --offline -- -D warnings`: passed.
- `cargo test --workspace --offline`: **521 passed**, no failures or ignored tests.
- `python3 tests/tls_blackbox.py`: **12 methods passed**, using independent Python
  ssl/raw MQTT and ephemeral OpenSSL identities. Both TLS versions × both MQTT
  versions × all QoS levels; verified CA/hostname, invalid identity/CLI, handshake
  limits/permit release/shutdown, clean close_notify, Will, SQLite SIGKILL recovery,
  40KiB multi-record payload and decoded MQTT packet-size rejection.
- Plaintext: **18 suites × memory/SQLite = 36 final suite executions passed**.
  Unittest output accounts for728 method executions including68 expected skips
  (storage/profile-specific branches);660 methods passed. Exact suites and final
  statuses: [final results](plaintext-final-results.json). Original logs retained.
- `python3 tests/check_conformance_matrix.py`, matrix rebuild check and six
  `acceptance_guard_negative.py` tests passed. Existing source-scoped dispositions
  were not promoted to TLS certification.

## Reproduced scheduling assumption and correction

The original memory RAP capacity suite failed once: an online subscriber could
move its ordinary copies from queued state into outgoing state before shared
admission, freeing queue capacity. A legal shared delivery violated the test's
assumption that all three copies competed for two occupied queued slots.

Independent comparison repeated the unchanged case20 times against each binary:
old `89293dc`19passed/1failed; TLS19passed/1failed, same assertion. This was not a
new TLS queue policy. Preserve the original failing suite log in
`plaintext-regressions.tar.gz`; comparison hashes/counts are in
`rap-admission-diagnosis.tar.gz`.

Quality commit `f01b67a` makes the recipient persistently offline before publication,
waits for MQTT DISCONNECT EOF and source PUBACK, then resumes Session Present.
Two ordinary copies occupy the queue until admission finishes. Assertions still
require exactly two ordinary copies and no shared copy; no runtime quota or
protocol behavior changed. Old/new × memory/SQLite ×10 repeats passed40/40.
The full corrected RAP suite also passed both storage modes on the coordinator.
Final36 results replace the original failed RAP execution with that complete rerun;
the failed attempt remains visible, rather than relabeled successful.

The TLS fixture initially failed Python's strict CA KeyUsage validation. Test-only
CA generation was corrected to include explicit keyCertSign/cRLSign; verification
flags, hostname checks and assertions were not weakened.

## Reproduction and scope

Build as above, then run `python3 tests/tls_blackbox.py`. OpenSSL and Python with
TLS1.2/1.3 support are required; no third-party Python MQTT package is used.
For each suite in `plaintext-final-results.json`, run `python3 tests/SUITE`;
for SQLite set `LYRNORMQ_TEST_STORAGE_PATH` to a fresh temporary database for each suite.
Each suite owns temporary listeners, clients and process cleanup.

Real independent worktrees owned transport and quality changes. A third Agent
reviewed CLI/startup and transport/flush/closing without editing shared files;
no blocking finding remained. lib.rs remains declarations/re-exports only and
the durable schema is unchanged.

These functional traces do not establish TLS capacity, production security
certification, exhaustive schedules or physical power-loss safety. mTLS,
certificate reload, WebSocket, shared dual listeners, TLS-enabled Rust loadgen
and production authentication rate limiting remain separate tasks. Prior Paho
suites and long Fuzz/soak campaigns were not rerun in this phase.

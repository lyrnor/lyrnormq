# HTTP authentication / authorization certificate configuration removal

User-directed simplification, 2026-11-02. Removes custom CA, client certificate,
private key, upload controls, validation and public DTO fields from both HTTP
stages. HTTPS continues to use reqwest default trusted roots and hostname
verification. MQTT listener TLS/mTLS and Kafka TLS are unaffected.

Requests containing `tls` are rejected. Stored empty/all-null placeholders are
normalized only during load, preserving credentials, enabled state, URLs and ACL
TTL; subsequent saves omit the obsolete fields. Nonempty/unknown old certificate
configuration is rejected without modifying the stored data.

## Actual verification

- Dashboard: 114 tests; TypeScript/Vite production build passed.
- Integrated Rust workspace: 763 tests excluding mqtt-auth-http, plus 26 tests
  in mqtt-auth-http; 789 passed, 0 failed, 0 ignored.
- Four independent loopback management HTTP/HTTPS tests passed in 2.068 seconds:
  field omission/strict rejection, untrusted HTTPS rejection, draft tests/secret
  redaction/authentication401, and unavailable status/ACL401 semantics.
- Workspace all-target strict Clippy, cargo fmt check and diff check passed.
- Regression-first: old implementation exposed the removed TLS field.
- Fixed startup validation failure cleanup to await SQLite owner close, preserving
  immediate exclusive reopen checks (32 repetitions). Removed runtime OpenSSL
  test subprocesses which inherited concurrent lock descriptors; used an existing
  public test-only certificate pair. Independent package parallel tests then
  passed 10 full runs (260 executions), followed by integrated tests above.

Implementation integration commits: 4612f08, bb306f5, 68ee9dd, e4fbb71.
This report covers the certificate simplification; it does not declare the pending
full HTTP/MQTT actor/recovery integration gate complete or report deployment.

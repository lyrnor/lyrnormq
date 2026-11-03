# Privileged HTTP downlink acceptance — 2026-11-03

[Contract](../../contracts/phase6e-http-publish.md), [API usage](../../http-publish.md).
Final design is Dashboard Bearer token; user canceled Basic/cache before integration.
No changes to mqtt-auth, mqtt-auth-http, Cargo.lock or CLI compared with main3ed1aec.
The canceled auth worktree was restored clean; none of its source/dependencies were
integrated. The public publisher handle is distinct from read-only management.

Environment: local Darwin arm64, Rust/Cargo1.98.1, locked/offline dependency builds;
Python stdlib raw HTTP/MQTT311/5 and real loopback auth/Webhook servers. Disposable
broker instances, ports, SQLite and journals; user's broker was not restarted.

| Check | Actual result |
| --- | --- |
| Rust workspace |837passed;1explicitlyignored live-PostgreSQL test |
| StrictClippy workspace all targets | passed |
| cargo fmt --all -- --check / git diff --check | passed |
| Native release | macOSarm64, embedded existing Dashboard |
| Real HTTP publishing |23/23,14.924s |
| Real system lifecycle regression |25/25,23.266s |
| Real existing HTTPauth/ACL regression |29/29,17.593s |

Final release SHA256:
`ea9dd57c7acadf921bed580e1429d01020ee8095ae91e11bf1574c91bedcde36`.
This exact immutable binary was used for all77 wire cases. The production source
was unchanged after its build (subsequent root additions were unit tests/docs).

## Verified boundaries

- SameDashboardBearer token, no Basic or separate token/credential cache. Missing,
  invalid, duplicate and Basic headers401; normal management protection unchanged.
- Configured MQTT backend deny401/403 and unavailable503 do not block HTTP202 plus
  real recipient delivery; backend request counts do not increase for the API.
  Normal MQTT subscription ACL continues to deny unauthorized filters.
- QoS0/1/2 and default UTF8 publishing across MQTT311/5; no synthetic MQTT session,
  CONNECT, Will, authentication call or client lifecycle event.
- Retained update/delete/replay, accepted retained update followed by SIGKILL and
  restart, offline persistent QoS2 after SIGKILL/resume.
- MQTT5 shared and NoLocal broker-origin semantics. MQTT311 $share remains rejected,
  preserving existing version-specific support (not falsely claimed supported).
- SQL observer emits client_id=http; actual Webhook receives transformed publication.
- JSON/body/actualpacket boundaries and reservedSYS guards; transport32slots/16MiB,
  preeffects budget/validation, fatal/start/stop, cancellation/storage ownership.

Meaningful old release regression returned405 instead of202. Earlier Basic-specific
red cases are historical canceled-design tests and are not current acceptance.
No production fix was needed after the final Bearer binary's first23-case run.

Transport agent fa8961b (176transport tests, strictClippy) and quality agents worked
in separate worktrees. Root integrated public API, Bearer route, state/wiring and
strict body validation with unit tests. No auth/cache branch production was merged.

No Linux execution, liveKafka/livePostgreSQL test, throughput/P99 claim, physical
power-loss guarantee or deployment. A lost/timed-out HTTP response is ambiguous;
there is no cross-recipient transaction or deduplication guarantee. Existing bounded
rule admission and durable-outbound acceptance semantics remain unchanged.

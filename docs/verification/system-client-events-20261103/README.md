# Client lifecycle system topic acceptance — 2026-11-03

Implemented contract: [phase6d](../../contracts/phase6d-system-client-events.md).
Usage: [system topics](../../system-topics.md).

Environment: local Darwin arm64, Rust/Cargo1.98.1, locked dependencies, native
release, stdlib Python raw MQTT311/5 sockets and real loopback HTTP Webhook servers.
All broker processes, ports and SQLite/journal directories in wire tests are owned
by disposable test fixtures. The user's existing broker was not restarted.

| Verification | Actual result |
| --- | --- |
| Rust workspace tests | 829 passed;1 explicitly ignored live-PostgreSQL test |
| Final executable tests after shutdown integration |40 passed (37unit+3logging) |
| Strict workspace Clippy all targets | passed |
| cargo fmt --all -- --check / git diff --check | passed |
| Native release | passed; macOS arm64 |
| Final lifecycle wire suite |25/25;20.513s wall time |
| Final static authentication regression |20/20;8.492s |
| Preceding integration basic MQTT regression |14/14;3.786s |
| Preceding integration Retained/Will regression |20/20;8.766s |
| Preceding integration Shared regression |22/22;10.125s |

Final binary SHA256:
`fbd8bca710827b92673112be6a5feb8c3c12b3374146f3ac9deb9aca06999485`.
Preceding integration binary:
`58b62221796145adb09da7e6bac4a82802bd582984c78129d576e4a5ba1cea49`.
The preceding basic/retained/shared runs predate only the additional anonymous
identity guard, overflow test and bounded executable outbound shutdown drain.
The complete workspace run contains the final170 transport and217 session tests;
the final executable40 tests were rerun after its drain integration.

## Verified behavior

- Real QoS1, nonretained `$SYS/events/client/connected` and `disconnected` topics;
  explicit wildcard subscriptions, root `#` dollar isolation, no schema_version.
- CONNACK success only; refused credentials do not generate connected/disconnected.
- Normal close, EOF, typed KeepAlive expiry, takeover, process graceful shutdown.
- Unique boot/connection/event identifiers; stale connection cleanup preserves the
  replacement; persistent subscriber offline delivery and reconnect.
- Broker origin is distinct from affected client metadata: MQTT5 No Local and
  shared delivery work, while SQL/Webhook retain affected client lane identity.
- Same event reaches MQTT and SQL→realHTTP Webhook.503 plus bounded shutdown and
  restart replays the durable event with unchanged event_id and disconnect reason.
- Reserved client PUBLISH/Will refused pre-effects even under permissive ACL;
  recovered legacy reserved Will retired durably; existing accepted QoS2 completes.
- Fixed128 metadata event queue (<17MiB queued raw identities), nonblocking overflow
  with warning/counter, finite2s lifecycle drain; executable permits2s outbound drain.

## Regression-first findings and limits

Old release lacked lifecycle events (real socket timeout). Session guard regression
failed before the fix. Initial21/23 lifecycle acceptance exposed missing graceful
shutdown Webhook delivery: the existing rules shutdown stopped SQL workers before
processing final observed events. The executable now stops MQTT/lifecycle producers,
allows up to2s for existing rules/outbounds to reach idle, then invokes unchanged
runtime shutdown. All25 final lifecycle cases pass, including an unavailable target
and durable restart. This does not promise delivery by exit during an outage.

The first sandbox-only workspace run failed socket tests with PermissionDenied;
rerunning with loopback permission passed. An older static-auth fixture's immediate
SQLite Will snapshot assertion transiently failed once under concurrent test load;
six targeted repeats and final full20 passed without weakening that assertion.

Lifecycle and rule admission remain bounded best effort before durable outbound
acceptance. Crash restart emits no synthetic per-client offline event. No physical
power-loss, Linux execution, live Kafka system-event acceptance, live PostgreSQL,
TLS/WS-specific event wire test, performance gain or deployment is claimed here.
Existing generic Kafka queue/scheduling and common network paths are reused.

## Isolated agent commits

Transport4f36949/2ecb1f4, session6a368dd, qualityc82527c/236db63/fc7c5cc were
implemented/tested in three separate worktrees and cherry-picked by the coordinator.
Coordinator owns contract, lockfile, executable shutdown integration and evidence.

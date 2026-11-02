# Phase6b — scoped HTTP authentication and operation authorization

Frozen 2026-11-02 after user explicitly narrowed scope. Supersedes Phase6a's
receive-time remote ACL, shared-member remote preflight, remote startup recovery
and special Will-authorization expansion. Reuse implemented HTTP runtime, cache,
SQLite configuration, management/UI and async authentication. No durable schema
or public wire configuration changes; lib.rs remains declarations/reexports only.

## Required behavior

CONNECT authenticates via HTTP204/401 before connection/session admission. Failed
or unavailable credentials never replace an existing same-ID connection or mutate
its persistent session. Bind username as existing user:<username> identity.
PUBLISH and each requested SUBSCRIBE filter authorize via HTTP204/403 before any
retained update, routing, session mutation or success response. Topic aliases must
be resolved by existing protocol/session validation before the remote publish ACL.
Unknown/malformed/unauthorized frames cannot escape this validation by a new path.
HTTPtimeout/networkfailure/5xx fail current operation closed, as unavailable rather
than business denial. Existing session, queued messages and subscriptions are not
removed because the HTTP service is unavailable. Per-filter SUBACK denial remains.
MQTT311 uses its existing conforming rejection/disconnect behavior, MQTT5 uses the
appropriate existing reason codes. No success-on-enqueue or fail-open authorization.

The same client actor may await one bounded HTTP preparation for the current
business frame. Other actors/connections must progress; no global lock acrossawait.
No new detached fanout/recovery state machine is needed for this phase. Prepare on
a clone, resolve missing point decisions, rerun unchanged base, seal before first
side effect. Recheck cancellation/generation before installing CONNECT or a late
business result. Install no speculative proposal, including speculative errors.

Once accepted, existing QoS handshakes, DUP retransmissions, PUBREL/PUBACK/PUBREC/
PUBCOMP, polling, delivery, retained replay and Will expiry do not issue fresh HTTP
ACLs. A duplicate inbound QoS exchange reuses the already-admitted exchange. Failed
requests leave existing state intact. HTTP changes affect subsequent new operations;
accepted in-flight exchanges continue under their admission. Current local static
policy behavior remains when HTTP is disabled. Static ownership checks are not
bypassed: a different username cannot resume another username's persistent state.

No per-recipient HTTP checks on actual send, shared selection/fallback or recovery.
Already-admitted subscriptions follow existing matcher/shared/NoLocal semantics.
No startup HTTP calls or permission-based pruning of stored sessions/queues/Will.
CONNECT need not add a remote Will ACL call; delayed Will uses existing accepted
state and local semantics. Do not claim retroactive HTTP revocation of old grants.

## API coordination

Add AccessAttempt::admitted_policy()->Arc<dyn AccessControl> with default policy()
for existing non-HTTP provider compatibility. HTTP attempts override with a compact
non-probing admission snapshot (flags/static fallback only), never pinning SDK
clients/generation slots/attempt memory after completion. Runtime::admitted_policy()
returns a non-probing base suitable for startup and existing accepted state.
Coordinator installs it as BrokerConfig.access_control while retaining original
local fallback inside Runtime. CONNECT actor uses request.access_attempt and seals
before admission; PUBLISH/SUBSCRIBE prepare a fresh attempt and use a scoped probe
only for that frame's requested action/resources. Housekeeping and installed engine
use admitted policy. Retained replay/shared/Will/control flow must not discover
new HTTP checks through internal AccessControl calls. Pending finish/rerun holds
admitted policy, not live HTTP attempt, and never re-requests authorization.

Non-probing policies must not mutate/prune existing state on provider availability
or later cache/config generation changes. Do not retain retired configuration
objects across idle connections. Pure both-HTTP-disabled path remains legacy local.
Configuration tests with8updates/idle clients must keep passing (generation cap4).

## Ownership and acceptance

Backend owner: crates/mqtt-auth-http/** and coordinated crates/mqtt-auth/src/remote.rs
only, independent /private/tmp/rmqtt-http-basic-backend branchcodex/http-basic-backend.
Transport owner: crates/mqtt-transport/** and coordinated mqtt-session only if
necessary, independent /private/tmp/rmqtt-http-basic-transport branchcodex/http-basic-transport.
Quality owner: tests/http_access* and HTTP CI test invocation only, independent
/private/tmp/rmqtt-http-basic-quality branchcodex/http-basic-quality. Preserve
independent oldscope tests as historical evidence, adapt active suite to new contract.
Coordinator: root/bin/lock/docs, management glue, integration/real acceptance.
No changes to Kafka, egress journals, MQTT protocol codec or unrelated modules.

Test authentication allow/refuse/unavailable for311/5 and takeover protection;
PUBLISH/SUBSCRIBE allow/refuse/unavailable forQoS0/1/2, mixed filters, topic aliases,
no unauthorized retained/routing/state effects; binary passwords/cache0/expiry/
clear/config changes; ACK/control and pending QoS2 continue without HTTP rechecks;
existing queued/retained/Will and shared routing generate no receive-time RPCs.
Run fmt, strictClippy/workspace/frontend tests, real HTTP/MQTT scoped blackbox,
local static-auth regressions, native Release and locked musl static build.
Record actual counts/results and remaining limits; do not merge/push automatically.

# Phase2o — decomposed whole-server acceptance

Pinned OASIS MQTT5 OS and authoritative DOCX plus MQTT311's explicit dual-profile
boundary are the source scope. Public features/profiles, schema7 and prior frozen
contracts remain. Protocol/Transport/Quality writers use separate assigned worktrees;
coordinator owns inventory, gate metadata, contracts, CLI/docs and integration.
Production fixes require exact normative evidence, a failing regression and a new
subsection here BEFORE implementation. Session/storage findings go to their owners.
No speculative sender-role requirement is inferred from a property name alone.

## Required finite acceptance criteria

G1 Source completeness: inspect authoritative DOCX and pinned HTML, enumerate and
classify unnumbered MUST/SHALL/REQUIRED, explicit ProtocolError/Malformed constraints
and tables. Resolve source anomalies and client/conditional/non-normative scope;
link implementation/test evidence or record each unresolved obligation explicitly.
G2 Endpoint directions: review allowed packet/property/reason contexts and client/
server roles at codec versus connection boundaries. Exercise legal control output,
invalid inbound types, initial/auth/connected/reauth states and terminal caps.
G3 Cross-layer transitions: verify generation/takeover, durable ID/QoS stages,
ReceiveMaximum/alias/expiry and no admission/ACK before required validation/commit.
G4 Fault recovery: actual storage contention/failure and process interruption must
prove committed ACK barriers, prior-state rollback and valid restart/replay. No
physical power-loss claim from SIGKILL. Bound time, resources and test databases.
G5 Fuzz: commit reproducible seed corpus/targets, run stable deterministic mutation
and actual coverage-guided campaigns when toolchain can be provisioned. Record
exact toolchains/seeds/run/time/size limits and all crashes/minimized regressions.
Never call a deterministic loop coverage-guided fuzzing. Fix confirmed findings.
G6 Unified closure: required checks, independent review and final same-artifact
wire/Paho regression; inventory/evidence guards and no unresolved confirmed MUST
mismatch in assessed scope. Preserve both existing SHOULD policy exceptions.

Each criterion must have a finite evidence statement and limitations. Bounded tests
are not a proof over every input; optional extensions and external producer duties
are not invented server MUSTs. Close only actually completed criteria; any remaining
source/direction/role issue keeps the parent gate open. Passing these criteria is
scoped project acceptance, not certification, full production readiness or a claim
that future normative/fuzz defects are impossible.

## Gate metadata contract

docs/conformance/server-acceptance.json has schema_version1, gate_id matching the
existing U7 row, scope string, overall_status open|passed, and criteria list.
Each criterion: id G1..G6 exactly once, title, required true, status open|passed,
requirements (nonempty strings), evidence (path:symbol references), commands
(actual command/results), limitations (strings). Passed criteria require nonempty
evidence AND commands. overall_status passed is permitted only when all required
criteria pass. U7 cannot be verified while any required criterion is open. Quality
extends the offline guard to enforce this shape/reference/dependency constraint.
Coordinator updates source-level decisions/evidence only after real audits/runs.

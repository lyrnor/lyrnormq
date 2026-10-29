# Phase2m — remaining normative interpretation and evidence

Pinned OASIS5 OS source/digest and Phase2k/l corrections remain authoritative.
Independent Protocol, Session and Transport writers own their isolated crate paths;
Quality owns tests/CI in its own worktree after a slot releases. Coordinator owns
contracts/matrix/docs/integration. Preserve schema7, public paths, thin lib.rs and
existing clock/session/outbox/storage architecture.

## Frozen acceptance scope

Audit remaining refusal contexts, conditional ClientID handling, CONNACK reasons,
AUTH diagnostic independence, property ID/type/context cross-product, optional
Unicode rejection policy, RAP overlap, default ordered Topics and PFI policy.
Use independently specified tables and actual byte/state/wire evidence; do not
infer a whole obligation from sibling tests or a feature name.

Tests may validate existing behavior. Confirmed mismatches require a failure-first
regression and a correction subsection here BEFORE production changes. Optional
choices, ambiguous cross-version refusal and unresolved mixed-RAP interpretation
are not automatically MUST defects. Do not sacrifice MQTT311 behavior merely to
simplify MQTT5 assertions. Keep unresolved rows explicit and distinguish disabled
conditional behavior from missing implementation. No public API/schema change is
approved by this initial scope; propose it first if required.

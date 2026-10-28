# MQTT5 normative coverage matrix

Source: [OASIS MQTT Version5.0 OS, 2019-03-07](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
This is a clause/evidence audit, not a feature checklist or a conformance declaration.
The assessed implementation is the TCP broker; client-only and conditional transport
requirements are assessed explicitly, not silently counted as successful tests.

## Artifacts and scope

- `mqtt5-matrix.csv`: one row per inventoried identifier/candidate, with implementation
  symbols, test cases, reviewers, status and the scope/limitation of the evidence.
- `import_source.py`: reproducible local-HTML inventory import; no network or status inference.
- `catalogue.json`: source URL/charset/SHA256,252 distinct literal numbered identifiers
  and102 supplementary paragraph candidates with stable content hashes/block offsets.
- `reviews/*.json`: reviewed observations, source commits and actual verification runs.
- `build_matrix.py`: deterministic offline merge. Contradictory observations retain
  the more conservative status; a codec test cannot hide a missing runtime assertion.

The official HTML contains250 literal numbered identifiers before AppendixB and
252 including the non-normative appendix index. These are identifier occurrences,
not252 independently applicable broker obligations. The102 additional candidates
are paragraphs without a numbered marker containing RFC2119 keywords or protocol/
malformed-error terminology. Some are terminology, client duties or optional rules.
They are preserved until reviewed, rather than inferred green. The heuristic is NOT
an exhaustive extraction of every normative sentence, table range or cross-reference.
An unmarked constraint/table sweep remains an explicit acceptance task.

HTML is windows-1252. The recorded digest pins the exact downloaded source;
regeneration must preserve the original bytes and account for source anomalies:

- `MQTT-3.2.2-8`: the appendix index has a normal identifier but the corresponding
  body reason-code paragraph has the malformed literal label `T-3.2.2-8]`. The body
  requirement still exists; this is a source-label problem, not permission to ignore it.
- `MQTT-4.2.0-1`: appendix alias of body `MQTT-4.2-1`; do not double count.

No full specification text is copied into the inventory. Paragraph hashes identify
review targets; the linked official document remains the behavioral authority.

## Status semantics

| Status | Meaning |
| --- | --- |
| verified | Auditor inspected the obligation and ran the referenced tests; validity is bounded by the recorded scenarios, not a proof over all inputs |
| test-gap | Implementation/context was inspected but complete, independent or adversarial evidence is missing; includes unresolved normative interpretation |
| gap | A concrete implementation mismatch is confirmed and remains unfixed |
| policy-exception | A deliberate deviation from a SHOULD recommendation with explicit tradeoff; never a waiver of a MUST |
| not-applicable | Client-only, disabled conditional extension, editorial candidate or source alias with an explicit reason |
| unreviewed | No accepted clause-level review; feature presence and nearby tests do not imply compliance |

No percentage based on these heterogeneous rows is reported. In particular,
not-applicable rows, aliases and heuristic candidates cannot inflate a pass rate.
Closed defects remain recorded in the fixes table and regression cases even when
their current rows become verified. See `docs/progress.md` for actual final totals.

## Current screening result

All354 inventoried entries have a first review disposition. This includes252 literal
numbered entries and102 unnumbered candidates, not354 distinct applicable MUSTs.
There are954 validated implementation/test references. Scope remains the pinned OS
source and the recorded scenarios, with no full conformance claim.

| Disposition | Numbered entries | Supplementary candidates | Total |
| --- | --- | --- | --- |
| verified within recorded evidence |198|23|221|
| test-gap / unresolved interpretation |20|41|61|
| not-applicable / source alias / editorial |34|36|70|
| SHOULD policy-exception |0|2|2|

Three confirmed implementation mismatches were fixed; none of those remains open.
The61 evidence gaps still block a full conformance conclusion. A screened candidate
is not automatically an executable requirement, and screening does not exhaust all
unnumbered sentences or property table combinations. Never turn this table into a
compliance percentage.

## Confirmed defects corrected in Phase2k

| Finding | Evidence and result | Source commit |
| --- | --- | --- |
| K1: server DISCONNECT during initial AUTH | `MQTT-3.14.0-1`; real TCP failure-first cases now receive failure CONNACK before acceptance; established reauth remains DISCONNECT |689624b|
| K2: multi-filter UNSUBSCRIBE observed stale state | `MQTT-3.10.4-6`, `3.11.3-1/2`; duplicate ordinary/shared filter sequence produces00,11,11; invalid later input preserves atomic rejection |4ac44bf|
| K3: public AUTH codec accepted omitted method | `MQTT-4.12.0-5`, unnumbered3.15.2.2.2; encode/decode reject missing method, with connection equality still enforced at Transport |81926bb|

Each correction was frozen in `docs/contracts/phase2k.md` before implementation;
regressions were captured failing before fixes. Existing clocks, schema7, sessions,
SQLite/outbox and aliases were reused. `lib.rs` stays declarations/reexports only.

## Keeping the matrix reviewable

```sh
python3 docs/conformance/import_source.py /path/to/mqtt-v5.0-os.html --check
python3 docs/conformance/build_matrix.py
python3 docs/conformance/build_matrix.py --check
python3 tests/check_conformance_matrix.py
```

Update a review only after inspecting the exact source clause and implementation.
Record real test symbols and commands/results, preserve scope limitations, regenerate
CSV, and run the guard. New confirmed gaps require a failing test and a frozen
behavior/API correction before owner implementation. Never promote a row merely
because another row in the same feature passed. CI checks inventory/evidence and
staleness offline; it does not substitute for reviewing the normative meaning.

## Remaining acceptance work

The current catalogue is screened. Prioritize the remaining explicit test gaps and
unresolved interpretations, then the exhaustive unnumbered/table sweep. Explicit open interpretations include mixed-RAP overlap
and adversarial/interleaved ordering. Shared QoS1 redistribution after selected-session
termination remains a documented SHOULD exception, not seamless group redelivery.
Secure transport is absent; PLAIN is not encryption. Additional SASL mechanisms,
TLS/WebSocket, broad Fuzz/fault/client campaigns and measured performance/operational
hardening remain separate tasks. None alone proves or disproves MQTT5 conformance.

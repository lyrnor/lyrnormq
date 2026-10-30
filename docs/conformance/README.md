# MQTT5 normative coverage matrix

Source: [OASIS MQTT Version5.0 OS, 2019-03-07](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).
This is a clause/evidence audit, not a feature checklist or a conformance declaration.
The original whole-server assessment covers the TCP broker; Phase4d adds separate
WS/WSS clause evidence. Client-only and conditional transport requirements are
assessed explicitly, not silently counted as successful tests.

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
There are1749 validated implementation/test references. Scope remains the pinned OS
source and the recorded scenarios, with no full conformance claim.

| Disposition | Numbered entries | Supplementary candidates | Total |
| --- | --- | --- | --- |
| verified within recorded evidence |219|62|281|
| test-gap / unresolved interpretation |0|0|0|
| not-applicable / source alias / editorial |33|38|71|
| SHOULD policy-exception |0|2|2|

Three confirmed implementation mismatches were fixed; none of those remains open.
The finite project acceptance gate G1..G6 passed in Phase2o. A screened candidate
is not automatically an executable requirement. Source review, endpoint roles and
actual campaigns are separately recorded below; they do not establish exhaustive
input correctness or official certification. Never turn this table into a compliance percentage.

## Confirmed defects corrected in Phase2k

| Finding | Evidence and result | Source commit |
| --- | --- | --- |
| K1: server DISCONNECT during initial AUTH | `MQTT-3.14.0-1`; real TCP failure-first cases now receive failure CONNACK before acceptance; established reauth remains DISCONNECT |689624b|
| K2: multi-filter UNSUBSCRIBE observed stale state | `MQTT-3.10.4-6`, `3.11.3-1/2`; duplicate ordinary/shared filter sequence produces00,11,11; invalid later input preserves atomic rejection |4ac44bf|
| K3: public AUTH codec accepted omitted method | `MQTT-4.12.0-5`, unnumbered3.15.2.2.2; encode/decode reject missing method, with connection equality still enforced at Transport |81926bb|

Each correction was frozen in `docs/contracts/phase2k.md` before implementation;
regressions were captured failing before fixes. Existing clocks, schema7, sessions,
SQLite/outbox and aliases were reused. `lib.rs` stays declarations/reexports only.

## Phase2l evidence followup

Independent literal fixtures add42 singleton/property packet cells, value/type/
context negatives and repeatable-property ordering. Session tests cover identifier
allocation/release/recovery and diagnostic independence. Actual wire tests cover
all75 invalid reserved-bit combinations for five control types, identifier flows,
takeover and byte-preserving Unicode/space equality. No new production defect was
confirmed; production behavior/API/schema are unchanged.

The followup closes44 evidence gaps and reclassifies one client-only obligation,
reducing61 test gaps to16. This is not45 new protocol features or a compliance
percentage. Full property/direction tables, PFI policy, refusal contexts, RAP and
adversarial ordering still need separate acceptance. See tasks.md for exact rows.

## Phase2m evidence followup

The independent Table2-4 oracle covers27 property IDs across16 columns (Will and
15 packet types):432 cells,64 allowed and368 prohibited. Exact encoding/decoding,
typed truncation and104 unassigned identifiers supplement the position table.
Unicode policy explicitly distinguishes mandatory invalid strings from optional
control/noncharacter rejection. These tests do not certify endpoint direction or
all possible values merely because packet-position syntax passed.

Ordering, legal CONNACK output and AUTH diagnostic evidence close additional rows;
ClientID rejection is conditional under the current accept-all-valid-ID policy.
Wrong protocol names already close silently; the old matrix had conflated that
path with unsupported levels. Unknown levels preserve the MQTT311 refusal rule,
so the MQTT5-specific pre-negotiation interpretation remains explicit. No production
change was needed. At that stage,7 rows remained: unknown-version context, mixed RAP,
PFI origin/forwarding, broad routing evidence and overall conformance. They are
inventoried gaps, not a claim that all other unmarked requirements are exhaustive.

## Phase2n resolved interpretations and behavior

The six specific boundary rows now have dispositions; the remaining row is the
whole-server acceptance gate U-7.1.1-56088afba646. This is not a declaration of
complete MQTT5 conformance or an exhaustive inventory of every unmarked obligation.

- Mixed RAP: for new live RETAIN1 publications and V5 recipients with eligible
  ordinary RAP0/RAP1 overlaps, one copy per matching subscription preserves that
  entry's grant, flag and optional identifier. This uses3.3.4's permitted multiple-
  copy path; it does not claim the standard uniquely specifies this algorithm.
  Homogeneous/nonretained/311/shared/replay behavior and captured old work remain.
- PFI: the two producer-assertion candidates are not-applicable to this broker as
  origin producer. The client supplies PUBLISH/Will bytes; forwarding preserves
  them and PFI. Optional no-validation is supported by3.1.3.2.3,3.3.2.3.2/3.3.2-4
  and the deployment explicitly discussed in5.4.9. Malformed client data is not
  certified as valid UTF8. Existing forwarder obligations retain positive tests.
- Unknown versions: `--protocol 5` gives explicit V5 semantics; `--protocol 3.1.1`
  gives explicit legacy semantics. Default Both preserves compatibility. The
  version row is verified for explicit V5; this does not infer protocol intent from
  unknown bytes or certify Both's fallback as universally MQTT5-specific.
- The broad matching paragraph describes dynamic-resource/ACL choices, not an
  additional universal-proof mandate. Existing bounded independent matcher, ACL
  and routing evidence verifies the implemented choices without claiming all inputs.

Schema7 stays unchanged. Actual old-artifact upgrade preserves old combined
inflight/queued metadata and applies splitting only to new fanout. The dedicated
upgrade test needs `/tmp/rmqtt-p2n-baseline`; it passed locally with the previous
real executable. A fresh CI host without that executable explicitly skips this
one SQLite case; the rest of the new suite is mandatory in its applicable mode.
Hosted CI execution is not claimed. Frozen decisions are in contracts/phase2n.md.

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

The current catalogue is screened. The remaining whole-server gate requires an
independent completeness assessment of applicable unnumbered/table/direction and
cross-layer obligations, plus broader independent client/fault/corpus evidence. Shared QoS1 redistribution after selected-session
termination remains a documented SHOULD exception, not seamless group redelivery.
The original Phase2o assessment covered plaintext TCP; PLAIN is not encryption.
Phase4c subsequently adds optional TLS1.2/1.3 with separate transport acceptance
under [its contract](../contracts/phase4c-tls.md). The historical TCP evidence and
row dispositions do not certify the new encrypted transport. Additional SASL
mechanisms, broad Fuzz/fault/client campaigns and measured performance/
operational hardening remain separate tasks. None alone proves or disproves MQTT5
conformance.

## Phase4d conditional WebSocket acceptance

WS/WSS now has separate scoped acceptance under
[Phase4d](../contracts/phase4d-websocket.md) and
[its verification record](../verification/websocket-20261030/README.md).
MQTT-6.0.0-1, -2 and -4 are verified within actual raw framing, malformed-input,
subprotocol and standard-client scenarios. MQTT-6.0.0-3 remains client-only N/A;
server refusal is not proof that external clients conform. Corresponding DOCX
paragraphs and4.2 byte-stream evidence were reconciled, with1451 authoritative
source/test links. The IANA table is registry description, not a new runtime duty.
Historical TCP G1..G6 scope is unchanged; this is not whole-server WS certification.


## Phase2o whole-server project acceptance

[Six frozen criteria](../contracts/phase2o.md) are passed in
[server-acceptance.json](server-acceptance.json). U7 now records this bounded
project acceptance, with zero inventoried test-gap rows. This is not a full MQTT5
compatibility or production-readiness declaration. Explicit V5 TCP scope, default
Both unknown-level qualification and the two SHOULD policy exceptions remain.

The independently imported official DOCX adds a separate completeness ledger:
285 keyword/error paragraphs and58 body tables,343 exact dispositions with1436
code/test links. This is a source inventory, not343 additional independent MUSTs.
[Importer](import_authoritative.py), [inventory](authoritative-inventory.json),
[semantic review](authoritative-review.json), [endpoint roles](endpoint-review.json)
and [offline checker](check_authoritative.py) preserve source hashes and applicability.
The seven literal unnumbered MUSTs and constraints sharing paragraphs with numbered
requirements were reviewed; five endpoint table dependencies were reconciled.
A keyword extractor alone is not a semantic proof.

Final evidence: Rust413,629 unique raw-wire unittest executions in memory/SQLite,
all Paho2.1 suites (67 separate unittest methods plus script assertions),9 SQL fault
cases,20,000 deterministic mutations and two actual cold guided Fuzz campaigns.
See [Fuzz budgets/results](../../fuzz/README.md) and [progress](../progress.md).
Real SQLite busy errors and SIGKILL validate commit/replay boundaries, not physical
power-loss safety. Finite traces do not exhaust concurrent schedules. No confirmed
production defect was found in this phase, so runtime/schema/API were preserved.

CI now rejects missing source decisions/review files, duplicate criteria, evidence
omissions and attempts to mark U7 verified with open criteria. Six negative tests
exercise those failures against isolated copies of the real metadata. Hosted CI
was configured but not run during this local acceptance.

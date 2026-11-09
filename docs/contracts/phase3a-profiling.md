# Phase3a — measured profiling before optimization

Freeze instrumentation/diagnostics first; no optimization is authorized by this
contract. Schema7/container1, proposal/discard/commit/install/reply and live byte
limits remain unchanged. Thin lib.rs only declares/reexports modules.

Opt-in LYRNORMQ_PROFILE=1 enables bounded process-wide counters/timing (Instant,
saturating totals, fixed labels, no payload/client-ID tracing). Disabled mode must
not collect times/allocate per event. Inclusive nested spans overlap: never add
nested percentages as exclusive CPU shares. This is instrumented elapsed time,
not statistical CPU profiling. Use identical small full-topology campaigns on
instrumented baseline; uninstrumented old/new formal campaigns stay comparable.

Session owns mqtt-session::profiling public module: fixed Operation enum covering
Clone, RecordsLive, RecordsPersistent, SerializeLive, SerializePersistent, Bounds,
Route, RouteBuild, Authorization; span(Operation)->Span RAII; report_json()->Result<String,
serde_json::Error>. Fixed atomic count/nanoseconds/bytes statistics; explicit record_bytes(Operation,usize) helper for serialization.
RouteBuild measures only Router construction/subscription indexing within Route. Span disabled is a no-op. Manual Clone may
replace derive only to time identical field clones; no copy-on-write or cache yet.
Instrument records/serialization/bounds/route/authorization without skipping work.

Transport owns worker profiling: fixed request categories, proposal clone,
command elapsed, snapshot elapsed+bytes, SQLite commit elapsed+bytes, success/error,
PollDelivery total/empty and notification fanout. Report on normal worker shutdown
(or bounded periodic checkpoints on the blocking worker) to LYRNORMQ_PROFILE_PATH;
TSV v1 transport rows and a SESSION_JSON line from session report_json, no extra
production dependency required. File I/O belongs only to blocking worker, never
async network tasks. Profile output failure must not fabricate successful evidence
or alter ACK/commit outcomes; emit diagnostic error. SIGINT drives graceful shutdown
for complete counters. Tests demonstrate opt-in/no-op/totals and operation semantics.

Quality owns benches/phase3/** and dedicated tests/phase3a_*: capture failure header,
reason, publisher/sequence/stage, partial ACK/receipt counts, elapsed and available
RSS samples, exact argv/artifact/flow, preserve stderr/profile after failure/timeout.
Explicit failed campaign status; never pool failed runs into successful summaries.
Bound buffers, time and telemetry. Add opt-in profiling launch/shutdown support and
small full-topology pilot (not four-subscriber smoke) without weakening existing
4096-publication/3-repeat comparable formal profile. Keep ACK-only stress separate.

Dependencies: session profiling API -> transport compilation integration; quality
can implement diagnostics in parallel. Coordinator freezes optimization contract
ONLY after measured evidence and ACK-only diagnostic outcome, then assigns separate
optimization ownership/worktrees. No speculation is promoted to a confirmed bug.

Transport diagnostic addition: fixed256 ERROR_REASON code/count counters for actual
worker failure reasons, plus unknown IO error count. No client/payload capture.
This separates observed quota/storage/protocol errors from deadline-only failures;
a timeout without a captured packet cannot establish a previous unknown reason.

Optional separate queue_wait span: opt-in Command timestamp before bounded sender
send; worker receipt records mailbox-capacity wait plus queued wait. Command elapsed
still excludes this wait. Internal Option<Instant> adds no timestamp when disabled.
This elapsed queue delay is not CPU time and may overlap other requests' execution.

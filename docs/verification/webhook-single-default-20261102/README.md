# Single-message Webhook default acceptance — 2026-11-02

New Dashboard-created outbounds use batch_size1 and batch_wait_ms0. Other defaults
are unchanged. Existing100/50 and custom saved policies are preserved, not migrated
or rewritten. Batching remains opt-in through advanced settings. POST JSON envelope
continues to contain an events array, now one event per request by default.

Isolated frontend worktree submitted22a8e97; root integrated and verified76 frontend
tests, strict TS/Vite/Prettier, Fmt, workspace all-target Clippy -D warnings,620 Rust
tests and native Release build. Regression explicitly failed with old defaults
before the two factory fields were changed; tests assert literal1/0 in the actual
closed-basic form request, not a comparison solely against the same factory.

Actual isolated Chrome created a Webhook without opening advanced settings and a
rule for topic qa/single. Authenticated config read confirmed persisted1/0. A real
MQTT5 client sent10 QoS0 messages in one burst: the local HTTP receiver recorded10
POST requests with one event each and every sequence0..9 delivered. This is a
functional check, not a latency/throughput benchmark or a guarantee of zero total
delivery time. Existing user services/profile untouched; QA services stopped.

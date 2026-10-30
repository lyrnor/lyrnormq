# Automatic identity and shared UI acceptance — 2026-10-30

Local Darwin arm64, Rust/Cargo1.98.1 stable, Python3.14.7. Native release only; no
server deployment or new performance measurement. Two independent implementation
worktrees and a read-only reviewer were used.

## Checks

69 frontend tests including shared controls/labels/tabs, cryptographic ID creation,
receiver-safe Crypto calls/fallback/collision limit, retry stability, old-ID editing,
secret protection, stopped controls and duplicate-name destinations. Strict
TypeScript/Vite production build/Prettier pass. Workspace Fmt, all-target Clippy
-D warnings, 620 Rust tests and embedded release build pass.

The later 69th test only added duplicate-name selection coverage; frontend
production source was already built with the same integrated code. Actual test
logs contain the preexisting jsdom window.scrollTo-not-implemented warning from
app navigation; actual browser reports no console or CSP errors. Regression RED
logs retained.

## Real browser

Separate headless Chrome/CDP profile, temporary local Broker and synthetic
MQTT5 publisher/Webhook receiver; no existing user service/profile touched.
Created two same-name Webhook outbounds using only Name and URL; generated IDs
were valid/distinct without visible input. Labelled Switch/Checkbox/Textarea and
controlled Tabs were exercised through native pointer interaction. Bound only the
second target by visible /secondary URL, then observed actual MQTT->SQL->HTTP.
Renaming preserved the outbound ID and existing rule reference; credentials
reopened masked/redacted. Disabled randomUUID in the QA page, created another
rule through native getRandomValues fallback, then restored the browser method.
No internal IDs appeared in page text or action ARIA. Mobile viewport/document
width390, component switch/checkbox/textarea confirmed. See browser-checks.json,
synthetic delivery JSON and real desktop/mobile screenshots. QA services stopped.

## Scope

Only Dashboard authoring hides/generates rule/outbound IDs. Config API version2
still uses stable IDs and outbound_ids; SQLite and MQTT session/ACK/outbox schema
are unchanged. Client ID/Subscription ID remain operational protocol fields.
Checkbox/Switch/Label/Tabs use existing Radix primitives; Textarea/Field/Alert
consolidate styling and label association. No new dependency or CSP relaxation.
No claim that every HTML layout or the custom plot should be replaced with a UI
library; existing componentized Drawer retains native modal semantics.

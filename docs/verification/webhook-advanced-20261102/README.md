# Webhook basic/advanced editor acceptance — 2026-11-02

Implemented in isolated frontend worktree and integrated on
codex/webhook-settings-integration. Only Dashboard source/components/tests change;
management API, SQLite schema, backend delivery behavior/limits are unchanged.

## Behavior

Basic editor contains name, Webhook address and authentication/headers. A Radix
Collapsible defaults closed and contains enabled/type plus batch count/wait, request
timeout, maximum attempts and retry interval. No queue-capacity input exists, even
when expanded. New outbounds keep system default256; basic-only edits preserve
existing capacity and non-default policies. This does not implement adaptive queue
resizing or remove the existing API field. Tuning summaries removed from list rows.

Collapsing preserves draft values. Hidden invalid policy is checked before saving,
reopened and focused/scrolled into view; unmounted closed controls cannot block
browser form validation as unfocusable required inputs. Rule enable behavior,
identity stability, masked credentials and stopped controls are preserved.

## Validation

74 frontend tests, strict TypeScript/Vite production build, Prettier, cargo fmt,
workspace all-target Clippy -D warnings, 620 Rust tests and native release build pass.
Actual runner logs retained. Tests cover closed basic fields, closed saves with
defaults/legacy custom values, queue omission, edit persistence, keyboard behavior,
invalid hidden policy and rule editor behavior; regression first failed before fix.

## Real browser

Independent temporary Broker/synthetic MQTT publisher/local HTTP receiver and
isolated native headless Chrome via CDP; existing user service/profile untouched.
Created a Webhook without opening advanced settings and confirmed default policy.
Emulated an existing custom configuration with capacity777 and non-default policy
through the authenticated QA API, then changed only name in the collapsed UI; all
policy values, disabled flag, capacity and identity remained unchanged.

Keyboard Enter/Space expands/collapses without submission. All five tuning inputs
appear, never capacity. Changed batch count to0, collapsed, then saved: no config
change, automatic expansion and visible focus on invalid input. Fixed to9, enabled
the outbound and saved collapsed: changes persisted, capacity777 stayed unchanged.
Created a rule with its normal enable switch and observed actual MQTT->SQL->HTTP
delivery. Mobile390px has no horizontal overflow and advanced defaults closed;
actual desktop/mobile screenshots and browser-checks.json retained. No captured
console errors or CSP violations; no new dependency or CSP permission.

The initial CDP startup was polled until DevToolsActivePort appeared. A keyboard
harness attempt omitted Enter's text event; supplying Chrome's real Enter character
fixed the harness and the complete scenario passed without app changes.

QA services/browser stopped. Screenshots show synthetic QA data, not production
metrics; no server deployment or new throughput/latency claim.

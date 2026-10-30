# Component dropdown verification — 2026-10-30

Replaced active native selects with the existing Radix UI DropdownMenu single-choice
component, styled with Tailwind. No dependency or CSP relaxation. Menu portals stay
inside the nearest native modal dialog; otherwise they use the document body.
The native dialog respects prevented keyboard events so the first Escape closes
the menu and a subsequent Escape closes the editor. The Escape regression failed
before the fix (escape-red.log). MQTT version filtering uses the same component.

Validation: 54 frontend tests, TypeScript/Vite production build, formatting and
workspace Clippy all targets with -D warnings, 620 Rust tests, Release build pass.

Real isolated native headless Chrome on local Darwin arm64 tested under the actual
Broker CSP. Pointer hit-testing confirmed the menu is visible in the dialog top
layer, below and equal-width to the trigger; no native select remains in the view.
Keyboard open/Enter/first Escape/subsequent Escape and outside-click dismissal pass.
Mobile viewport/document width390 has no overflow. MQTT 3.1.1 and empty/all-protocol
selection pass. No console errors or CSP violations recorded in browser-checks.json.
An initial browser harness used a button selector for an anchor navigation control;
correcting the test locator allowed the complete run to pass without app changes.

Screenshots show the actual component using temporary QA configuration. Existing
user Broker/browser profiles are untouched; temporary QA processes are stopped.

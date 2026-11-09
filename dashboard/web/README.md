# lyrnormq Dashboard (React)

Production React/TypeScript frontend for broker observation and rule management,
following `docs/contracts/phase4f-dashboard.md` and
`docs/contracts/phase5a-rules-webhook.md`. The sibling `dashboard/design` remains the
original visual prototype; this application contains no demo broker data.

## Build and serve

The React production bundle is embedded in the Rust executable at compile time
using `include_dir`. Build the frontend **before** compiling lyrnormq. From the
repository root, the combined build wrapper runs `npm ci`, `npm run build`, then
`cargo build --release -p lyrnormq`:

```sh
./scripts/build-lyrnormq.sh
```

The wrapper forwards additional Cargo flags, for example
`./scripts/build-lyrnormq.sh --target x86_64-unknown-linux-musl`. It uses Cargo's normal
toolchain; configure a cross-linker separately when cross-compiling.

To run the frontend checks or build manually:

```sh
(cd dashboard/web && npm ci && npm test && npm run build)
cargo build --release -p lyrnormq
```

Frontend source changes require another `npm run build` before Cargo compilation;
Cargo tracks the resulting dist tree and re-embeds it. Missing or invalid build
assets fail compilation rather than producing an empty Dashboard. The Rust build
never runs npm or fetches frontend dependencies automatically.

Run the compiled executable using its management options:

```sh
./target/release/lyrnormq \
  --listen 127.0.0.1:1883 \
  --admin-listen 127.0.0.1:8080 \
  --admin-token-file /path/to/admin-token
```

The executable serves its embedded Dashboard from any working directory. Runtime
hosts need neither Node.js nor a separate `dist` directory. Management remains
opt-in; without the management options, no Dashboard HTTP listener is enabled.

For an intentional external asset override, add
`--dashboard-dir /absolute/path/to/dist`. This replaces the **entire** embedded
bundle and uses the bounded asset and sensitive-file checks. An invalid override
fails startup; there is no embedded fallback and no implicit lookup of a dist
directory relative to the working directory. Rebuild the frontend and executable
for the normal embedded deployment path.

The token file contains a 32–256 character administrator token. Visit the actual
management listener address and enter the token in the login form. Do not pass
it in a URL. This HTTP listener is restricted to loopback; access remotely through
an SSH tunnel or authenticated TLS reverse proxy. The default Vite development
server has no mock API or proxy configured; use the built assets served by lyrnormq
for same-origin API requests.

## Data and security

- The token lives only in React memory. Logout or HTTP 401 unmounts the console,
  aborts requests and discards cached data; refreshing the page requires login.
- Active-page data polls every five seconds while the document is visible. No
  overlapping poll is started, and route/query changes cancel older requests.
- Rates use real cumulative PUBLISH frame counters and sample elapsed time, include
  retransmissions, and require two successful samples. History is bounded to sixty
  samples and clears on detected counter/uptime/time resets. Unsafe JavaScript
  integer counters are not used to compute rates.
- The UI shows explicit loading, empty, unavailable and stale-data states. Browser
  strings are rendered through React escaping. All resources are local and the
  production bundle requires no inline script/style CSP exceptions.
- Session protocol `unknown` and Receive Maximum `null` (restored before reconnect)
  appear as “未协商”; the UI does not invent negotiated metadata.
- CPU, RSS, latency and event history are not collected in this phase. Session
  mutations, user/ACL editing and certificate reload are unavailable. Logical state
  bytes are explicitly distinguished from process RSS. Retained payload contents
  are never requested or displayed.

Unit/integration tests cover API authorization/error handling, pagination, UTF-8
search bounds, real-rate calculation/reset/history bounds, polling cancellation and
backpressure, memory-only login/logout/401, restored negotiation labels, and XSS
escaping. Live browser integration and broker blackbox tests are run by the
coordinator after front/back-end integration.

## UI stack and component provenance

The live interface uses Tailwind CSS v4 through `@tailwindcss/vite` and genuine
shadcn/ui source components, generated from the official `ui.shadcn.com` registry
using the shadcn CLI **3.8.5** (`new-york` configuration in `components.json`).
Button, Card, Input, Badge, Table and Native Select live under `src/components/ui`.
The generated imports were adjusted to the project's `@/lib/utils` alias; their
variant/accessibility primitives are retained. Icons come from `lucide-react`.
The CLI itself is not a runtime or permanent development dependency.

Layout, spacing, responsive behavior and state colors use Tailwind utilities.
The previous 2,191-line handcrafted stylesheet was removed; `src/style.css` is
61 lines containing theme tokens, base styles and the native dialog backdrop.
The compiled Tailwind stylesheet also includes library utilities; its generated
size is distinct from manually maintained CSS.

Details use a browser-native `<dialog>` rendered as a side panel, rather than
claiming to be shadcn Sheet. This preserves the management server's strict
`script-src 'self'; style-src 'self'` policy: Radix modal scroll-lock/focus-guard
and Tabs roving-focus helpers introduce runtime styles. The view filters use
shadcn Buttons with `aria-pressed`; no Radix Tabs or style-injection exception is
required. Native modal focus, Escape handling, focus restoration and absence of
injected styles have regression tests; real-browser validation runs on integration.

Official references: [Tailwind Vite integration](https://tailwindcss.com/docs/installation/using-vite)
and [shadcn Vite installation](https://ui.shadcn.com/docs/installation/vite).

## SQL rules and Webhook outbound

When the broker starts with `--rules-config PATH`, the Rules page manages three
separate objects: SQL rules, Webhook connectors and outbound actions. The existing
broker/session views remain read-only; only explicit rule configuration saves use
authenticated JSON PUT, and SQL sample execution uses POST without outbound side
effects. The rule APIs are `/api/v1/rules/config`, `/status`, and `/test`.

The page displays actual runtime counters and labels the mode **best effort**:
MQTT acknowledgement does not await HTTP; process-local queued events/counters can
be lost on crash, and capacity overflow drops/counters the affected copy. Successful
configuration saves persist and apply a new runtime version; old queued work may be
cancelled and counted. This is not a durable action outbox or an exactly-once claim.

Connector header values use password fields. Public `[redacted]` values are sent
back unchanged to preserve existing credentials; new values remain only in component
memory and disappear after save/close/logout. URL userinfo/query/fragment and unsafe
system headers are rejected. SQL sample inputs/results also remain local to the
open editor and are not persisted in browser storage or sent to a Webhook.

Forms validate identities, UTF-8 byte limits, references and resource limits before
submitting. API validation remains authoritative. Status polls every five seconds
only while the page is visible, without overlapping requests; route/logout aborts
inflight requests and discards editor state. Refresh failures explicitly mark stale
statistics. No mock data or automatic configuration mutation is used.

A runtime status with `running: false` displays a stopped/fail-closed warning and
disables configuration writes and SQL execution while leaving published config
inspectable. Save failures are described as unconfirmed (not unchanged): a config
rename may already have published new settings before a later storage sync failure.
The operator must refresh to inspect the published config and repair/restart the
broker before rule execution resumes; ordinary MQTT remains independent.

Webhook receivers should deduplicate by `delivery_id` (source event + rule + action),
which stays stable across retries. `event_id` identifies the source event and is
shared by intentional rule/action fanout, so using it alone could suppress valid
separate deliveries. The best-effort banner states this boundary explicitly.

# rmqtt Dashboard (React)

Production React/TypeScript frontend for the read-only management API frozen in
`docs/contracts/phase4f-dashboard.md`. The sibling `dashboard/design` remains the
original visual prototype; this application contains no demo broker data.

## Build and serve

```sh
cd dashboard/web
npm ci
npm test
npm run build
```

Start rmqtt from the repository root using its management options:

```sh
cargo run --release -p rmqtt -- \
  --listen 127.0.0.1:1883 \
  --admin-listen 127.0.0.1:8080 \
  --admin-token-file /path/to/admin-token \
  --dashboard-dir dashboard/web/dist
```

The token file contains a 32–256 character administrator token. Visit the actual
management listener address and enter the token in the login form. Do not pass
it in a URL. This HTTP listener is restricted to loopback; access remotely through
an SSH tunnel or authenticated TLS reverse proxy. The default Vite development
server has no mock API or proxy configured; use the built assets served by rmqtt
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
- CPU, RSS, latency and event history are not collected in this phase. Rule engine,
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

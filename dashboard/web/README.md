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

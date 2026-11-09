# Phase4d — MQTT over WS/WSS

Frozen before implementation, 2026-10-30. Extends Phase4c transport, preserving
Phase4/4b session, QoS, outbox, storage schema, authentication, expiry and Will.
No cluster, shared multiple listeners, HTTP administration, Origin ACL, proxy
identity headers, compression, browser cookie authentication or certificate reload.

## Public API and CLI

- Keep existing `serve`, `serve_tls`, `TlsConfig` and public MQTT import paths.
- Export `WebSocketOptions { path: String, max_message_size: usize }`, Clone/Debug.
  Default path `/mqtt`, max_message_size 1 MiB. Validate before accepting clients:
  path starts `/`, length <=1024 bytes, contains no whitespace/control/`?`/`#`;
  max_message_size in 1..=16 MiB. HTTP header cap is fixed at 16 KiB.
- `serve_ws(TcpListener, BrokerConfig, WebSocketOptions, watch::Receiver<bool>)`.
- `serve_wss(TcpListener, BrokerConfig, TlsConfig, WebSocketOptions,
  watch::Receiver<bool>)`. Each function owns one existing broker instance;
  listeners do not share state or SQLite files across invocations/processes.
- CLI `--websocket` selects WS; combined with the existing complete TLS identity
  pair selects WSS. `--ws-path PATH` and `--ws-max-message-size BYTES` require
  `--websocket`. Reject duplicate WS flags/options, invalid/empty values and WS
  options with export-state. Default bind remains 127.0.0.1:1883, examples specify
  8083/8084 explicitly. No insecure WSS certificate fallback.

## HTTP upgrade and MQTT framing

- Use maintained RFC6455 implementation (tokio-tungstenite candidate, fixed in
  Cargo.lock); do not implement a new WebSocket codec. Server HTTP handshake is
  GET/HTTP1.1, version13, valid Upgrade/Connection/key. Refuse missing/wrong `mqtt`
  subprotocol; select and return exactly `mqtt` from offered token list.
- Match request URI path exactly to configured path; query does not change routing.
  Wrong path is HTTP404; invalid protocol/handshake fails without MQTT state.
  Do not negotiate permessage-deflate or trust forwarded identity headers.
- Bound HTTP request bytes **before** library parsing/callback allocation at 16KiB;
  preserve any post-header WebSocket bytes pipelined in the same TCP/TLS read.
- Client data must be masked binary frames; text, invalid RSV/opcodes/masking,
  malformed fragmentation/length/control frames close the connection. Where the
  connection is writable, bounded close uses1003 for text,1002 for protocol error,
  1009 for WS size overflow. Never send MQTT PUBLISH in a text frame.
- Reject a text data frame at its initial header even when FIN=0; do not wait for
  the complete fragmented text message. Enforce minimal extended-length encoding
  as an explicit strict-input policy (RFC6455's encoding MUST is sender-side;
  do not invent a separate MQTT receiver MUST). A finite raw-header policy guard
  is permitted where the library hides these boundaries; it must not replace the
  maintained codec's masking, message reassembly, control or encoding logic.
- Flatten binary messages into the existing incremental MQTT reader: several MQTT
  packets in one message, partial packet across independent binary messages, and
  fragmented binary messages must work. Control Ping/Pong/Close can interleave
  with fragments. Empty binary messages are valid and do not mean EOF.
- MQTT packet-size bound still applies to decoded MQTT bytes. The separate WS
  message/frame bound applies to aggregated binary message payload including all
  fragments. Outgoing chunks must fit the WS message bound without truncation.
- WebSocket Ping gets the corresponding Pong; do not echo unsolicited Pong.
  WS control traffic does not count as MQTT activity or extend MQTT Keep Alive.
  No unbounded control queues or polling busy loop on repeated empty/control input.

## IO, limits and lifecycle

- Reuse the current MQTT network loop, SessionEngine, TLS identity and outbox;
  no parallel MQTT implementation. Plain TCP retains owned halves and static IO.
- Acquire max_connections permit before TLS/HTTP handshakes; release on all failures.
  TLS, HTTP upgrade and MQTT CONNECT have independent handshake_timeout deadlines.
  Shutdown cancels pending handshakes; no session/takeover/Will before MQTT CONNECT.
- Read/write buffers and any bridge/queues must have explicit byte/item limits.
  Write buffer must not use the WebSocket library's unlimited default. No locks
  held across await and no unsafe/unwrap/expect in production code.
- MQTT `write_all` + `flush` must continue to mean bounded progress to the actual
  WS/TLS socket, not merely enqueue into a detached bridge. WS control response,
  Close and shutdown writes are bounded by write_timeout too. Never introduce
  a detached pump whose blocked write can survive connection cleanup.
- Generalize the internal graceful-close policy for TLS/WS/WSS: normal MQTT
  DISCONNECT completes bounded WS Close1000 and WSS TLS shutdown, without draining
  queued business output. Keep terminal MQTT frame ordering and plaintext behavior.
- Complete the peer WS Close exchange before releasing a normally closed socket;
  WSS also completes bounded reciprocal TLS close_notify. Keep the closing state
  inside the existing write deadline, discard post-close application input, and
  never retain an unbounded drain task. Missing peer replies may time out/abort;
  cooperative peers must not receive a reset from unread closing records.
- Full reciprocal closing is selected only after a valid MQTT DISCONNECT has
  completed successfully. A private per-WS-stream normal-disconnect signal may
  carry that decision from the MQTT loop to the IO adapter; TCP/TLS allocate no
  such signal. Peer WS Close without MQTT DISCONNECT, MQTT timeout and protocol
  failures remain abnormal: flush the bounded closing response/alert when possible
  but do not wait for peer EOF before generation cleanup, Will or Session Expiry.
  This exception does not relax strict normal MQTT/WSS close tests.
- WS Close (even1000), TLS close_notify, TCP drop or WS protocol failure without
  MQTT DISCONNECT use existing ungraceful cleanup and Will/Will Delay semantics.
  Preserve durable inflight stages/identifiers, offline queues and session resume.

## Acceptance, isolation and scope

Transport Agent owns crates/mqtt-transport/** in its assigned independent worktree;
Quality Agent owns tests/** and CI in another; coordinator owns bin/lyrnormq, root
lockfile, docs, contracts and integration. No agent edits shared Cargo.lock or
another module. lib.rs contains documentation, declarations and re-exports only.

Independent raw Python RFC6455/ssl client tests, with byte fixtures/oracles, cover:
WS/WSS × MQTT3.1.1/5 × QoS0/1/2; retained; binary coalescing/splitting/fragmentation;
Ping/Pong; text/unmasked/RSV/length/size errors; path/subprotocol; bounded header,
slow upgrade/connection caps/shutdown; verified WSS CA/hostname; normal/abrupt/WS
Close Will; persistent offline/inflight recovery and process crash with SQLite.
At least one standard MQTT client interoperability check should run when the SDK
is available, with dependencies/environment and actual result recorded.

Run fmt, strict workspace all-target Clippy, workspace tests, TLS regression and
relevant existing plaintext dual-storage regressions. Add failing reproducer before
fixing confirmed bugs; preserve failures. Update only applicable WebSocket matrix
rows using actual source review/evidence; do not widen full conformance claims.
No deployment, capacity claims or server load test in this phase.

Sources: [MQTT5 section6](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)
(MQTT-6.0.0-1..4), [MQTT3.1.1 section6](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html),
[RFC6455](https://www.rfc-editor.org/rfc/rfc6455.html), and maintained library
documentation for finite buffers and Stream/Sink semantics.

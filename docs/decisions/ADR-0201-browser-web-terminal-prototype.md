# ADR-0201: Browser Web Terminal Prototype

- Status: Accepted
- Date: 2026-06-19
- Decision Origin: Design

## Context

Telnet remains the primary GoCluster user interface, but it limits the future
UI to fixed-width terminal text. The first browser milestone needs to prove the
transport path without changing GoCluster's runtime telnet server or inventing
a new command/API contract.

Cloudflare Workers can serve static assets, accept browser WebSocket
connections, and open outbound TCP sockets with `connect()`. Workers cannot
accept inbound raw TCP, and outbound TCP cannot target localhost, private IP
ranges, Cloudflare IP ranges, or the initiating Worker. Browser clients also
cannot open raw TCP directly. The browser-facing side therefore needs to be
HTTPS/WebSocket, while the Worker dials the existing telnet listener.

The shipped telnet config uses a minimal IAC handshake and server-side echo.
Xterm.js is not a telnet protocol implementation, so raw telnet negotiation
bytes must not be rendered to the terminal. At the same time, the Worker should
avoid text-decoding the TCP stream because telnet negotiation bytes are binary.

## Decision

Add `cloudflare/web-terminal/` as a separate Cloudflare Worker/static-assets
bundle for a Phase 0 browser terminal prototype.

The bundle:

- serves a static Xterm.js terminal page;
- exposes `GET /healthz` for a simple Worker health check;
- exposes `GET /ws` as the browser WebSocket endpoint;
- opens one outbound TCP socket per browser WebSocket using Cloudflare
  `connect()`;
- takes `CLUSTER_HOST`, `CLUSTER_PORT`, `UPSTREAM_TLS`,
  `ALLOWED_ORIGINS`, and `WS_MAX_MESSAGE_BYTES` from Worker variables;
- fails closed when `CLUSTER_HOST` is not configured or the WebSocket `Origin`
  is not allowlisted;
- does not let the browser choose the upstream host or port;
- forwards Worker-to-browser traffic as binary WebSocket messages;
- strips telnet IAC negotiation bytes in the browser before writing text to
  Xterm.js;
- avoids automatic reconnect in the first prototype to prevent reconnect
  storms during upstream outages;
- does not change GoCluster runtime code, telnet commands, filters, config, or
  server-side admission behavior.

## Alternatives considered

1. Add a first-party GoCluster WebSocket API immediately.
   - Deferred. This is the better long-term architecture, but it would combine
     transport proof, protocol design, parser/event schema, auth, and UI work.
2. Build a Worker-side semantic parser before proving the terminal bridge.
   - Rejected for this slice. A parser belongs after the raw browser terminal
     path is proven against the live telnet stream.
3. Use a blind text-decoding Worker proxy.
   - Rejected because telnet IAC negotiation bytes are binary and can be
     corrupted or rendered incorrectly if decoded as text in the Worker.
4. Add automatic reconnect in the browser.
   - Rejected for the prototype because a cluster outage could produce
     synchronized reconnect churn.

## Consequences

### Benefits

- Users can reach the existing telnet UX from a browser-hosted static page.
- The prototype proves Cloudflare WebSocket-to-TCP transport without touching
  GoCluster's hot telnet path.
- The upstream target remains server-controlled through Worker variables.
- The raw terminal remains a fallback while richer structured UI work is
  deferred.

### Risks

- GoCluster sees Cloudflare Worker egress addresses instead of browser user IPs.
  Cluster-side per-IP admission, last-login IP, and connection logs are not
  end-user accurate for browser sessions.
- Plain upstream TCP remains unencrypted unless the telnet listener is exposed
  with TLS and `UPSTREAM_TLS=on`.
- Worker open-connection limits and Cloudflare plan limits bound concurrent
  browser sessions outside GoCluster's own limiter.
- Xterm.js gives terminal rendering, not semantic spot tables or richer DX
  workflows.

### Operational impact

- Deploying the browser terminal is a separate Cloudflare Worker/static-assets
  deployment. Git changes do not deploy it automatically.
- `www.n2wq.com` must route through Cloudflare to this Worker or be attached as
  a custom domain.
- `CLUSTER_HOST` must be a public non-Cloudflare, non-private hostname for the
  GoCluster telnet listener.
- Browser-session abuse controls need Cloudflare edge controls in addition to
  GoCluster's existing telnet admission gates because GoCluster sees Worker
  egress addresses.

## Links

- Related issues/PRs/commits: none
- Related tests: `cloudflare/web-terminal/test/worker.test.js`
- Related docs: `cloudflare/web-terminal/README.md`, `README.md`,
  `docs/OPERATOR_GUIDE.md`, `customgpt/source-map.md`,
  `customgpt/operator-guide-index.md`, `customgpt/troubleshooting-index.md`
- Related TSRs: none
- Supersedes / superseded by: none

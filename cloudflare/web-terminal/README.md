# GoCluster Web Terminal

This Worker bundle is the Phase 0 browser terminal prototype. It serves a
static Xterm.js page and exposes `/ws`, a WebSocket endpoint that opens one
outbound TCP socket to the existing GoCluster telnet listener.

The prototype intentionally preserves the existing telnet user experience. It
does not parse spots, add a new command API, or change GoCluster runtime code.

## Contract

- Static app: `GET /`
- Health check: `GET /healthz`
- Terminal bridge: `GET /ws` with `Upgrade: websocket`
- Browser side: WebSocket plus Xterm.js
- Worker side: outbound TCP using Cloudflare `connect()`
- Upstream target: fixed by Worker environment variables only

The Worker sends TCP chunks to the browser as binary WebSocket messages. The
browser strips telnet IAC negotiation bytes before writing text to Xterm.js.
This keeps GoCluster's telnet handshake from rendering as stray terminal
characters while avoiding Worker-side text decoding of the TCP stream.

## Configuration

Set `CLUSTER_HOST` as a Worker variable before deployment. It must be the
public origin hostname for the GoCluster telnet listener. The checked-in
`wrangler.toml` carries the non-secret defaults for the other variables:

```text
CLUSTER_PORT=8300
UPSTREAM_TLS=off
ALLOWED_ORIGINS=https://www.n2wq.com
WS_MAX_MESSAGE_BYTES=512
```

`CLUSTER_HOST` must resolve to a public non-Cloudflare, non-private address.
Cloudflare Workers outbound TCP sockets cannot connect to `localhost`, private
network IPs, Cloudflare IP ranges, or back to the same Worker. Do not let the
browser supply the target host or port.

`UPSTREAM_TLS=off` matches the current plain telnet deployment shape. If the
cluster listener is later exposed with TLS, set `UPSTREAM_TLS=on` and update the
origin listener accordingly.

`ALLOWED_ORIGINS` is fail-closed. Include `http://localhost:8787` for local
Wrangler testing and `https://www.n2wq.com` for production.

## Development

```text
npm install
npm run build
npm test
npx wrangler dev
```

`npm run build` vendors the pinned Xterm.js browser assets into
`public/vendor/` and verifies that required static and Worker files exist.

## Deployment

1. Confirm `www.n2wq.com` is routed through Cloudflare.
2. Configure Worker variables for the production origin and allowed origin.
3. Deploy this bundle with Wrangler.
4. Route `www.n2wq.com/*` to this Worker or attach it as a custom domain.
5. Open `https://www.n2wq.com/`.
6. Confirm the terminal connects, shows the normal login prompt, accepts a
   callsign, receives live output, sends commands, and closes cleanly.

## Prototype Limitations

- GoCluster sees the Cloudflare Worker egress address, not the browser user's
  real IP. Cluster-side per-IP admission, login logs, and last-login IP are not
  end-user accurate for browser sessions.
- The bridge has no automatic reconnect loop. Users reconnect manually to avoid
  reconnect storms during cluster or network outages.
- This is not yet the richer structured UI. It is a browser-hosted terminal
  bridge proving the transport path.
- Cloudflare deployment state is separate from Git state. Merging or pushing
  this directory does not deploy the Worker.

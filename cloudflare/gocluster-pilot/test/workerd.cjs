// Run with the Miniflare 5 runtime used by Wrangler and ws 8 on NODE_PATH.
// This exercises the real Worker WebSocket splice against a loopback origin.
const assert = require("node:assert/strict");
const http = require("node:http");
const path = require("node:path");
const { once } = require("node:events");
const test = require("node:test");
const { Miniflare, convertV4MiniflareOptions } = require("miniflare");
const { WebSocketServer } = require("ws");

test("splices the private upgrade, text frames and normal close", { timeout: 15000 }, async (t) => {
  const host = "gocluster-pilot.n2wq-api.workers.dev";
  const ip = "2001:db8::42";
  const origin = http.createServer();
  const sockets = new WebSocketServer({ noServer: true });
  let received;
  origin.on("upgrade", (req, socket, head) => {
    received = req;
    sockets.handleUpgrade(req, socket, head, (ws) => {
      ws.on("message", (data) => ws.send(data.toString()));
      ws.send("login: ");
    });
  });
  origin.listen(0, "127.0.0.1");
  await once(origin, "listening");
  t.after(() => {
    for (const ws of sockets.clients) ws.terminate();
    sockets.close();
    origin.close();
  });
  const address = "127.0.0.1:" + origin.address().port;
  const runtime = new Miniflare(convertV4MiniflareOptions({
    modules: true,
    scriptPath: path.resolve(__dirname, "../src/worker.mjs"),
    compatibilityDate: "2026-11-23",
    cf: false,
    bindings: { CLUSTER_EDGE_SECRET: "fixture-only" },
    serviceBindings: { CLUSTER_ORIGIN: { external: { address, http: {} } } },
  }));
  t.after(() => runtime.dispose());
  const response = await runtime.dispatchFetch(`https://${host}/ws?target=ignored`, {
    headers: {
      Upgrade: "websocket", Origin: `https://${host}`, "CF-Connecting-IP": ip,
      Cookie: "fixture=private", "X-GoCluster-Visitor-IP": "127.0.0.1",
    },
  });
  assert.equal(response.status, 101);
  assert.equal(received.url, "/ws");
  assert.equal(received.headers.host, host);
  assert.equal(received.headers.origin, `https://${host}`);
  assert.equal(received.headers["cf-connecting-ip"], ip);
  assert.equal(received.headers["x-gocluster-visitor-ip"], ip);
  assert.equal(received.headers["x-gocluster-edge-secret"], "fixture-only");
  assert.equal(received.headers.cookie, undefined);
  const ws = response.webSocket;
  const frames = [];
  ws.addEventListener("message", ({ data }) => {
    frames.push(data);
    if (frames.length === 1) ws.send("N0CALL-2");
    else ws.close(1000, "done");
  });
  const closed = once(ws, "close");
  ws.accept();
  const [event] = await closed;
  assert.equal(event.code, 1000);
  assert.deepEqual(frames, ["login: ", "N0CALL-2"]);
});

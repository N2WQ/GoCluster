import assert from "node:assert/strict";
import test from "node:test";
import worker from "../src/worker.mjs";

const origin = "https://gocluster-pilot.n2wq-api.workers.dev";
const ip = "2001:db8::42";
const headers = { Upgrade: "websocket", Origin: origin, "CF-Connecting-IP": ip };
const request = (path = "/ws", init = {}) => new Request(origin + path, { headers, ...init });

test("rejects malformed admission before consulting the private binding", async () => {
  let forwarded = 0;
  const env = { CLUSTER_EDGE_SECRET: "fixture", CLUSTER_ORIGIN: { fetch() { forwarded++; } } };
  for (const [req, status] of [
    [new Request("https://example.org/ws", { headers }), 404],
    [new Request(origin.replace("https:", "http:") + "/ws", { headers }), 404],
    [request("/ws", { method: "POST" }), 405],
    [request("/ws", { headers: { ...headers, Upgrade: "h2c" } }), 400],
    [request("/ws", { headers: { ...headers, Origin: "https://example.org" } }), 403],
    [request("/ws", { headers: { ...headers, "CF-Connecting-IP": "192.0.2.1, 192.0.2.2" } }), 403],
    [request("/ws", { headers: { Upgrade: "websocket", Origin: origin, "X-Forwarded-For": ip } }), 403],
    [request("/other"), 400],
  ]) {
    assert.equal((await worker.fetch(req, env)).status, status);
  }
  assert.equal(forwarded, 0);
  assert.equal((await worker.fetch(request(), { CLUSTER_ORIGIN: env.CLUSTER_ORIGIN })).status, 503);
  assert.equal((await worker.fetch(request(), { CLUSTER_EDGE_SECRET: "fixture" })).status, 503);
  assert.equal(forwarded, 0);
});

test("fixes destination and forwards only the approved edge attributes", async () => {
  let seen;
  const env = {
    CLUSTER_EDGE_SECRET: "private-fixture",
    CLUSTER_ORIGIN: { fetch(url, init) { seen = { url, init }; return Response.redirect("https://example.org"); } },
  };
  const req = request("/ws?target=https://example.org", { headers: {
    ...headers, Cookie: "private", Authorization: "private",
    "Cf-Access-Jwt-Assertion": "private", "Cf-Access-Authenticated-User-Email": "private",
    "X-Forwarded-For": "127.0.0.1", "X-GoCluster-Visitor-IP": "127.0.0.1",
    "X-GoCluster-Edge-Secret": "caller-supplied", "Sec-WebSocket-Protocol": "private",
  } });
  const response = await worker.fetch(req, env);
  assert.equal(seen.url, origin.replace("https:", "http:") + "/ws");
  assert.equal(seen.init.redirect, "manual");
  assert.equal(seen.init.method, "GET");
  assert.deepEqual(seen.init.headers, {
    Upgrade: "websocket", Origin: origin, "CF-Connecting-IP": ip,
    "X-GoCluster-Visitor-IP": ip, "X-GoCluster-Edge-Secret": "private-fixture",
  });
  assert.equal(response.status, 502);
  assert.equal(response.headers.get("Location"), null);
});

test("sanitizes origin refusal and failure responses", async () => {
  for (const [upstream, expected] of [[400,400], [403,403], [429,429], [503,503], [500,502], [200,502]]) {
    const env = {
      CLUSTER_EDGE_SECRET: "fixture",
      CLUSTER_ORIGIN: { fetch: () => new Response("private", { status: upstream, headers: { "Set-Cookie": "private" } }) },
    };
    const response = await worker.fetch(request(), env);
    assert.equal(response.status, expected);
    assert.equal(await response.text(), "");
    assert.equal(response.headers.get("Set-Cookie"), null);
    assert.equal(response.headers.get("Cache-Control"), "no-store");
  }
  const env = { CLUSTER_EDGE_SECRET: "fixture", CLUSTER_ORIGIN: { fetch() { throw Error("private"); } } };
  assert.equal((await worker.fetch(request(), env)).status, 502);
});

test("serves the console root with the socket policy", async () => {
  const env = { ASSETS: { fetch(req) {
    assert.equal(req.url, origin + "/index.html");
    return new Response("console");
  } } };
  const response = await worker.fetch(request("/", { headers: {} }), env);
  assert.equal(await response.text(), "console");
  assert.ok(response.headers.get("Content-Security-Policy").includes("wss://gocluster-pilot.n2wq-api.workers.dev/ws"));
});

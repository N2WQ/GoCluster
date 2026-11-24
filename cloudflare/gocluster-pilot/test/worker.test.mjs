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

test("reports the current request's IPv4 without consulting the origin or assets", async () => {
  const env = {
    CLUSTER_ORIGIN: { fetch() { assert.fail("visitor address reached the cluster"); } },
    ASSETS: { fetch() { assert.fail("visitor address reached the assets binding"); } },
  };
  for (const [forwarded, expected] of [
    ["192.0.2.17", "192.0.2.17"], ["198.51.100.24", "198.51.100.24"],
    ["::ffff:203.0.113.80", "203.0.113.80"], ["2001:db8::42", null],
    ["192.0.2.1, 192.0.2.2", null], ["192.0.2.256", null], ["192.0.02.4", null], [null, null],
  ]) {
    const requestHeaders = { "X-Forwarded-For": "203.0.113.1", "X-GoCluster-Visitor-IP": "203.0.113.2" };
    if (forwarded !== null) requestHeaders["CF-Connecting-IP"] = forwarded;
    const response = await worker.fetch(request("/visitor-ip", { headers: requestHeaders }), env);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("Cache-Control"), "no-store");
    assert.equal(response.headers.get("Access-Control-Allow-Origin"), null);
    assert.deepEqual(await response.json(), { ipv4: expected });
  }
  const post = await worker.fetch(request("/visitor-ip", { method: "POST", headers: {} }), env);
  assert.equal(post.status, 405);
  assert.equal(post.headers.get("Allow"), "GET");
  assert.equal((await worker.fetch(request("/visitor-ip"), env)).status, 400);
});

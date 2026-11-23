const pilotHost = "gocluster-pilot.n2wq-api.workers.dev";
const pilotOrigin = `https://${pilotHost}`;
const policy = `default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data:; connect-src 'self' wss://${pilotHost}/ws; manifest-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`;

// This binding fixes the destination to one loopback service on the tunnel.
// The URL supplies the HTTP Host seen by GoCluster. Browser headers, query
// strings and redirected requests must never steer the authenticated request.
async function upgrade(request, env, headers) {
  if (request.method !== "GET") {
    return new Response(null, { status: 405, headers: { ...headers, Allow: "GET" } });
  }
  if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
    return new Response(null, { status: 400, headers });
  }
  if (request.headers.get("Origin") !== pilotOrigin) {
    return new Response(null, { status: 403, headers });
  }
  // Cloudflare supplies this address at ingress. GoCluster strictly parses
  // it before admission. Never infer it from client-provided forwarding lists.
  const ip = request.headers.get("CF-Connecting-IP");
  if (!ip || !/^[0-9a-fA-F:.]{2,45}$/.test(ip)) {
    return new Response(null, { status: 403, headers });
  }
  if (!env.CLUSTER_ORIGIN || !env.CLUSTER_EDGE_SECRET) {
    return new Response(null, { status: 503, headers });
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10000);
  try {
    const upstream = await env.CLUSTER_ORIGIN.fetch(`http://${pilotHost}/ws`, {
      method: "GET",
      redirect: "manual",
      signal: controller.signal,
      headers: {
        Upgrade: "websocket",
        Origin: pilotOrigin,
        "CF-Connecting-IP": ip,
        // The origin checks equality to detect any VPC rewrite of the CF header.
        "X-GoCluster-Visitor-IP": ip,
        "X-GoCluster-Edge-Secret": env.CLUSTER_EDGE_SECRET,
      },
    });
    if (upstream.status === 101 && upstream.webSocket) {
      // Let the runtime splice the upgraded connection. JS never receives
      // spot frames or owns a second queue, heartbeat or close handshake.
      return new Response(null, { status: 101, webSocket: upstream.webSocket });
    }
    // The public side receives no private error body, cookies, or redirect.
    await upstream.body?.cancel();
    const status = [400, 403, 429, 503].includes(upstream.status) ? upstream.status : 502;
    return new Response(null, { status, headers });
  } catch {
    return new Response(null, { status: 502, headers });
  } finally {
    clearTimeout(timer);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const headers = {
      "Cache-Control": "no-store",
      "Content-Security-Policy": policy,
      "X-Content-Type-Options": "nosniff",
    };
    if (url.origin !== pilotOrigin) return new Response(null, { status: 404, headers });
    if (url.pathname === "/ws") return upgrade(request, env, headers);
    if (request.headers.has("Upgrade")) return new Response(null, { status: 400, headers });
    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response(null, { status: 405, headers: { ...headers, Allow: "GET, HEAD" } });
    }
    if (url.pathname === "/") url.pathname = "/index.html";
    const asset = await env.ASSETS.fetch(new Request(url, request));
    const response = new Response(asset.body, asset);
    for (const [name, value] of Object.entries(headers)) response.headers.set(name, value);
    return response;
  },
};

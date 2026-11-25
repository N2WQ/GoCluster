const consoleOrigin = "https://ui.n2wq.com";
const clusterHost = "gocluster-pilot.n2wq-api.workers.dev";
const clusterOrigin = `https://${clusterHost}`;
const legacyOrigin = "https://gocluster-pilot.n2wq-api.workers.dev";

function visitorIPv4(request) {
  const ip = request.headers.get("CF-Connecting-IP")?.replace(/^::ffff:/i, "");
  if (!ip || !/^(?:0|[1-9]\d{0,2})(?:\.(?:0|[1-9]\d{0,2})){3}$/.test(ip)) return null;
  return ip.split(".").every(octet => Number(octet) <= 255) ? ip : null;
}

// This binding fixes the destination to one loopback service on the tunnel.
// GoCluster checks the fixed Host and Origin on this private hop. The browser
// origin is validated at the Worker and never controls those upstream headers.
async function upgrade(request, env, headers, browserOrigin) {
  if (request.method !== "GET") {
    return new Response(null, { status: 405, headers: { ...headers, Allow: "GET" } });
  }
  if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
    return new Response(null, { status: 400, headers });
  }
  if (request.headers.get("Origin") !== browserOrigin) {
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
    const upstream = await env.CLUSTER_ORIGIN.fetch(`http://${clusterHost}/ws`, {
      method: "GET",
      redirect: "manual",
      signal: controller.signal,
      headers: {
        Upgrade: "websocket",
        Origin: clusterOrigin,
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
    const allowedOrigin = url.origin === consoleOrigin || url.origin === legacyOrigin;
    const socketPolicy = allowedOrigin ? ` wss://${url.hostname}/ws` : "";
    const headers = {
      "Cache-Control": "no-store",
      "Content-Security-Policy": `default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data:; connect-src 'self'${socketPolicy}; manifest-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`,
      "X-Content-Type-Options": "nosniff",
    };
    if (!allowedOrigin) return new Response(null, { status: 404, headers });
    if (url.pathname === "/ws") return upgrade(request, env, headers, url.origin);
    if (request.headers.has("Upgrade")) return new Response(null, { status: 400, headers });
    if (url.pathname === "/visitor-ip") {
      if (request.method !== "GET") {
        return new Response(null, { status: 405, headers: { ...headers, Allow: "GET" } });
      }
      return Response.json({ ipv4: visitorIPv4(request) }, { headers });
    }
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

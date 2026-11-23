const pilotHost = "gocluster-pilot.n2wq-api.workers.dev";
const policy = `default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data:; connect-src 'self' wss://${pilotHost}/ws; manifest-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const headers = {
      "Cache-Control": "no-store",
      "Content-Security-Policy": policy,
      "X-Content-Type-Options": "nosniff",
    };
    if (url.hostname !== pilotHost) return new Response(null, { status: 404, headers });
    if (url.pathname === "/ws") return new Response(null, { status: 503, headers });
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

const pilotHost = "gocluster-pilot.n2wq-api.workers.dev";
const page = `<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GoCluster private pilot</title>
<main>
<h1>GoCluster private pilot</h1>
<p>You reached the pilot through Cloudflare Access.</p>
<p>The live cluster connection is pending.</p>
</main>
</html>`;

export default {
  fetch(request) {
    const url = new URL(request.url);
    const headers = {
      "Cache-Control": "no-store",
      "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'; base-uri 'none'",
      "X-Content-Type-Options": "nosniff",
    };
    if (url.hostname !== pilotHost || url.pathname !== "/") {
      return new Response(null, { status: 404, headers });
    }
    if (request.headers.has("Upgrade")) {
      return new Response(null, { status: 400, headers });
    }
    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response(null, { status: 405, headers: { ...headers, Allow: "GET, HEAD" } });
    }
    headers["Content-Type"] = "text/html; charset=utf-8";
    return new Response(request.method === "HEAD" ? null : page, { headers });
  },
};

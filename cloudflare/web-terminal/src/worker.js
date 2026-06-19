import { connect } from "cloudflare:sockets";
import {
  bridgeWebSocketToTcp,
  isAllowedOrigin,
  workerSettings
} from "./bridge.js";

const CLOSE_ERROR = 1011;

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/healthz") {
      return json({ status: "ok" });
    }
    if (url.pathname === "/ws") {
      return handleWebSocket(request, env, ctx);
    }
    if (env.ASSETS) {
      return env.ASSETS.fetch(request);
    }
    return new Response("Not found", { status: 404 });
  }
};

export async function handleWebSocket(request, env, ctx, deps = {}) {
  if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
    return new Response("Expected a WebSocket connection.", { status: 426 });
  }

  let settings;
  try {
    settings = workerSettings(env);
  } catch (error) {
    return new Response(`Worker configuration error: ${errorMessage(error)}`, { status: 500 });
  }

  const origin = request.headers.get("Origin") || "";
  if (!isAllowedOrigin(origin, settings.allowedOrigins)) {
    return new Response("Forbidden origin.", { status: 403 });
  }

  const pairFactory = deps.WebSocketPair || WebSocketPair;
  const pair = new pairFactory();
  const [client, server] = Object.values(pair);
  server.accept();

  let socket;
  try {
    const connectFn = deps.connect || connect;
    socket = connectFn(
      { hostname: settings.host, port: settings.port },
      { secureTransport: settings.secureTransport }
    );
    if (socket.opened) {
      await socket.opened;
    }
  } catch (error) {
    safeClose(server, CLOSE_ERROR, "TCP connect failed");
    return new Response(`TCP connect failed: ${errorMessage(error)}`, { status: 502 });
  }

  const bridge = bridgeWebSocketToTcp(server, socket, {
    maxMessageBytes: settings.maxMessageBytes
  });
  ctx.waitUntil(bridge.done);

  return new Response(null, { status: 101, webSocket: client });
}

function safeClose(ws, code, reason) {
  try {
    ws.close(code, reason);
  } catch {
    // WebSocket close is best-effort during failure propagation.
  }
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" }
  });
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

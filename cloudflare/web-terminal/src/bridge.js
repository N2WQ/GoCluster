const DEFAULT_CLUSTER_PORT = 23;
const DEFAULT_MAX_MESSAGE_BYTES = 512;
const MAX_ALLOWED_MESSAGE_BYTES = 4096;
const CLOSE_NORMAL = 1000;
const CLOSE_TOO_LARGE = 1009;
const CLOSE_ERROR = 1011;

export function workerSettings(env = {}) {
  const host = String(env.CLUSTER_HOST || "").trim();
  if (!host) {
    throw new Error("CLUSTER_HOST is required");
  }

  const port = parsePort(env.CLUSTER_PORT, DEFAULT_CLUSTER_PORT);
  const secureTransport = parseSecureTransport(env.UPSTREAM_TLS);
  const maxMessageBytes = parseMessageLimit(env.WS_MAX_MESSAGE_BYTES);
  const allowedOrigins = parseAllowedOrigins(env.ALLOWED_ORIGINS);

  return { host, port, secureTransport, maxMessageBytes, allowedOrigins };
}

export function isAllowedOrigin(origin, allowedOrigins) {
  if (!Array.isArray(allowedOrigins) || allowedOrigins.length === 0) {
    return false;
  }
  return allowedOrigins.includes(origin);
}

export function bridgeWebSocketToTcp(server, socket, options = {}) {
  const maxMessageBytes = options.maxMessageBytes || DEFAULT_MAX_MESSAGE_BYTES;
  const writer = socket.writable.getWriter();
  let closed = false;
  let writeChain = Promise.resolve();

  const closeAll = (code = CLOSE_NORMAL, reason = "closed", closeWebSocket = true) => {
    if (closed) {
      return;
    }
    closed = true;
    try {
      writer.releaseLock();
    } catch {
      // The lock can already be released after a stream failure.
    }
    try {
      socket.close();
    } catch {
      // Closing is best-effort because the peer may already be gone.
    }
    if (closeWebSocket) {
      safeClose(server, code, reason);
    }
  };

  server.addEventListener("message", (event) => {
    if (closed) {
      return;
    }
    const bytes = webSocketMessageBytes(event.data);
    if (bytes.byteLength > maxMessageBytes) {
      closeAll(CLOSE_TOO_LARGE, "message too large");
      return;
    }
    writeChain = writeChain
      .then(() => writer.write(bytes))
      .catch(() => closeAll(CLOSE_ERROR, "TCP write failed"));
  });

  server.addEventListener("close", () => closeAll(CLOSE_NORMAL, "browser closed", false));
  server.addEventListener("error", () => closeAll(CLOSE_ERROR, "browser error", false));

  const done = (async () => {
    const reader = socket.readable.getReader();
    try {
      while (!closed) {
        const result = await reader.read();
        if (result.done) {
          closeAll(CLOSE_NORMAL, "TCP closed");
          return;
        }
        if (result.value) {
          server.send(result.value);
        }
      }
    } catch {
      closeAll(CLOSE_ERROR, "TCP read failed");
    } finally {
      try {
        reader.releaseLock();
      } catch {
        // The reader may already be released after a stream failure.
      }
    }
  })();

  return { done, close: closeAll, writesDone: () => writeChain };
}

export function webSocketMessageBytes(data) {
  if (typeof data === "string") {
    return new TextEncoder().encode(data);
  }
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  return new TextEncoder().encode(String(data ?? ""));
}

function parsePort(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("CLUSTER_PORT must be an integer from 1 to 65535");
  }
  return port;
}

function parseSecureTransport(value) {
  const normalized = String(value || "off").trim().toLowerCase();
  if (normalized === "off" || normalized === "on" || normalized === "starttls") {
    return normalized;
  }
  throw new Error("UPSTREAM_TLS must be off, on, or starttls");
}

function parseMessageLimit(value) {
  if (value === undefined || value === null || value === "") {
    return DEFAULT_MAX_MESSAGE_BYTES;
  }
  const limit = Number(value);
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_ALLOWED_MESSAGE_BYTES) {
    throw new Error(`WS_MAX_MESSAGE_BYTES must be an integer from 1 to ${MAX_ALLOWED_MESSAGE_BYTES}`);
  }
  return limit;
}

function parseAllowedOrigins(value) {
  return String(value || "")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean);
}

function safeClose(ws, code, reason) {
  try {
    ws.close(code, reason);
  } catch {
    // WebSocket close is best-effort during failure propagation.
  }
}

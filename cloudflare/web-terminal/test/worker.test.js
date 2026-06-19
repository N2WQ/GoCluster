import test from "node:test";
import assert from "node:assert/strict";
import {
  bridgeWebSocketToTcp,
  isAllowedOrigin,
  webSocketMessageBytes,
  workerSettings
} from "../src/bridge.js";
import { TelnetIacFilter, webSocketUrl } from "../public/terminal.js";

test("workerSettings validates required host and bounds", () => {
  assert.throws(() => workerSettings({}), /CLUSTER_HOST/);
  assert.throws(() => workerSettings({ CLUSTER_HOST: "dx.example", CLUSTER_PORT: "0" }), /CLUSTER_PORT/);
  assert.throws(() => workerSettings({ CLUSTER_HOST: "dx.example", UPSTREAM_TLS: "maybe" }), /UPSTREAM_TLS/);
  assert.throws(() => workerSettings({ CLUSTER_HOST: "dx.example", WS_MAX_MESSAGE_BYTES: "99999" }), /WS_MAX_MESSAGE_BYTES/);

  assert.deepEqual(workerSettings({
    CLUSTER_HOST: "dx.example",
    CLUSTER_PORT: "8300",
    UPSTREAM_TLS: "off",
    ALLOWED_ORIGINS: "https://www.n2wq.com, http://localhost:8787",
    WS_MAX_MESSAGE_BYTES: "128"
  }), {
    host: "dx.example",
    port: 8300,
    secureTransport: "off",
    maxMessageBytes: 128,
    allowedOrigins: ["https://www.n2wq.com", "http://localhost:8787"]
  });
});

test("origin allowlist fails closed", () => {
  assert.equal(isAllowedOrigin("https://www.n2wq.com", ["https://www.n2wq.com"]), true);
  assert.equal(isAllowedOrigin("https://evil.example", ["https://www.n2wq.com"]), false);
  assert.equal(isAllowedOrigin("https://www.n2wq.com", []), false);
});

test("webSocketMessageBytes preserves string and binary inputs", () => {
  assert.deepEqual([...webSocketMessageBytes("A\r")], [65, 13]);
  assert.deepEqual([...webSocketMessageBytes(new Uint8Array([1, 2]).buffer)], [1, 2]);
  assert.deepEqual([...webSocketMessageBytes(new Uint8Array([3, 4]))], [3, 4]);
});

test("bridge forwards browser input to TCP and TCP chunks to browser", async () => {
  const server = new FakeWebSocket();
  const socket = fakeSocket([new Uint8Array([72, 105, 13, 10])]);
  const bridge = bridgeWebSocketToTcp(server, socket, { maxMessageBytes: 16 });

  server.dispatch("message", { data: "K1ABC\r" });
  await bridge.writesDone();
  await bridge.done;

  assert.deepEqual(socket.writes.map((chunk) => [...chunk]), [[75, 49, 65, 66, 67, 13]]);
  assert.deepEqual(server.sent.map((chunk) => [...chunk]), [[72, 105, 13, 10]]);
  assert.deepEqual(server.closes.at(-1), { code: 1000, reason: "TCP closed" });
});

test("bridge closes on oversized browser message", async () => {
  const server = new FakeWebSocket();
  const socket = fakeSocket([]);
  const bridge = bridgeWebSocketToTcp(server, socket, { maxMessageBytes: 4 });

  server.dispatch("message", { data: "12345" });
  await bridge.writesDone();

  assert.equal(socket.closed, true);
  assert.deepEqual(server.closes.at(-1), { code: 1009, reason: "message too large" });
});

test("browser close closes TCP without echoing another close", () => {
  const server = new FakeWebSocket();
  const socket = fakeSocket([]);
  bridgeWebSocketToTcp(server, socket, { maxMessageBytes: 16 });

  server.dispatch("close", {});

  assert.equal(socket.closed, true);
  assert.equal(server.closes.length, 0);
});

test("TelnetIacFilter strips option negotiation across chunks", () => {
  const filter = new TelnetIacFilter();

  const first = filter.filter(new Uint8Array([255, 251]));
  const second = filter.filter(new Uint8Array([3, 72, 105, 255]));
  const third = filter.filter(new Uint8Array([252, 1, 13, 10]));

  assert.deepEqual([...first], []);
  assert.deepEqual([...second], [72, 105]);
  assert.deepEqual([...third], [13, 10]);
});

test("TelnetIacFilter strips subnegotiation", () => {
  const filter = new TelnetIacFilter();
  const output = filter.filter(new Uint8Array([65, 255, 250, 31, 0, 80, 255, 240, 66]));

  assert.deepEqual([...output], [65, 66]);
});

test("webSocketUrl derives scheme from page location", () => {
  assert.equal(webSocketUrl({ protocol: "https:", host: "www.n2wq.com" }), "wss://www.n2wq.com/ws");
  assert.equal(webSocketUrl({ protocol: "http:", host: "localhost:8787" }), "ws://localhost:8787/ws");
});

class FakeWebSocket {
  constructor() {
    this.sent = [];
    this.closes = [];
    this.listeners = new Map();
  }

  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) || [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  send(data) {
    this.sent.push(data);
  }

  close(code, reason) {
    this.closes.push({ code, reason });
  }

  dispatch(type, event) {
    for (const listener of this.listeners.get(type) || []) {
      listener(event);
    }
  }
}

function fakeSocket(chunks) {
  const socket = {
    writes: [],
    closed: false,
    readable: new ReadableStream({
      start(controller) {
        for (const chunk of chunks) {
          controller.enqueue(chunk);
        }
        controller.close();
      }
    }),
    writable: new WritableStream({
      write(chunk) {
        socket.writes.push(new Uint8Array(chunk));
      }
    }),
    close() {
      socket.closed = true;
    }
  };
  return socket;
}

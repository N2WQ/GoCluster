import { TelnetIacFilter, webSocketUrl } from "./terminal.js";

const terminal = new Terminal({
  cursorBlink: true,
  convertEol: true,
  fontFamily: "'Cascadia Mono', Consolas, 'Liberation Mono', monospace",
  fontSize: 15,
  scrollback: 5000,
  theme: {
    background: "#101418",
    foreground: "#f2f5f8",
    cursor: "#f2f5f8",
    selectionBackground: "#315a7d"
  }
});

const terminalHost = document.getElementById("terminal");
const statusText = document.getElementById("status-text");
const reconnectButton = document.getElementById("reconnect");
const disconnectButton = document.getElementById("disconnect");
const clearButton = document.getElementById("clear");
const filter = new TelnetIacFilter();
const decoder = new TextDecoder();

let socket = null;

terminal.open(terminalHost);
terminal.focus();

terminal.onData((data) => {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(data);
  }
});

reconnectButton.addEventListener("click", () => connectTerminal());
disconnectButton.addEventListener("click", () => {
  if (socket) {
    socket.close(1000, "manual disconnect");
  }
});
clearButton.addEventListener("click", () => terminal.clear());

connectTerminal();

function connectTerminal() {
  if (socket && socket.readyState !== WebSocket.CLOSED) {
    socket.close(1000, "reconnect");
  }

  setStatus("connecting");
  const nextSocket = new WebSocket(webSocketUrl());
  nextSocket.binaryType = "arraybuffer";
  socket = nextSocket;

  nextSocket.addEventListener("open", () => {
    setStatus("connected");
    terminal.focus();
  });

  nextSocket.addEventListener("message", (event) => {
    const bytes = toBytes(event.data);
    const displayBytes = filter.filter(bytes);
    if (displayBytes.byteLength > 0) {
      terminal.write(decoder.decode(displayBytes, { stream: true }));
    }
  });

  nextSocket.addEventListener("close", () => {
    if (socket === nextSocket) {
      setStatus("closed");
    }
  });

  nextSocket.addEventListener("error", () => {
    if (socket === nextSocket) {
      setStatus("error");
    }
  });
}

function setStatus(status) {
  statusText.textContent = status;
  document.body.dataset.connection = status;
}

function toBytes(data) {
  if (typeof data === "string") {
    return new TextEncoder().encode(data);
  }
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  return new Uint8Array();
}

const IAC = 255;
const SE = 240;
const SB = 250;
const WILL = 251;
const WONT = 252;
const DO = 253;
const DONT = 254;

export class TelnetIacFilter {
  constructor() {
    this.state = "data";
  }

  filter(input) {
    const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
    const output = [];

    for (const byte of bytes) {
      switch (this.state) {
        case "data":
          if (byte === IAC) {
            this.state = "iac";
          } else {
            output.push(byte);
          }
          break;
        case "iac":
          if (byte === IAC) {
            this.state = "data";
          } else if (byte === WILL || byte === WONT || byte === DO || byte === DONT) {
            this.state = "option";
          } else if (byte === SB) {
            this.state = "subnegotiation";
          } else {
            this.state = "data";
          }
          break;
        case "option":
          this.state = "data";
          break;
        case "subnegotiation":
          if (byte === IAC) {
            this.state = "subnegotiation-iac";
          }
          break;
        case "subnegotiation-iac":
          if (byte === IAC) {
            this.state = "subnegotiation";
          } else if (byte === SE) {
            this.state = "data";
          } else {
            this.state = "subnegotiation";
          }
          break;
        default:
          this.state = "data";
          if (byte !== IAC) {
            output.push(byte);
          }
      }
    }

    return new Uint8Array(output);
  }
}

export function webSocketUrl(locationLike = globalThis.location) {
  const protocol = locationLike.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${locationLike.host}/ws`;
}

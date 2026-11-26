# ADR-0379 — Browser replacement and Telnet NOP handling

- Status: Accepted
- Date: 2026-11-26
- Decision Origin: Troubleshooting chat, approved scope v1

## Context

[TSR-0146](../troubleshooting/TSR-0146-session-eviction-retry-and-telnet-nop.md)
records a production browser replacement loop and a separate Telnet NOP
compatibility defect reproduced during the audit.

## Decision

One Telnet connection and one browser connection may share a full SSID.
A new connection replaces only the slot for its own transport. As it ends a
displaced browser session, the existing writer sends the terminal JSON v1
record `["notice","session-replaced",message]` when JSON is active. The
message carries the configured prose, which may be empty. Text framing uses
the configured prose. Both cases use the bounded control queue and the
existing write deadline.

After confirmed replacement the browser waits for manual **Reconnect**.
The browser recognizes the shipped legacy duplicate-login text for older
nodes. A closure without a recognized reason uses the existing eight retry
delays with jitter. Short-lived connections share the remaining retry budget.
Sixty continuous seconds in Live on one socket replenish the budget. Manual
reconnect starts a fresh attempt.

Both inbound TCP transports silently consume a complete Telnet NOP. The
existing wire observer translates a standalone NOP to the ignored GA command
inside the inbound Ziutek adapter. Native readers classify the completed
command before handling a pending CR companion. Wire accounting charges the
actual input bytes. Idle-read classification excludes complete NOPs while
retaining evidence of real partial input and incomplete Telnet sequences.

## Alternatives considered

A browser fix based solely on matching text would address the shipped prose
but could leave a custom notice unrecognized. A stable JSON reason and a
bounded retry budget cover both custom messages and loss of the last record.

Patching the shared Ziutek module would reach the decoder directly and would
also change RBN and peer callers. The listener adapter permits a length-
preserving change in the receiving path that needs it.

## Consequences

Operators can publish the updated browser before releasing the Go binary.
The browser can use the default legacy notice and bounded retries against an
older node. The terminal record requires a writable displaced socket and JSON
writer state. The retry budget covers failure to receive it. NOP handling
continues to respect the Telnet login, command recovery, and upload bounds.

## Links

- [ADR-0376, shared browser and Telnet profile](ADR-0376-shared-browser-telnet-profile.md)
- [Browser console](../../cloudflare/gocluster-pilot/README.md)
- [Telnet wire guide](../../telnet/README.md#json-output)
- [Same-SSID wire tests](../../telnet/shared_ssid_test.go) and [NOP tests](../../telnet/nop_test.go)

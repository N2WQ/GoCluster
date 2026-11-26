# TSR-0146 — Browser eviction loops and Telnet NOP disconnects

Status: Monitoring
Date Opened: 2026-11-26
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: Browser and Telnet session lifecycle
Trigger Source: Operator report
Led To ADR(s): ADR-0379

## RCA Summary

A burst of browser replacements and the `T440` display described different
events. A displaced JSON-mode browser treated the old replacement prose as
ordinary output and retried. The approved fix gives the browser a terminal
reason and carries its retry budget across short-lived logins. A separate
locally reproduced Telnet NOP defect required changes in the inbound reader.

## Production evidence

The November 26 Network screenshot showed `T440`, the cumulative prelogin
timeout count. In the supplied system logs for that process, all 440 timeouts
matched an accepted TCP connection by address and port. Two source addresses
accounted for 436. The matches clustered at 30 seconds.
The log ends the attempt at expiry without identifying which login stage
consumed the budget.

In the same run, one full SSID accounted for 1,198 of 1,203 duplicate-login
evictions. The burst lasted about 35 minutes, mostly one or two seconds apart.
Replacement entries coincided with WebSocket opens and closures of the old
JSON session. The node logged revision `36cba5b1`. The logs do not identify
the individual tabs or the exact browser asset each tab loaded.

## Root cause or best current explanation

The browser recognized the legacy duplicate-login prose before switching to
JSON. After the switch it displayed the wrapped text as ordinary output and
treated the socket closure as retryable. Each successful login replenished
the retry budget. Two competing browsers could therefore keep evicting each
other. The audit reproduced the behavior with the node revision's companion
browser bundle and the checkout bundle.

The audit also reproduced an immediate disconnect when Ziutek v0.1.0
received Telnet NOP. Native consumed NOP but could close on the next idle
deadline because it classified the control bytes as partial command input.
The supplied production logs had no `unknown command: 241` signature.

## Fix or mitigation

Use one browser and one logger per full SSID. A second browser takes over from
the first, which waits for manual reconnect when it receives the replacement
reason. Both inbound Telnet transports consume NOP as protocol control.

## Verification and monitoring

The browser regression exercises the terminal notice, mixed-version legacy
text, and a notice lost during repeated short-lived connections. The Go tests
check same-SSID WebSocket replacement with a live Telnet logger, and native
and Ziutek sessions across an idle deadline and line and upload boundaries.
See [ADR-0379](../decisions/ADR-0379-browser-replacement-and-telnet-nop.md)
for the decision and its verification links.

For a recurrence, retain the node revision, browser asset hash, and the
time window of `Evicted existing session` and `Registered client` entries.
Use `Login timeout for` entries to investigate a rising `Prelogin T`.
Correlate the evidence by timestamp without sharing raw source addresses
or callsigns in public reports. Production rollout and post-rollout results
are tracked separately from local verification.

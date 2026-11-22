# ADR-0368 — Opt-in CW call retention

- Status: Accepted
- Date: 2026-11-21
- Decision origin: Approved Scope Ledger v2 for the CW trial

[ADR-0369](ADR-0369-cw-call-retention-shipped-defaults.md) supersedes the
shipped-default decision below.

## Decision

Publish the existing RBN CW frequency separation and burst/WPM safeguards
with separate optional booleans. The shipped configuration sets both false.
Omission also disables each safeguard. Operators can enable either in a private
configuration and restart. The burst safeguard requires ordinary correction
enabled and selective rescue disabled. When both safeguards qualify,
frequency separation retains the original first.

Retain the timing and evidence rules in [ADR-0367](ADR-0367-cw-call-retention-on-experimental-branch.md).
The burst comparison uses local read times and a separate 60-second decay
decision. The fixed history holds up to 32,768 reports. The existing resolver
pressure summary includes occupied rows and evictions while that history is active.

## Consequences

Operators can trial either safeguard with the published executable and roll it
back through the same private configuration. Network buffering can change which
reports form an observed burst. Archived report timing and controlled runtime
checks support the [trial guide](../cw-call-correction-branch.md). Live accuracy
and capacity need measurement on live traffic.

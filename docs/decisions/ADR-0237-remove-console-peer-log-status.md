# ADR-0237: Remove Console Peer-Log Status

- Status: Accepted
- Date: 2026-10-03
- Decision Origin: Design

## Context

The owner reported that the additional `Peer log` row in the console Ingest
Sources panel had not been requested or approved. Approved console-removal
Scope Ledger v1 selects removal of that row while retaining the existing
connected-source total and connected peer callsigns, without a new peer-count
row. Historical PC18/PC92 approval records remain unchanged.

## Decision

Remove the console peer-log status row in every logging state. Preserve source
connectivity, peer callsigns and the existing peer-count fallback when callsigns
are unavailable. Do not add or relocate a status display.

Keep diagnostic ownership, the companion, logging, loss accounting and the
manager's `DiagnosticStats()` API unchanged. Supersede only ADR-0234's console
status-display contract.

## Alternatives considered

1. Keeping the row conflicts with the owner's selected console presentation.
2. Hiding only the ready state would leave the unwanted row in failure states.
3. Adding a replacement status or peer-count row exceeds the approved scope.

## Consequences

### Benefits

- The panel retains its source-connectivity and peer-callsign presentation.

### Risks

- Operators no longer see peer-log degradation, cleanup failure or loss
  counters in this panel, including when the companion or disk is unavailable.
- The retained programmatic counters are not an alternative operator display.

### Operational impact

- Keep the sibling peerdiag companion and existing logging configuration.
- Source totals count enabled ingest sources, with peering as one source;
  they are not connected-peer totals.
- No deployment or cluster restart is included in this change.

## Links

- [ADR-0234](ADR-0234-peer-owned-resources-and-exact-spot-keys.md), superseded
  only for console peer-log status display.
- [Peer operations](../../peer/README.md#diagnostics-and-persistence)
- [Ingest rendering tests](../../internal/cluster/main_test.go)
- [Source connectivity tests](../../internal/cluster/main_stats_test.go)

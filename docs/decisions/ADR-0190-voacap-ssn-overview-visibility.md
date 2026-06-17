# ADR-0190: VOACAP SSN Overview Visibility

- Status: Accepted
- Date: 2026-06-17
- Decision Origin: Scope Ledger v1

## Context

Runtime VOACAP fallback uses `SunspotMonitor` to maintain an EWMA SSN and expose
the rounded current SSN generation used for VOACAP forecast cache keys and deck
generation. ADR-0182 made that state durable across restarts, but the
interactive Overview page did not show the current value. Operators had to infer
VOACAP SSN readiness from diagnostics such as `vssn`, startup logs, or
`SET DIAG PATH` output after a cache hit.

The existing Caches & Data Freshness footer already shows adjacent runtime data
freshness for CTY, FCC, and skew. That line is the right compact surface for a
single VOACAP SSN generation value.

## Decision

Add `VOACAP SSN: <integer|n/a>` to the same Overview line as CTY, FCC, and
skew.

The displayed integer is the value returned by the existing VOACAP SSN provider
`CurrentSSN(now)`. That is the rounded current SSN generation used by VOACAP
forecast cache keys and deck generation. If the provider is absent or has no
initialized generation, display `n/a`.

Do not display the raw decimal EWMA on the Overview page. Do not change NOAA
fetching, EWMA update math, recompute thresholds, VOACAP cache keys, deck
generation, fallback worker behavior, or propagation-log counters.

## Alternatives considered

1. Show raw EWMA as a decimal value.
   - Rejected because VOACAP cache keys and decks use the rounded current
     generation, not the decimal EWMA.
2. Add a separate VOACAP status row.
   - Rejected because the requested operator signal is a compact value and the
     Caches & Data Freshness footer already owns neighboring freshness data.
3. Show the value only in logs or `SET DIAG PATH`.
   - Rejected because the goal is routine console visibility before a specific
     path diagnostic is requested.

## Consequences

### Benefits

- Operators can see the current VOACAP SSN generation on the Overview page.
- `n/a` makes missing or uninitialized SSN state visible without waiting for a
  path diagnostic.
- The change reuses the existing read-only SSN provider and does not introduce a
  second source of truth.

### Risks

- The line is slightly longer. Keeping only the integer generation avoids the
  wider raw EWMA display.
- Operators may still need startup logs or support diagnostics to determine why
  the value is `n/a`.

### Operational impact

No VOACAP prediction, fallback, cache, worker, persistence, YAML, telnet,
propagation-log, or path-glyph behavior changes. This is an interactive console
observability change only.

## Links

- Related issues/PRs/commits: none
- Related code: `internal/cluster/bootstrap.go`,
  `internal/cluster/main_runtime.go`, `internal/voacap/sunspot_monitor.go`
- Related tests: `internal/cluster/main_stats_test.go`,
  `ui/dashboard_v2_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `customgpt/support-cards/path-reliability.md`
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0182 operator diagnostics visibility

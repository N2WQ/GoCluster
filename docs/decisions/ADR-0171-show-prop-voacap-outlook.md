# ADR-0171: SHOW PROP VOACAP Outlook

- Status: Superseded
- Date: 2026-06-10
- Decision Origin: Scope Ledger v2

## Context

Operators and telnet users need a direct answer to "when can I work this path?"
The existing VOACAP fallback already computes rolling hourly forecasts for a
band and path, runs both directions, applies the requesting user's receive-side
noise penalty at lookup time, and keeps the lookup delayed and nonblocking. The
fallback previously exposed that information only indirectly when replacing an
insufficient live path glyph.

The new command must not block a long-lived telnet session on an external
VOACAP process, must not introduce a second cache or config surface, and must
not make bucket p50 appear to be a forecast for future hours.

## Decision

Add `SHOW PROP <call|prefix|grid> [band] [mode]` as a telnet command backed by
the existing VOACAP fallback cache.

The command:

- starts from the requesting user's saved `SET GRID`
- resolves the target as an explicit grid, gridstore callsign, or CTY-derived
  prefix/callsign center
- uses `FT8` when mode is omitted
- queries all configured VOACAP fallback bands when band is omitted
- returns only cached rows from the current UTC hour through
  `voacap_fallback.forecast_hours`
- reports a cold miss as "computing, ask again shortly" while using the
  existing delayed fallback enqueue path

Displayed rows use:

```text
EFF = merge_receive_weight * RX + merge_transmit_weight * TX
```

where `RX` is target-to-user after the requesting user's `SET NOISE` penalty and
`TX` is user-to-target. `REL` is the GoCluster path class selected from the
requested mode thresholds. Bucket p50 is intentionally not displayed.

## Alternatives considered

1. Add a synchronous VOACAP command path.
   - Rejected because telnet sessions must remain responsive and external
     VOACAP process time is not bounded tightly enough for an interactive
     command.
2. Show bucket p50 beside the VOACAP rows.
   - Rejected because p50 is current observed evidence, not an hourly forecast,
     and showing it next to future VOACAP hours implies more precision than the
     model has.
3. Add a per-command hour-count argument.
   - Rejected for this slice. The existing `voacap_fallback.forecast_hours`
     setting already owns cache horizon and resource cost.
4. Add a new YAML setting or disk cache for `SHOW PROP`.
   - Rejected because the approved feature is an exposure of the existing
     fallback cache, not a new retention or operator-control surface.

## Consequences

### Benefits

- Gives users a direct cache-first hourly outlook without changing live glyph
  semantics.
- Reuses the existing bounded delayed fallback queue and cache.
- Keeps sufficient bucket p50 authoritative for spot display.
- Makes `EFF`, `RX`, `TX`, and `REL` visible without exposing p50 as a future
  forecast.

### Risks

- A cold path requires one follow-up command after the fallback delay and worker
  run complete.
- All-band requests can enqueue or observe misses for multiple bands at once,
  bounded by the existing queue and cache settings.
- CTY-derived prefix/callsign targets are country-center approximations when no
  explicit grid or gridstore record exists.

### Operational impact

- No new YAML keys, disk cache, or synchronous VOACAP execution.
- HELP, README, operator docs, path docs, and support-agent routing must stay
  aligned because this is a user-visible command.
- The number of displayed hours follows `voacap_fallback.forecast_hours` and
  cached current-hour-forward records only.

## Links

- Related code: `telnet/show_prop.go`, `telnet/server.go`,
  `pathreliability/voacap_fallback.go`
- Related config: `data/config/path_reliability.yaml`
- Related tests: `telnet/show_prop_test.go`,
  `pathreliability/voacap_fallback_test.go`, `commands/processor_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `telnet/README.md`, `commands/README.md`,
  `customgpt/support-cards/path-reliability.md`
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0162 and ADR-0169 exposure of cached
  VOACAP forecast records; superseded by ADR-0172 for `SHOW PROP` command
  behavior

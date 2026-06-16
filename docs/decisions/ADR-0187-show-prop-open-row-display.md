# ADR-0187: SHOW PROP Open Row Display

- Status: Accepted
- Date: 2026-06-16
- Decision Origin: Scope Ledger v1

## Context

ADR-0172 made `SHOW PROP <call|prefix|grid> [band] [mode]` a cache-backed,
glyph-first point-to-point outlook. It preserved the existing VOACAP fallback
worker, bounded single-band wait, nonblocking all-band refresh behavior, and
mode-specific `EFF`, `RX`, and `TX` glyph columns.

The command still displayed every cached hourly record. That made the output
noisy for operators asking "when can I work this path?", because rows whose
merged `REL` prediction was `UNLIKELY` or `CLOSED` consumed most of the screen
on poor paths while adding little immediate contest value.

## Decision

Keep ADR-0172's cache, worker, wait, refresh, target-resolution, mode-default,
and glyph-column contracts, but filter displayed forecast rows by the final
merged `REL` prediction.

`SHOW PROP` now displays only rows whose `REL` prediction is:

- `HIGH`
- `MEDIUM`
- `LOW`

Rows whose `REL` prediction is `UNLIKELY` or `CLOSED` are hidden. The filter is
based on the `REL` column, not individual `EFF`, `RX`, or `TX` glyphs, so a row
can still display a weak leg glyph when the merged path class is `LOW`.

If a cache window exists but every cached row is hidden, the command reports
that there are no `HIGH`/`MEDIUM`/`LOW` rows in the current forecast window.
Cold misses, partial windows, single-band waits, and all-band background
refreshes keep the ADR-0172 behavior.

## Alternatives considered

1. Keep showing all cached rows.
   - Rejected because it makes the command less useful for the primary operator
     question: which upcoming hours look workable?
2. Add a new command argument such as `SHOW PROP ALL`.
   - Rejected for this slice. The goal is a simpler default, and a new option
     would expand parser/help/test scope.
3. Filter on `EFF`, `RX`, or `TX` glyphs independently.
   - Rejected because `REL` is the command's final merged prediction. Per-leg
     filtering would hide valid marginal paths when one direction is weak but
     the merged class remains `LOW`.
4. Make the filter configurable.
   - Rejected because this is a user-experience default, not a new operator
     policy surface. No YAML or per-user state is needed.

## Consequences

### Benefits

- `SHOW PROP` focuses on hours that are more likely to be useful during
  operating or contesting.
- Output is shorter without changing the underlying forecast cache horizon.
- The command remains bounded and continues to use the existing VOACAP worker
  and cache path.

### Risks

- Operators no longer see closed or unlikely hours by default in `SHOW PROP`.
- A poor path can produce no displayed rows even when the cache is warm, so the
  no-open-row message must distinguish that case from a cold miss.
- Documentation and support routing must explain that the row filter is based
  on `REL`, not individual leg glyphs.

### Operational impact

No config, cache, worker queue, threshold, or live spot glyph behavior changes.
`SHOW PROP` is now a useful-hours view over the same cached VOACAP forecast
window.

## Links

- Related code: `telnet/show_prop.go`
- Related tests: `telnet/show_prop_test.go`, `commands/processor_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `telnet/README.md`, `commands/README.md`,
  `customgpt/support-cards/path-reliability.md`,
  `customgpt/troubleshooting-index.md`
- Related ADRs: ADR-0172, ADR-0171, ADR-0166
- Related TSRs: none

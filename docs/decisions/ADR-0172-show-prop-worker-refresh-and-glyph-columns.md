# ADR-0172: SHOW PROP Worker Refresh And Glyph Columns

- Status: Accepted
- Date: 2026-06-10
- Decision Origin: Scope Ledger v4

## Context

ADR-0171 added `SHOW PROP <call|prefix|grid> [band] [mode]` as a cache-first
view of the rolling VOACAP fallback horizon. That first slice intentionally
reported cold misses as "computing, ask again shortly", defaulted omitted mode
to FT8, and displayed numeric `EFF`, `RX`, and `TX` SNR values.

Follow-up operator review changed the desired user experience:

- command output should use the same mode-specific glyph vocabulary users see
  in telnet spots
- omitted mode should default to CW
- an explicit single-band command should not force the user to ask again when a
  VOACAP run can usually finish quickly
- VOACAP execution should still use the existing fallback worker, FIFO queue,
  cache, and inflight bounds rather than a second worker or priority lane

VOACAP process execution is usually fast, but it is serialized by the Go runner
mutex and the external VOACAP lock file. A command path still needs a bounded
wait and a safe fallback status.

## Decision

Supersede ADR-0171 for `SHOW PROP` command behavior.

`SHOW PROP` now:

- defaults omitted mode to CW
- keeps single-band and all-band forms
- displays mode-specific glyphs for `EFF`, `RX`, and `TX`
- keeps `REL` as text for the merged effective path class
- keeps bucket p50 out of the command output
- treats empty or partial current-hour-forward cache windows as refresh-worthy
- bypasses `voacap_fallback.delay_seconds` only for command-triggered refreshes
- uses the existing VOACAP fallback worker queue and cache for refresh work
- waits only for explicit single-band requests, bounded by
  `voacap_fallback.show_prop_wait_milliseconds`
- keeps all-band requests nonblocking while enqueueing refreshes for missing or
  partial bands

The new YAML setting:

```yaml
voacap_fallback:
  show_prop_wait_milliseconds: 750
```

uses milliseconds, allows `0` to enqueue without waiting, and is bounded to
`0..2000`.

`EFF` remains the configured receive/transmit merge. `RX` is the DX-to-user leg
after the requesting user's receive-noise penalty. `TX` is the user-to-DX leg.
`RX` and `TX` are per-leg projections on the same glyph scale, not independent
live spot glyph decisions.

## Alternatives considered

1. Add a separate direct `SHOW PROP` worker.
   - Rejected. VOACAP already serializes through the shared run directory and
     lock file, so a second worker lane adds complexity without real
     parallelism.
2. Add a priority queue for command refreshes.
   - Rejected. Expected `SHOW PROP` command volume is low, and priority would
     alter the existing worker fairness model.
3. Wait for all-band requests.
   - Rejected. All-band cold or partial cache can require many band forecasts;
     the command remains bounded by showing available rows and refreshing in
     the background.
4. Keep numeric `EFF`, `RX`, and `TX` columns.
   - Rejected. Users are already trained on glyphs, and the command should be
     consistent with telnet spot output.
5. Keep FT8 as the omitted-mode default.
   - Rejected. CW is the preferred default operating mode for this command.

## Consequences

### Benefits

- Single-band `SHOW PROP` usually returns useful rows on the first request.
- All VOACAP execution still flows through the existing bounded worker queue.
- Output uses the same glyph vocabulary as telnet spot displays.
- The command remains bounded under VOACAP lock contention, queue pressure, or
  process failures.

### Risks

- A single-band command may now wait up to the configured budget.
- All-band requests can enqueue one refresh per missing or partial configured
  band, bounded by existing VOACAP band configuration and queue depth.
- A freshly generated current-hour window can still have fewer rows than the
  configured horizon if VOACAP lacks common directional records.

### Operational impact

- Operators can tune or disable single-band command waiting with
  `voacap_fallback.show_prop_wait_milliseconds`.
- HELP, README, operator docs, path docs, telnet docs, and support-agent routing
  must describe CW default, glyph columns, and the single-band versus all-band
  refresh distinction.
- Existing live fallback glyph behavior keeps its delayed nonblocking semantics.

## Links

- Related code: `telnet/show_prop.go`, `pathreliability/voacap_fallback.go`,
  `pathreliability/config.go`
- Related config: `data/config/path_reliability.yaml`
- Related tests: `telnet/show_prop_test.go`,
  `pathreliability/voacap_fallback_test.go`,
  `pathreliability/config_test.go`, `commands/processor_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `telnet/README.md`, `commands/README.md`,
  `customgpt/support-cards/path-reliability.md`,
  `customgpt/troubleshooting-index.md`
- Related TSRs: none
- Supersedes / superseded by: supersedes ADR-0171 for `SHOW PROP` command
  behavior; continues ADR-0162 and ADR-0169 VOACAP cache/merge semantics

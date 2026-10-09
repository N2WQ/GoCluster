# TSR-0049 - NOFILTER Restored NEARBY Restrictions

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: filter reset and telnet configuration
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

- NOFILTER acknowledged that all filters were disabled, but could retain old
  geography while turning NEARBY off.
- `Filter.Reset` cleared continents, zones, grids and DXCC, then called
  `DisableNearby`, which restored the old snapshot. State rules were reset
  afterward and therefore escaped the defect.
- Retire NEARBY before resetting every domain. Ordinary NEARBY OFF still
  restores its snapshot; RESET FILTER still selects cluster defaults.
- Tests cover all ten geography axes and snapshot retirement. A checkout
  observer first proves a restrictive filter hides an existing archive row,
  then sees it after NOFILTER and after server replacement.
- Support answer: NOFILTER clears restrictions and disables NEARBY while
  retaining the user's GRID. OFF restores prior geography only when its
  snapshot has not already been cleared by reset.

## Checker Boundaries

State-only tests cannot reveal this reset-order bug. History also bypasses
ordinary filtering when the DX call matches the user's own normalized call;
use a different DX identity and an explicit restrictive before-control.

The isolated fixture's manual bridge omits production geography enrichment.
A named REJECT NA does not hide unknown geography. Its first attempted control
failed for that reason. An EU-only allowlist hides unknown geography and then
reveals the exact row after reset; matcher tests separately exercise literal
known-geography and valid-grid candidates.

## Decision Disposition And Verification

This corrects the established permissive NOFILTER contract, with no new policy,
schema or ordinary OFF/default semantics, so no separate ADR is required.
Commands and observed results are recorded in the
[execution record](../command-fix-execution.md#checkpoint-02-nofilter-and-nearby-restoration).
Local qualification does not claim deployment or production-load evidence.

Monitor user reports of missing spots alongside `SHOW FILTER` and NEARBY state.
The supported distinctions are described in the
[telnet README](../../telnet/README.md#grid-noise-and-nearby).

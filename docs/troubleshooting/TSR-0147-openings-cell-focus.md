# TSR-0147 - Openings Cell Selection Carried Into Spots

Status: Monitoring
Date Opened: 2026-11-26
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: Browser console navigation
Trigger Source: Operator report
Led To ADR(s): none

## RCA Summary

- What happened: After opening a band and continent cell, clicking Spots left the list narrowed to that cell. Returning to the Openings grid could also make other continents appear empty.
- Why: The cell wrote its selection into the browser's band and continent controls. Navigation changed the view and kept that selection.
- What fixed it: The console tracks whether Spots was opened from a cell and clears that selection when the operator leaves the drill-down.
- How we know: A local Edge regression reproduced the original behavior and passed with the correction.
- Operator/support answer: Select the Spots icon to return to All bands under the active rules and search. On an older bundle, dismiss the continent chip and then select All in the band strip.

## Triggering Request

- Request date: 2026-11-26
- Request summary: Return to All bands when reopening Spots after selecting an Openings cell.
- Request reference: Owner chat and Approved v1.

## Symptoms and Impact

The spot list hid other bands and continents. The grid could show zero stations in other continents even when matching reports were available. The symptoms concern what the browser displays from reports it received.

## Timeline

1. 2026-11-26. The owner reported that returning to Spots kept the cell selection.
2. 2026-11-26. A local Edge check reproduced the retained selection and verified that clearing the continent chip and selecting All restored the rows.
3. 2026-11-26. The owner selected All as the return behavior. The local regression passed after implementation.

## Hypotheses and Tests

1. The browser retained the cell selection across navigation. A simulated live feed started with reports on two bands and two continents. The selected cell narrowed the rows, then the Spots toolbar action kept the same rows and the browser Back action opened a grid with the other continent disabled. Outcome: Supported.
2. The node stopped supplying other spots. Clearing the two display controls immediately restored the original rows from the same simulated feed. Outcome: Rejected for the reproduced case.

## Findings

- Root cause: A cell opens the same `#stream` route as ordinary Spots. Clicking the Spots toolbar icon from a cell can therefore leave the fragment unchanged. The explicit toolbar handler must clear the temporary selection even when no browser navigation event fires.
- Contributing factor: The continent chip can be cleared independently, so its presence alone cannot identify an active drill-down.

## Decision Linkage

The owner selected All as the return behavior. The operator guide covers this browser interaction. [ADR-0377](../decisions/ADR-0377-browser-band-strip-filter-scope.md) continues to govern band eligibility, counts, and filter previews.

## Verification and Monitoring

- Validation steps run: The Edge Openings tests in `cloudflare/gocluster-pilot/test/filters.cjs` cover the toolbar, browser Back and Forward, search, and a manually chosen band outside a drill-down.
- Signals to monitor: A selected band or continent chip persisting after the Spots toolbar action, or matching activity disappearing from other continents after reopening the grid. Check the delivered app asset before attributing a report to this fix.
- Rollback triggers: Use the [console rollback procedure](../../cloudflare/gocluster-pilot/PRODUCTION.md#rollback) if the released UI prevents normal navigation or displays the wrong spots.

## References

- [Openings guide](../../cloudflare/gocluster-pilot/README.md#read-the-openings-grid)
- [Browser regression tests](../../cloudflare/gocluster-pilot/test/filters.cjs)

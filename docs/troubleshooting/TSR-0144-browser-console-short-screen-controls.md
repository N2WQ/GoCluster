# TSR-0144 - Browser Console Controls Clipped on Short Screens

Status: Monitoring
Date Opened: 2026-11-24
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: Browser console layout and focus
Trigger Source: Operator report
Led To ADR(s): none

## RCA Summary
- What happened: The operator reported being unable to scroll the browser console on an iPhone in landscape.
- Why: The frame enforced a fixed viewport height and locked document scrolling. The header and expanded console could exhaust the workspace. The Post a spot dialog had no scroll path to its footer, and the toolbar hid or clipped the launcher at some widths.
- What fixed it: Constrained layouts let the document scroll while retaining the Spots list scrollport. The toolbar wraps, and overlays can scroll within the visible viewport.
- How we know: The original layout reproduced the clipping locally in Chromium. The responsive browser tests cover reachability, rotation, retained edits, a simulated keyboard-size viewport reduction, and the docked desktop layout.
- Operator/support answer: Scroll the page to reach the command console. Scroll inside a drawer or dialog to reach its actions. Record the iOS version, orientation, and whether the keyboard and Safari bars are visible if a control is still inaccessible.

## Triggering Request
- Request date: 2026-11-24
- Request summary: Ensure the console is usable at different screen sizes and orientations after the landscape report.
- Request reference (chat/issue/link): Owner chat and Approved v1 responsive layout scope.

## Symptoms and Impact
- What failed or looked wrong? The Spots list and Filters content area could be reduced to zero height. History Search and the Post a spot footer could be unreachable.
- User/operator impact: Operators could lose access to results or actions without rotating the device or changing the layout.
- Scope and affected components: Browser UI in the Cloudflare console bundle.

## Timeline
1. 2026-11-23 - The operator reported the iPhone landscape scrolling problem.
2. 2026-11-24 - Local browser checks reproduced clipping in replay and with a simulated live node.
3. 2026-11-24 - The responsive layout and interaction checks were added under Approved v1.

## Hypotheses and Tests
1. Hypothesis A - The fixed frame could exhaust usable workspace height.
   - Evidence/commands: At 844 × 300 CSS pixels with replay and the command console open, the Spots scrollport had zero height.
   - Outcome: Supported.
2. Hypothesis B - The spot form footer might be reachable by scrolling.
   - Evidence/commands: At 844 × 260 CSS pixels, the footer was outside the viewport. A wheel gesture over the form left the footer in the same position.
   - Outcome: Rejected.

## Findings
- Root cause (or best current explanation): Both the page frame and the dialog prevented the available content from reflowing into a reachable vertical scroll path.
- Contributing factors: Width-only breakpoints, a dock sized by viewport height, and toolbar overflow clipping.
- Why this did or did not require a durable decision: The correction concerns browser presentation and preserves the browser console transport and session contract.

## Decision Linkage
- ADR created/updated: none
- Decision delta summary: none
- Contract/behavior changes: Document scrolling in constrained layouts and viewport-bounded scrolling for overlays.

## Verification and Monitoring
- Validation steps run: Local Microsoft Edge responsive browser tests. The [console release checks](../../cloudflare/gocluster-pilot/PRODUCTION.md#test-before-publishing) cover physical iPhone Safari qualification, which remains open.
- Signals to monitor (metrics/logs): Reports of missing controls in landscape, when zoomed, or while the on-screen keyboard is visible.
- Rollback triggers: The release runbook covers a UI regression that prevents ordinary console use.

## References
- Issue(s): none
- PR(s): none
- Commit(s): n/a
- Related ADR(s): [ADR-0375](../decisions/ADR-0375-private-browser-console-transport.md)
- Related docs: [Production browser console](../../cloudflare/gocluster-pilot/README.md)

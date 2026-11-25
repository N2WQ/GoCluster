# ADR-0377: Browser band strip filter scope

- Status: Accepted
- Date: 2026-11-25
- Decision Origin: Design

## Context

The browser band strip showed all bands and counted received reports while the
filter summary and Spots could already be previewing different rules. Quiet
bands need stable selectors, and eligible counts need to reflect actual matches.

## Decision

Use the same confirmed GO profile or valid, clearly labeled draft for the
filter summary and band strip. Keep the last confirmed rules when the draft is
invalid, stale, or conflicting. When the browser lacks the active rule bank,
leave the selectors available and mark counts unavailable until a valid GO
draft is available for an explicitly labeled preview.

Hide a band only when its compiled rules rule it out using band predicates and
known Boolean results. Keep a quiet badge when eligibility depends on other
spot properties. Count ordinary matching reports in the browser's retained
15-minute window. SELF bypass adds no eligibility or count. **All** sums the
displayed band counts. An excluded selected band returns the display to **All**.

## Alternatives considered

1. Use the received records to determine badge visibility. This would hide
   eligible bands during quiet periods.
2. Prove eligibility across every possible combination of spot fields. This
   would enlarge the browser's filter analysis and its compatibility surface.
3. Count received reports without applying the current rules. This could show
   activity the corresponding summary excludes.

## Consequences

The strip stays stable on quiet feeds. Its counts describe the retained reports
the browser saw and can verify. Some uncertain bands remain present with quiet
badges. The conservative analysis uses the existing browser rule limits and
requires no network requests or change to the node's delivery contract.

## Links

- [Browser console guide](../../cloudflare/gocluster-pilot/README.md#toolbar-and-band-counts)
- [Browser filter regressions](../../cloudflare/gocluster-pilot/test/filters.cjs)
- [ADR-0376](ADR-0376-shared-browser-telnet-profile.md)

# TSR-0054 - NEARBY Opposite Endpoint Veto

Status: Monitoring
Date Opened: 2026-10-09
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: filter NEARBY, live delivery and history
Trigger Source: Chat request
Led To ADR(s): none

## RCA Summary

NEARBY could reject a spot whose DX or DE cell matched the user's cell whenever
the opposite side had no usable grid or mapping. This contradicted the existing
DX-or-DE condition documented in HELP and the telnet guide.

## Root cause or best current explanation

The cell helper returned two unavailable cells if either grid was empty. The
matcher then required both cells to be available before testing either equality.
Removing only one of those prerequisites leaves the other veto in place.

## Fix or mitigation

Resolve each endpoint independently and compare the valid user cell with either
result. Keep the existing band resolution and fine-cache precedence. Fine
matching uses nonzero Spot cache IDs before deriving cells from grids; coarse
matching on 160m, 80m and 60m derives grids. Spot cache ownership remains governed
by [ADR-0146](../decisions/ADR-0146-h3-path-cell-duplicate-work-removal.md).

Both unavailable endpoints, neither endpoint matching, an unknown band, or an
unavailable user cell still reject. NEARBY's suspension of ordinary location
rules and unrelated filter refusals remain. This is a correction to the existing
contract, so no new durable decision or separate ADR is required.

## Checker Boundaries

Unit tests swap DX and DE on fine and every coarse band. Cached fine IDs with
missing or conflicting grids must retain precedence; coarse matching must not
borrow them. A joined fresh child loads private count-valid H3 tables with the
distant control cells removed. Required positive and mapping-miss guards prove
that this exercises a valid but unmappable grid, rather than invalid syntax or
the parent's already initialized global tables.

Consumer tests call actual live delivery and a captured history predicate with
different user and spot callsigns, avoiding the own-call exemption. The checkout
fixture adds a fixed set of synthetic metadata rows to its existing bounded
queue. Before enabling NEARBY, the driver must see every positive and negative
row in the real archive. After activation, only matching rows may remain.
Both dialects and fresh fixture processes are required.

These tests do not qualify production enrichment, a separate unpaused socket
delivery stream, geographic model accuracy, or production load. The allocation
guard is a bounded-cost check, not a comparative performance measurement.

## Decision Disposition And Verification

The [execution record](../command-fix-execution.md#checkpoint-08-independent-nearby-endpoints)
records source discovery, the failure matrix, observed commands and limitations.
The [telnet guide](../../telnet/README.md#grid-noise-and-nearby) explains the
operator-visible condition. Monitor missing-spot reports against the user grid,
band, available endpoint cells and other active filters.

Support answer: either DX or DE may supply the matching nearby cell. The other
side does not need a usable grid. If neither matches, the spot is rejected.

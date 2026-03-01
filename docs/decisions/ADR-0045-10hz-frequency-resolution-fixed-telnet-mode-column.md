# ADR-0045 - 10 Hz Frequency Resolution and Fixed Telnet Mode Column

Status: Accepted
Date: 2026-02-28
Decision Makers: Cluster maintainers
Technical Area: spot, skew, main output pipeline, telnet formatting
Decision Origin: Design
Troubleshooting Record(s): none
Tags: frequency-resolution, telnet-format, compatibility

## Context
- Frequency values were rounded to 100 Hz (0.1 kHz) in key normalization/correction stages, which discards information now needed by operators.
- The telnet DX line is a fixed-column protocol used by downstream clients; mode must stay anchored at column 40.
- We need higher frequency precision without changing line length, tail anchors, or mode column placement.

## Decision
- Move canonical frequency rounding to nearest 10 Hz (0.01 kHz), half-up, in these pipeline stages:
  - spot construction normalization (`spot.NewSpot`, `spot.NewSpotNormalized`)
  - skew correction application (`skew.ApplyCorrection`)
  - frequency averaging application (`main.processOutputSpots`)
- Update telnet spot rendering to always show frequency with exactly two decimal places.
- Keep existing fixed-width telnet layout unchanged, including:
  - frequency field end at column 25,
  - mode start at column 40,
  - fixed tail anchors for grid/confidence/time.

## Alternatives Considered
1. Keep 100 Hz rounding and one-decimal telnet frequency
   - Pros: no compatibility risk, no test/doc churn.
   - Cons: loses requested precision and cannot represent 10 Hz corrections.
2. Use 10 Hz internally but keep one-decimal telnet display
   - Pros: minimal protocol surface change.
   - Cons: hides effective precision from clients and creates operator confusion.
3. Expand telnet line format with wider frequency field
   - Pros: explicit precision with easier visual parsing.
   - Cons: breaks fixed-column consumers and violates existing output contract.

## Consequences
- Positive outcomes:
  - End-to-end 10 Hz precision is preserved where frequencies are normalized/corrected.
  - Telnet clients receive explicit two-decimal frequency values without column drift.
  - Mode and tail parsing compatibility remain stable for fixed-column consumers.
- Negative outcomes / risks:
  - Any consumer that assumed one decimal in frequency text must adapt.
  - Small changes in averaging/skew thresholds can alter borderline correction outcomes.
- Operational impact:
  - Logs and dashboards that parse telnet lines can rely on two-decimal frequency text.
  - No queue/backpressure/shutdown contract changes.
- Follow-up work required:
  - Keep regression coverage for mode-column anchor and fixed tail positions.

## Validation
- Added/updated tests for:
  - 10 Hz half-up rounding in spot normalization and skew correction.
  - Telnet format alignment with two-decimal frequency and mode at column 40.
- Evidence that would invalidate this decision:
  - Any observed telnet column drift or broad client breakage from two-decimal parsing assumptions.

## Rollout and Reversal
- Rollout plan:
  - Ship as a single protocol-format update with docs and tests.
- Backward compatibility impact:
  - Line length and field anchors remain stable; only frequency text precision changes (one decimal to two).
- Reversal plan:
  - Revert rounding precision and format width in the same three pipeline stages and `spot.FormatDXCluster`, then publish a superseding ADR.

## References
- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0031-skew-selection-min-abs-skew.md`
- Troubleshooting Record(s): none
- Docs:
  - `README.md`

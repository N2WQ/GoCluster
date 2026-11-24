# TSR-0143 - Path Cap Diagnostic Ageing

Status: Monitoring
Date Opened: 2026-11-24
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: path reliability, receiver accounting, Telnet diagnostics
Trigger Source: Production propagation audit
Led To ADR(s): none

## RCA Summary

- What happened: In six production logs, `cap_limited` appeared on 70.04% of
  recorded path evaluations. A controlled reproduction set the flag after
  ageing alone with equal raw usable and admitted support.
- Why: The diagnostic compared a lifetime input count with the rounded,
  aged capped count. The count gap could grow without receiver-cap reduction.
- What fixed it: Compare raw usable and admitted units at the same sampling
  time, with the existing `1e-9` diagnostic tolerance and weight check.
  The merge reads capped availability independently of this flag.
- How we know: Controlled cases exercise ageing, a fractional loss hidden
  by integer rounding, unattributed input, known zeroes, and Telnet output.
- Operator/support answer: Compare `cap_limited` only within the same
  diagnostic behavior. The old figure counts flags and cannot identify
  genuine reductions. A change across deployment can reflect the new
  accounting while glyph and fallback rules stay the same.

## Evidence

The six UTC-dated files cover November 18–23, 2026. They contain 16,492,156
flags across 23,546,954 repeated path evaluations. The aggregate logs lack
the paired raw and admitted values needed to estimate false positives.

## Root cause or best current explanation

The lifetime input count stayed high as the rounded capped count decayed.
The count comparison could therefore set the flag even when the raw and
admitted quantities declined by exactly the same amount.

## Fix or mitigation

The flag now describes cap-lane evidence reduction, including usable
unattributed reports the cap lane excludes. `/c` and `/rx` use the flag
for display, and `|w` selects the capped or active weight accordingly.
The support-floor and receiver-diversity comparisons remain governed by
[ADR-0309](../decisions/ADR-0309-recent-report-support-and-receiver-units.md).

## Verification And Monitoring

- `pathreliability/cap_diagnostics_test.go` covers the tolerance boundary,
  repeated decay, real fractional loss, and capped zero provenance.
- `telnet/path_cap_diagnostics_test.go` covers the rendered diagnostic and
  the propagation counter from actual path predictions.
- Use the deployment timestamp to distinguish old and new metrics. If
  path ratings or fallback decisions change for the same evidence, review
  the scoring path before interpreting the change as a telemetry effect.

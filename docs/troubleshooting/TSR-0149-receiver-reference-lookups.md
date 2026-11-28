# TSR-0149 - Tagged receiver misses reliability and frequency correction

Status: Monitoring
Date Opened: 2026-11-28
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: receiver reliability, frequency skew, resolver confidence
Trigger Source: Operator report
Led To ADR(s): none

## RCA Summary

The operator reported excess `S` confidence on busy contest calls. RBN
reporters with a terminal `-#` could miss the reliability and frequency
correction entries for their receiver ID. For example, `M9PSY-#` received the
default weight of 1.0 even though `M9PSY` was below the 0.95 minimum. The
missed skew correction for `LB9KJ-1-#` could put its evidence in a different
frequency bucket from other receivers.

## Triggering Request

Diagnose excess `S` during live CW traffic and implement the two lookup fixes
under Approved v3.

## Symptoms and Impact

An unexpectedly admitted receiver can add candidate pressure and trigger
protected evidence loss. A missed skew correction can split support across
frequency buckets. A recognized station can also display `S` while it has
thin current evidence, as observed in the ED7W trace. The glyph by itself
cannot identify which condition occurred.

## Timeline

1. The live `e9a1275b` sample and contest captures exposed apparent receiver
   reliability bypass and continuing evidence loss.
2. Baseline regressions reproduced both lookup misses. The patched tests
   exercised resolver admission and RBN and PSKReporter ingest.

## Hypotheses and Tests

- A matched replay of 1,531 selected archived CW reports, with the
  16-candidate limit and input frequencies fixed, increased reliability
  rejections from 25 to 502 and reduced candidate evictions from 46 to 28.
  Evidence loss affected 27 of 126 archived `S` rows on the baseline and
  12 of 126 with the patch. These archived reports approximate resolver
  input after delivery and cannot establish live causality or skew impact.
- In a three-report RBN fixture with a one-candidate limit and independent
  recognition of the focal call, the final projected glyph was `S/--%`
  on baseline, `S/100%` with reliability alone, `S/--%` with skew alone,
  and `V/100%` with both fixes. The fixture isolates the two effects.

## Root cause or best current explanation

The ingest paths retained the source marker on the receiver ID, and both
lookup paths required an exact table key. The shipped entries used the
receiver ID without the marker.

## Fix or mitigation

The lookup now tries the exact receiver ID before removing one terminal
`-#` for a fallback. Reliability checks mode then global for each spelling.
Both lookups keep numeric IDs, and the evidence keeps the original receiver ID.
On RBN, burst history still receives the original CQ frequency.

## Decision Linkage

The repair applies existing receiver reference data to decorated receiver
IDs. Candidate capacity and protected evidence-loss authority continue to
follow ADR-0292 and ADR-0384.

## Verification and Monitoring

After deployment, compare reliability drops, candidate and receiver
evictions, and DIAG CONF `S/--%`, `S/100%`, and transitions to `V` across
comparable traffic windows. Check reports from all `S` stations, including
W3LPL, A44A, HG7T, and W2FU when present.

## References

- [Resolver cap-loss authority](../decisions/ADR-0292-resolver-cap-loss-authority.md)
- [CW resolver receiver budget](../decisions/ADR-0384-cw-resolver-receiver-budget.md)
- [Receiver reliability lookup](../../spot/reliability.go)
- [Frequency skew lookup](../../skew/skew.go)

# TSR-0093 - Path Median and Snapshot Recovery

Status: Resolved
Date Opened: 2026-10-13
Date Resolved: 2026-10-14
Owner: Repository owner and Codex
Technical Area: pathreliability, internal/propreport
Trigger Source: Chat request
Led To ADR(s): ADR-0301
Tags: exact median, float32, repeated snapshot, malformed variance

## RCA Summary
- What happened: review of path_relia at 684d145 found a false p50 tie and two
  malformed-log recovery gaps after the timestamp-accounting repair.
- Why: float64 summation can absorb positive float32 mass; whole-AsOf reader
  replacement can erase an earlier complete band; variance validation permits
  a degeneracy flag inconsistent with the publisher's maximum.
- What fixed it: owner-approved v3 selects exact integer median comparison and
  per-band last-valid snapshot recovery with consistent degeneracy flags.
  Slice 1 implements the median; slice 2 implements the reader recovery.
- How we know: separate non-steered mathematical review derived full-domain
  bounds and golden vectors. Native baseline regressions reproduced -14/MEDIUM
  with 113 reports and three receivers. Candidate median targeted and full
  ordinary/race/analyzer gates pass, with guided fuzz and matched performance
  evidence. Native reader baseline regressions reproduce band loss and both
  contradictory flags; candidate collector and Generate JSON/text cases pass.
  Both reader full Windows ordinary/race lanes and analyzers pass; guided
  fuzzing passes 193,886 executions with 76 new coverage inputs. Separate
  read-only source review and follow-up review find no remaining blocker.
- Operator/support answer: inspect observed p50 and evidence provenance before
  tuning thresholds. Correct histogram arithmetic does not establish RF/QSO
  calibration or the field prevalence of these defects.

## Triggering Request
- Request date: 2026-10-13
- Request summary: consider independent review findings and repair them in
  small sequential slices with full testing before advancing.
- Request reference (chat/issue/link): owner chat, exact Approved v3.

## Symptoms and Impact
- Balanced endpoint bins plus a tiny positive middle bin could produce -14
  instead of -5.5 dB, changing FT8 HIGH to MEDIUM under shipped gates.
- A later usable 40m band with incomplete 20m could erase prior valid 20m.
- Positive maximum with deg=1 could produce a false degeneracy conclusion.
- Reporting defects require malformed records; production prevalence and QSO
  impact were not measured. Normal publisher output supplies consistent flags.

## Timeline
1. 2026-10-13 - Source and exact-rational review confirmed the three mechanisms.
2. 2026-10-13 - Owner selected exact stored-mass median and approved v3.
3. 2026-10-13 - Detailed falsifiability review required bitwise immutability,
   valid-domain fuzz coverage, and explicit known-broken baseline guards.
4. 2026-10-13 - Native regression fixtures reproduced the old answers; exact
   integer implementation passed 38 targeted cases without skips.
5. 2026-10-14 - Both complete 70-package Windows lanes passed 9,306 cases with
   the same 77 existing skips; analyzers passed. Guided fuzz passed 225,007
   executions and 22 new coverage inputs. Six benchmark pairs and four controlled
   profiles passed their guards. Necessary exact-arithmetic CPU costs above the
   10% review trigger were reviewed and retained before the reader slice.
6. 2026-10-14 - Reader baseline fixtures reproduced partial-band loss and both
   contradictory flags. Candidate targeted checks passed all 86 cases; both
   complete 70-package Windows ordinary/race lanes passed 9,337 cases with the
   same 77 existing skips. All 19 required checks passed. Vet, Staticcheck,
   lint and guided reader fuzzing passed. A consumer band-identity oracle
   refinement was reviewed and tested before the full reporting-package runs.

## Hypotheses and Tests
1. Float64 accumulation guarantees exact half-mass comparisons for float32 bins.
   - Evidence/commands: independent rational vectors, native TestExactP50Golden
     and TestExactP50ShippedPrediction baseline execution.
   - Outcome: Rejected. Owner vectors returned -7 instead of 5.5/8, and the
     shipped chronological fixture returned -14/MEDIUM instead of -5.5/HIGH.
2. Five 64-bit words can cover the full stored-mass domain.
   - Evidence/commands: independent bound, rational helper reconstruction and
     TestExactP50MassEncodingAndCarries.
   - Outcome: Supported. A bin needs 277 bits, 50-bin total 283, doubled prefix
     284; the fixed accumulator provides 320.
3. Whole-sample deduplication preserves every prior usable band.
   - Evidence/commands: snapshot_logs.go whole-map replacement at equal AsOf.
   - Outcome: Rejected by source and native regression fixtures. The candidate
     preserves the prior count/weight pair and variance while replacing other
     complete bands, including known zeros.
4. An ordered nonnegative variance payload is usable even when deg contradicts max.
   - Evidence/commands: TestSnapshotRepeatedVariancePreservesValidBands and
     TestGenerateSnapshotRecovery, both contradictory flags with/without prior
     data and on-disk JSON/text assertions.
   - Outcome: Rejected. The baseline accepts both contradictions; the candidate
     retains prior valid variance or leaves availability false when none exists.

## Root cause or best current explanation
At the baseline, p50 independently rounded total and prefix sums in float64.
Positive retained mass could disappear from either sum and change the crossing
or equality decision. The modern reader overwrote whole band maps at a repeated
AsOf, and variance validation did not enforce the publisher's deg/max relation.

## Findings
- Exactness is relative to stored float32 bins, not pre-quantization reports.
- Count/weight replacement must preserve a complete pair from one captured
  logger prefix. Existing encounter order remains the replacement order.
- Valid zero replaces prior nonzero evidence; unavailable or malformed data
  does not become quantitative zero.
- Both deg=1 with max>0 and deg=0 with max=0 contradict the publisher contract.

## Decision Linkage
- ADR created/updated: [ADR-0301](../decisions/ADR-0301-exact-path-median-and-snapshot-recovery.md).
- Decision delta summary: bounded exact quantile comparison and per-band
  malformed modern snapshot recovery.
- Contracts retained: ADR-0131 representatives/midpoint convention, ADR-0139
  active histogram lane, ADR-0300 timestamps/materialization/report availability.

## Verification and Monitoring
- Validation steps run: native median targets, 38 cases without skips; complete
  ordinary/race root lanes, 53 tested and 17 build-only packages, 9,306 cases and
  77 existing skips each; vet, Staticcheck, lint, docs/support/TSR/map checks.
  Guided fuzz passes 225,007 executions, 22 new coverage inputs. The earlier
  noninstrumented run is supplemental stress evidence. Six benchmark pairs
  preserve zero median/update/prediction allocations and the 448-byte bucket.
  Dense p50 costs 410.75 vs 55.74 ns; warm prediction medians rise about 2-5%.
  A single serial profile workload rises 11.59%, requiring review; exactness is
  retained. Profiles preserve 16,384 buckets/8,192 sidecars and comparable live
  owner heap. Reader targeted checks pass 86 cases without skips; its complete
  ordinary/race lanes each pass 9,337 cases across 53 tested and 17 build-only
  packages, with the same 77 existing skips and all 19 required checks passing.
  Reader guided fuzz passes 193,886 executions and 76 new coverage inputs.
  Both slices pass analyzers, docs/support/TSR/maps and fresh diff review.
  Independent mathematical review and separate source reviews actually occurred.
  No production p99 or RF/QSO calibration claim follows from this validation.
- Signals to monitor: observed p50/provenance, receiver/count floors, median
  CPU/allocations, and report variance availability.
- Rollback triggers: rational-oracle disagreement, race, allocation/size bound
  failure, or unreviewed material runtime cost. No intermediate deployment.

## References
- Commit(s): 684d1458cc19a6cd36b38431102fe6756d0105b8 baseline;
  cbac42d4db72752fb5be1e56bc3253897aea3d9d exact-median slice.
- Related ADR(s): ADR-0131, ADR-0139, ADR-0300, ADR-0301.
- Related docs: [path README](../../pathreliability/README.md),
  [operator guide](../../data/config/PATH_PREDICTIONS.md).

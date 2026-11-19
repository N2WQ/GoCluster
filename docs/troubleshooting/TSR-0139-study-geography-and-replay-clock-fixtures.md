# TSR-0139 - Study Geography and Replay Clock Fixtures

Status: Monitoring
Date Opened: 2026-11-17
Date Resolved: n/a
Owner: Repository maintainer / Codex
Technical Area: cmd/rbn_replay study inputs, CTY/H3 provenance, fixture clocks
Trigger Source: Chat request
Led To ADR(s): ADR-0360
Tags: study, receiver geography, conditional inputs, sample clock

## RCA Summary

- What happened: archive-readiness review found no DE geography entering empty
  study history. A new CSV integration fixture then timed out after six minutes.
- Why: RBN CSV contains receiver calls and SNR, not receiver coordinates; H3
  tables transform supplied grids but do not supply them. Separately, the fixture
  bypassed `replayDay` and left the sample clock at zero time.
- What fixed it: approved v4 selects an explicit frozen CTY representative-input
  scenario with schema-2 provenance and coverage. The fixture now uses ordinary
  replay initialization rather than changing production scheduling.
- How we know: actual CSV-to-resolver-V-to-CustomSCP controls distinguish two
  derived cells from same-cell/unknown inputs; ordinary and race study receipts
  each passed 158 cases without skips. Earlier failed evidence remains retained.
- Operator/support answer: distinguish missing/corrupt setup assets from unknown
  row locations, and do not infer historical sites or qualification from seeded
  geography tests. A fixture sample-loop timeout is not production load evidence.

## Triggering Request

The owner requested actual consumer reachability before archive tuning under
[approved v3](../call-correction-v2-scope-ledger-v3.md). Readiness review exposed
the geography gap; separate scientific review and scope/test challenge preceded
the owner's exact `Approved v4` input amendment on 2026-11-17.

## Root cause or best current explanation

`newReplaySpot` supplies no DE grid. Without a location input, the unchanged
positive CustomSCP geographic minima cannot qualify empty daily archive history.
Constructed seeded-cell tests proved gate wiring, not archive input availability.
This differs from missing H3 initialization described by
[TSR-0128](TSR-0128-shared-geography-startup.md).

The first `TestCCV2StudyCSVGeographyReachesCustomSCP/two_cells` fixture called
`processCSVRow` directly after resolving its day. `nextSampleAt` was still zero;
`advanceReplayTime` repeatedly emitted minute samples while catching up to the
modern row timestamp. The timeout stack traversed those functions. Ordinary
`replayDay` initializes that clock before processing rows, so this failure did
not establish a production scheduler or CustomSCP bottleneck.

## Hypotheses and Tests

1. Loading H3 tables alone provides receiver geography. Rejected: current CSV
   projection and `newReplaySpot` contain no DE grid. Actual no-study CSV and
   unknown-location controls cannot satisfy the two-cell history gate.
2. A seeded-cell positive test establishes archive reachability. Rejected:
   only the separate CSV hook test derives inputs before real V-only learning;
   four receivers across two derived cells qualify, while same-cell/unknown
   controls retain the geographic rejection.
3. The six-minute fixture timeout measures production load. Rejected: the
   captured stack and uninitialized sample clock explain catch-up work. Using
   `replayDay` passed the five-case CSV follow-up without a scheduler change.

## Fix or mitigation

The study-only `cty_representative_v1` hook uses the frozen CTY image and live
portable receiver normalization before focal-A learning. Known four-character
grids are derived; missing matches, invalid coordinates and empty receivers stay
unknown. Valid zero coordinates remain valid. No history minimum is lowered,
and ordinary replay/server inputs remain unchanged.

Schema-2 plans/reports bind CTY/res1/res2 roles and hashes to actual arm paths.
Setup failure occurs before raw archive parsing; it cannot masquerade as unknown
row coverage. Decision records and paired comparison bind location covariates
and reconcile all focal-A known/unknown rows, including warmup and nonoutput
outcomes. Withheld B rows do not learn. Preserve old artifacts rather than
combining schema-1 reports with this scenario.

## Decision Linkage

[ADR-0360's v4 follow-up](../decisions/ADR-0360-call-correction-v2-evidence-and-bounded-bonuses.md#follow-up-approved-v4-cty-input-scenario)
records the selected conditional input policy. CTY representative locations are
not reported grids or reconstructed historical sites. This accepted input
uncertainty is separate from RBN reference-label uncertainty; using the same
proxy in each arm does not cancel nonlinear geographic-eligibility errors.

## Verification and Monitoring

- Retained receipts: `.tmp/call-correctionv2/s0b-study-targeted.txt` (timeout),
  `s0b-csv-followup.txt` (five passes), `s0b-study-followup-all.txt` (158 ordinary
  passes) and `s0b-study-race.txt` (158 race passes), all under the same directory.
  Passing groups reported zero skips.
- The lead also recorded 156 passes without skips for the detached pre-mask
  comparator and successful vet/Staticcheck/lint cleanup. These are tool/setup
  checks, not completed official archive results.
- Rebuilt controls were frozen with schema-2 plan SHA256
  `8cdba5fde284f8bf7ad34ede758afc9bcd3856bbf99a44c9321376f868864c42`.
  A control freeze does not qualify a candidate. Official tuning/holdout and
  activation decisions remain outside this record; final holdouts remain unopened
  at this checkpoint.
- Monitor complete input coverage, unknown reasons, CTY/H3 role binding and
  paired raw/reference/covariate consistency. Refuse drift or incomplete evidence
  rather than dropping unknown rows or relaxing history requirements.

## References

- Source/tests: `cmd/rbn_replay/study_geography.go`, `study_geography_test.go`,
  `study_observer.go`, `study_compare.go`, and `runner.go`.
- [Approved v4 input amendment](../call-correction-v2-scope-ledger-v4.md).
- [Execution evidence](../call-correction-v2-execution.md#s0b-study-input-amendment).
- [Study schema and evidence limits](../rbn_replay.md#explicit-study-input-scenario-schema-2).
- [Existing replay deadline contract](TSR-0101-replay-deadline-scheduling.md).

## Follow-up: a clock-dependent wire fixture

The final race lane reached the new legacy RBN SNR consumer test after UTC
midnight. Its literal `1200Z-14dB` input expected noon to survive unchanged.
`parseTimeFromRBNAt` deliberately uses today's date and clamps timestamps more
than two minutes in the future, so the correct adapter returned the current
time. This was a new test-oracle defect, not an observed data race or a change
to source timestamp policy. The failed full-run receipt is retained as
`.tmp/call-correctionv2/s7-final-race-full.txt`.

The consumer fixture now uses distinct `0000Z` and `0002Z` fused timestamps.
Both are admissible under the same-day/two-minute-skew contract at every UTC
clock; a shared current-time fallback cannot ordinarily satisfy both. Fixed-date
adapter tests continue to own future-clamp and same-day semantics. Production
parsing is unchanged. The complete RBN ordinary rerun passed 129 cases without
skips in `s7-final-rbn-clock-ordinary.txt`. RBN vet/Staticcheck/lint then pass.
The race continuation passes all 11 unfinished packages, 4,512 native test
events and five skips in `s7-final-race-continuation.txt`.
`s7-final-race-coverage-audit.json` verifies complete 76-package coverage and
exact ordinary/race skip identity, without inventing a single passed full run.

Check literal clock values against adapter policy before retrying long lanes.
A source-compatible fixture repair requires affected checks and unfinished
packages, not reclassification of the failed run as a pass.

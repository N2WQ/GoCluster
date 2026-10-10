# TSR-0075 - Historical Analysis Claims and Failures

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: historical decision readers, classification and report claims
Trigger Source: Chat request
Led To ADR(s): ADR-0277
Tags: analysis, outcomes, scientific-claims, SQL, exit-status

## RCA Summary
- What happened: distance-3 analysis printed fixed optimality, calibration and
  stability advice, classified all non-applied labels as rejections and could
  return success after a failed query or scan.
- Threshold simulation similarly printed fixed stability forecasts, presented a
  first-row tuple as common settings and omitted removed applications from net
  change. Fixed positive extras could overflow into permissive comparisons.
- Distance summaries inferred reliability/calibration advice without labels,
  treated unknowns as rejections and could overflow score sums/even medians.
- Decision inspection used inconsistent outcome predicates and ignored row
  iteration/close failures; unknowns could consume recognized-rejection caps.
- Why: hard-coded narrative and binary classification bypassed input-derived
  evidence; log-and-return main implied successful exit.
- What fixed it: descriptive counts, separate unknowns, explicit hypothetical
  gates and checked loading/reporting with unsuccessful status on failure.
- How we know: checkpoints 26/27/28b/29 execution matrices and qualification pair literal
  data/set vectors with actual-main status and isolated SQL driver faults.
- Operator/support answer: the report describes logged observations and scalar
  comparisons. It does not establish correction accuracy or optimal settings.

## Triggering Request
- Request date: 2026-10-10.
- Request summary: implement approved v3 historical command fixes.
- Request reference: `docs/command-fix-scope-ledger-v3.md`, checkpoints 26/27/28b/29.

## Symptoms and Impact
- Empty denominators produced non-finite percentages.
- Unknown or case-variant outcomes changed populations incorrectly.
- Fixed claims could be mistaken for a measured evaluation.
- A failed load could be mistaken for a successful empty population.

## Hypotheses and Tests
1. A logged confidence score proves accuracy or calibration.
   - Evidence: no independent labels or calibration experiment occur here.
   - Outcome: remove empirical advice rather than invent it.
2. Every non-applied row is a rejected correction.
   - Evidence: NULL, empty, padded and arbitrary labels are distinct observations.
   - Outcome: recognize case-only known labels; keep other values unknown.
3. A nonzero test child alone proves the intended failure path.
   - Evidence: launch or schema errors can fail before reporting.
   - Outcome: pair valid controls with causal diagnostics and stage witnesses;
     iteration/close faults must retire rows and refuse partial results.

## Findings
- Root cause: unsupported narrative, binary classification and unchecked
  command failure boundaries.
- Threshold net arithmetic must account for both additions and removals;
  heterogeneous logged tuples establish no common current configuration.
- No current producer of this legacy schema was found in inspected source.
  Reader qualification cannot certify real database provenance.
- Scenarios are hypothetical scalar predicates, not complete model replay or
  recommendations to change settings.

## Decision Linkage
- ADR created/updated: ADR-0277.
- Contract changes: descriptive reader claims and outcome semantics.

## Verification and Monitoring
- Validation steps: checkpoints 26/27/28b/29 in the execution record.
- Signals: exact partitions/means/set transitions, empty N/A and causal failures.
- Rollback triggers: unknowns enter rejected sets, fixed advice returns, failed
  loads succeed or scores acquire unsupported scientific meaning.
- Limits: local SQLite/arithmetic checks do not establish empirical correction
  accuracy, current configuration parity or external service health.

## References
- Issue(s): approved v3 command remediation.
- PR(s): none.
- Commit(s): checkpoint 26 on `fix_commands`; see execution history.
- Related ADR(s): ADR-0277; ADR-0023 remains the pipeline replay decision.
- Related docs: `docs/historical-analysis.md`, `cmd/analyze_distance3/`,
  `cmd/simulate_threshold/`, `cmd/analyze1c/`, `cmd/inspect_decisions/`.

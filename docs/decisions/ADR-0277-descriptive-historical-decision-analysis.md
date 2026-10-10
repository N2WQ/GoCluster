# ADR-0277: Descriptive Historical Decision Analysis

- Status: Accepted
- Date: 2026-10-10
- Decision Origin: Troubleshooting chat

## Context

Approved v3 retains historical decision commands while repairing trust defects.
The distance-3 tool's fixed scientific conclusions do not follow from its data.
A stored confidence score is not independently labeled accuracy; passing a few
scalar gates does not replay the complete correction model. ADR-0023 still
selects `rbn_replay` for pipeline evidence generation. Retained legacy readers
inspect existing data and do not supersede that path.

## Decision

- Report input-derived distance-3 observations. Recognize case-only applied and
  rejected labels; NULL/empty/other values are unknown. Name denominators;
  applied-only summaries exclude other outcomes. Empty statistics and
  zero-denominator ratios are unavailable.
- Preserve the six scalar scenarios with explicit hypothetical bases/extras.
  Report retained/removed/added recognized sets and signed net change; show
  unknown passes separately. Do not call these current settings or model replay,
  or infer accuracy, calibration, reliability or optimality.
- Fail on loading and output errors. Failed loads publish no partial counts.
  Own rows and database until explicit retirement.
- Preserve schema, database flag/default, command-local lifetime and production
  correction behavior. Synthetic fixtures do not prove database provenance.
- This record governs the repaired distance-3 reader. Other historical readers
  receive their own approved qualification before repaired claims are made.

## Alternatives considered

1. Retain fixed advice: unsupported by input and inconsistent with trust goals.
2. Infer verdicts from stored scores: lacks independent outcomes/calibration.
3. Remove tools or build a shared replay framework: exceeds approved scope.

## Consequences

### Benefits
- Truthful status, visible populations and explicit scenario assumptions.
- Useful historical observations with supportable claims.

### Risks
- Input validity/provenance remain operator responsibilities.
- Complete-row loading uses memory proportional to selected input cardinality.

### Operational impact
- Existing flags/defaults remain; report wording changes deliberately.
- Use status and causal stderr. Late stdout failure can leave partial output.
- No independent empirical accuracy/calibration review occurred for this reader;
  lead-owned arithmetic/claim checks establish only its descriptive contract.

## Links
- Related issues/PRs/commits: approved v3 checkpoint 26.
- Related tests: `cmd/analyze_distance3/` tests and execution qualification.
- Related docs: `docs/historical-analysis.md`, `docs/command-fix-execution.md`.
- Related TSRs: `docs/troubleshooting/TSR-0075-historical-analysis-claims-and-failures.md`.
- Supersedes / superseded by: none; clarifies retained-reader boundaries alongside
  ADR-0023 without changing pipeline replay.

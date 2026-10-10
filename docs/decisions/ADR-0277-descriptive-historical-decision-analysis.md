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
- Extend the descriptive contract to `simulate_threshold`: preserve its selected
  distances1..3, flags/defaults and assumed distance-3 extras. Report all stored
  baseline tuple frequencies, not a first-row common configuration. Retained
  applications plus added rejections define the recognized passing count
  (original applied minus removed plus added); net is added minus removed.
  Unknown passes remain separate. Name all-selected/recognized/applied denominators; zero ratios are
  unavailable. Integer overflow cannot make an impossible extra gate permissive.
- Extend the contract to `analyze1c`: preserve its four-column legacy projection
  and all-distance population. Applied-only mean/median use exact accumulation
  before final approximate floating presentation. Empty summaries are N/A;
  rejection reasons exclude unknown outcomes and retain their missing bucket.
  Report no correlation, calibration or reliability evaluation.
- Extend the contract to `inspect_decisions`: consistent case-only outcome
  partitions, binary raw-label grouping and recognized-only capped samples.
  Preserve distinct projections and reason fallbacks. All four selections must
  load before reporting; they retain independent statement snapshots and do not
  promise chronology, input validity or correction accuracy.
- This record governs the repaired distance-3, threshold, distance-summary and
  inspection readers. Other readers
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
- Related issues/PRs/commits: approved v3 checkpoints 26/27/28b/29.
- Related tests: `cmd/analyze_distance3/`, `cmd/simulate_threshold/`,
  `cmd/analyze1c/`, `cmd/inspect_decisions/` and execution qualification.
- Related docs: `docs/historical-analysis.md`, `docs/command-fix-execution.md`.
- Related TSRs: `docs/troubleshooting/TSR-0075-historical-analysis-claims-and-failures.md`.
- Supersedes / superseded by: none; clarifies retained-reader boundaries alongside
  ADR-0023 without changing pipeline replay.

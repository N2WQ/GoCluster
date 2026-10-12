# ADR-0298: Consistent Resolver Support Basis

- Status: Accepted
- Date: 2026-10-12
- Decision Origin: Troubleshooting chat

## Context

Call percentages previously changed to receiver coverage when the call's
weighted numerator was zero. Actual all-zero populations also gained an
invented unit-weight denominator at publication. Ranking, displayed coverage
and waiting-decoder support could consequently describe different evidence.
The owner selected consistent weighting and no support from all-zero weights.

## Decision

Percentages describe the emitted call's eligible distinct receiver evidence.
Use the weighted numerator and unique-receiver weighted denominator for weighted
populations. A known zero numerator and a positive fraction floored to zero
both provide available 0%. A weighted population with total zero is unavailable.
Only deliberately unweighted evidence provides raw receiver coverage. Missing
eligible call evidence and protected loss remain unavailable. Ranked raw-zero
rows cannot borrow stale winner support. Keep integer flooring and glyph rules.

Carry a compact scalar basis through owner/control publication, Compact, fresh
projection, neighborhood synthesis and temporal copies. Unset positive weighted
snapshots retain compatibility. Unset zero, contradictory raw metadata and mixed
bases fail closed. Explicit unavailable remains absorbing even with positive
totals. Real publishers declare weighted basis at zero too. Exclude the marker
from JSON; introduce no config switch, registry or persisted/artifact field.
Missing reliability assets/entries keep the existing numerical weight 1 default.

All-zero weighted observations add zero decoder support for present, absent and
subject candidates. Unavailable basis adds no support authority. Preserve
positive-total smoothing, missing-snapshot subject fallback, transitions, priors
and existing loss abstention. Positive-weight loss emission remains unchanged;
loss still blocks current percentage/correction authority. Current release
evidence must authorize a decoded proposal. Classify final neighborhood ranks
on their own aggregate basis, after unique-receiver union is complete.

Known zero display alone does not authorize family hiding. Existing positive
raw support and positive weighted numerator requirements remain. A positive
numerator rounded to 0% remains usable under existing ties. Preserve C/B emitted
identity precedence, independent historical recognition and CustomSCP V-only
learning. A2 zero-weight versus B1 unit-weight minimum-report rejection emits
A/P0 rather than raw A66. Keep network/archive formats and thresholds unchanged.

## Alternatives considered

- Raw fallback would make the percentage change meaning when weights reject
  support. The owner selected consistent weighting.
- Keeping all-zero decoder smoothing would leave residual candidate/subject
  support. The owner explicitly selected zero contribution.
- A runtime raw switch or side registry adds policy/state absent from this scope.

## Consequences and operational impact

Displayed zero agrees with candidate weighting. All-zero low-cardinality decoder
scores change intentionally; model priors and transitions remain independent.
Support coverage is a method contract, not calibrated call accuracy. Snapshot
ownership/capacities, schema/defaults, output witnesses and resource bounds stay
unchanged. Local contract and allocation checks do not establish field accuracy,
production throughput or latency limits.

## Links

- Refines [ADR-0027](ADR-0027-resolver-reliability-weighted-voting.md) and
  [ADR-0293](ADR-0293-correction-decision-diagnostics.md).
- Preserves [ADR-0295](ADR-0295-fresh-admitted-family-display.md) family admission
  and [ADR-0297](ADR-0297-core-accepted-observation-learning.md) learning policy.
- [TSR-0091](../troubleshooting/TSR-0091-resolver-zero-support-basis.md).
- [Authority](../call-correction-followup-scope-ledger-v2.md) and
  [execution evidence](../call-correction-followup-execution.md).

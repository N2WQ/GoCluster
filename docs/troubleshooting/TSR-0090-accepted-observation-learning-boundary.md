# TSR-0090 - Accepted Observation Learning Boundary

Status: Monitoring
Date Opened: 2026-10-11
Date Resolved: n/a
Owner: Codex
Technical Area: call history, WhoSpotsMe and output acceptance
Trigger Source: Chat request
Led To ADR(s): ADR-0297
Tags: learning, acceptance, stabilizer, output, shutdown

## RCA Summary

- What happened: immediate stabilized learning depended on secondary lane
  eligibility, while delayed and non-stabilized paths used core acceptance.
- Why: an output-lane denial was passed as though it meant pending stabilizer
  ownership. Receiver stores can hide duplicate recording by overwriting rows.
- What fixed it: record once at each resolved core-acceptance path before output
  gates, using one acceptance clock and retaining every store's eligibility.
- How we know: actual pipeline/scheduler/shutdown fixtures use qualified positive
  controls and WhoSpotsMe occurrence counts, with history and delivered readbacks.
- Operator/support answer: thinning, family hiding and output queue pressure do
  not change what accepted eligible receiver observations teach the method.

## Triggering Request

The owner selected core-accepted learning, independently of secondary output,
with pending/suppressed/discarded work excluded and CustomSCP V-only preserved.
Approved authority: [S3 ledger](../call-correction-followup-scope-ledger-v2.md).

## Symptoms and Impact

Identical accepted evidence could produce different future history depending on
output pressure. A distinct-receiver readback could remain one even if an
observation was recorded twice. A zero count without eligible country/continent
metadata could also hide a broken negative-path assertion.

## Timeline

1. Source review found inconsistent admission boundaries across delivery paths.
2. The owner selected accepted observations as history's meaning.
3. Postapproval test review required positive occurrence controls and actual
   shutdown transfer/discard distinctions before implementation verification.

## Hypotheses and Tests

1. Output denial means unresolved ownership: rejected; secondary thinning is an
   output decision after core acceptance.
2. One distinct receiver proves one learning event: rejected; overwritten rows
   conceal duplicates. WhoSpotsMe independently counts accepted occurrences.
3. Zero WhoSpotsMe proves no learning: only after the same configuration proves
   a successful one-record control with valid ADIF/continent and query clock.
4. No work may learn after stop: rejected; transferred envelopes and forced
   releases may still reach acceptance under existing shutdown ownership.
5. V can demonstrate natural overflow: rejected; V/C bypass holding. Compare
   store-eligible deltas and independently prove delayed promotion to V.

## Findings

- Root cause: immediate stabilized admission conflated output eligibility with
  pending work. Other accepted paths did not use that output restriction.
- Durable lesson: define the evidence boundary separately from routing; prove
  store eligibility before interpreting negative results, and use a count that
  receiver deduplication cannot conceal.

## Decision Linkage

[ADR-0297](../decisions/ADR-0297-core-accepted-observation-learning.md) governs
learning. Family display witnesses remain governed by
[ADR-0295](../decisions/ADR-0295-fresh-admitted-family-display.md).

## Verification and Monitoring

[Execution evidence](../call-correction-followup-execution.md) records actual
results and checkpoint status. Required evidence covers all accepted paths,
pending/reheld/suppressed/discarded work, actual transfers/forced releases,
secondary and queue gates, final identity and store/disk safeguards. This proves
the selected method contract, not field accuracy or production capacity.

## References

- Code: `internal/cluster/output_pipeline_delivery.go` and existing recorder
  eligibility in `bootstrap.go`/`ft_confidence_runtime.go`.
- Tests: accepted-learning path, hold, output, identity and shutdown fixtures.

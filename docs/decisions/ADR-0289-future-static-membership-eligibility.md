# ADR-0289: Future Static Membership Eligibility

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Design

## Context

Custom SCP static recognition and eligible call counts checked the lower
retention horizon without excluding future membership timestamps. A retained
future marker could therefore supply the local S floor before its observation
time. Historical receiver queries already distinguish retention from eligibility.

## Decision

Static membership qualifies only in the inclusive interval from the configured
static-horizon cutoff through the query's stored second. Preserve `StaticContains`
as a process-clock lookup and `StaticCallCount(now)` as an explicit-clock count.
Keep future records under existing retention, refresh, cleanup and persisted
encoding. Retained `StaticCalls` remains a cardinality diagnostic, not eligible
recognition. A private clock-taking helper allows deterministic membership checks
without changing the public runtime or historical replay clock contract.

The local S floor may still qualify through independent exact or family history
while static membership is future. Peer publication preserves original fields.

## Alternatives considered

- Deleting future markers conflates query eligibility with storage retention.
- Changing public caller clocks would broaden this patch into replay clock policy.
- Lower-bound-only recognition authorizes evidence before its stored time.

## Consequences

Future-only static recognition now waits for clock catch-up. No storage format,
retention, admission, cleanup, model or percentage rule changes. This establishes
timestamp eligibility, not RF episode association or calibrated call accuracy.

## Links

- Refines [ADR-0285](ADR-0285-individual-historical-receiver-freshness.md).
- Approved scope: `docs/call-correction-evidence-scope-ledger-v2.md`, B1.5.
- Validation: `docs/call-correction-evidence-execution.md`.

# TSR-0081 - Temporal Saved Authority

Status: Monitoring
Date Opened: 2026-10-10
Date Resolved: n/a
Owner: Codex
Technical Area: temporal correction, release evidence, confirmed history
Trigger Source: Chat request
Led To ADR(s): ADR-0282
Tags: correction, temporal, freshness, delivery, bounded history

## RCA Summary

- What happened: saved expired support or a newer split still produced corrected
  identities, and rejected proposals polluted confirmed path history.
- Why: release trusted a saved selection; decoding wrote history before gates.
- What fixed it: fresh immutable projection, ordinary gates and mutation-owned
  confirmation, independently bounded history and request-owned handoff accounting.
- How we know: compiled baseline counterexamples plus actual TCP/DIAG, PC61,
  sealed buffer and reopened archive regressions; checkpoint results are recorded
  in the linked execution record.
- Operator/support answer: proposed, accepted and handed-off are distinct;
  fresh eligible evidence does not prove the original RF transmission episode.

## Triggering Request

- Request date: 2026-10-10
- Request summary: five bounded tested method patches, user Approved v1.
- Request reference: approved method execution record.

## Symptoms and Impact

Expired unswept A3/B1 evidence delivered A/C/75%; a newer A3/B2 split was also
bypassed. CTY-rejected, invalid and unchanged proposals counted as committed and
seeded later path switches. Replay forced EOF could strand future-due requests.

## Timeline

1. Baseline 37a9d0d compiled regressions reproduced these failures before repair.
2. P3 separated proposals from accepted identity mutations and fresh authority.
3. Final checkpoint and review results belong to the execution record.

## Hypotheses and Tests

1. Old saved authority reaches users: supported by connected baseline sinks.
2. Rejected/unchanged proposals seed history: supported by counters and the next
   accepted correction on the same decoder/key.
3. Reopened state alone proves fresh eligibility: rejected; unswept immutable
   publications require filtering at the release clock.

One replay C4 fixture initially used another bucket and failed to publish past
evaluation cadence; those fixture failures are excluded from defect evidence. An initial unavailable DIAG
expectation omitted the existing `--%` token; the fixture was corrected without
changing output formatting.

## Findings

- Root cause: decoder-time saved support/history crossed the release authority
  boundary without current eligibility or successful correction acceptance.

## Decision Linkage

- ADR created/updated: ADR-0282.
- Decision delta summary: fresh read-only projection and accepted-history owner.
- Contract/behavior changes: stale proposals lose authority, current ordinary
  evidence can recover, forced replay retirement consumes all pending holds.

## Verification and Monitoring

- Validation steps run: exact commands/results in the execution record.
- Monitor proposal/accepted/handoff counters, history cardinality/evictions and
  temporal overflow; handoff is an attempt, not a receipt guarantee.
- Roll back for evidence-integrity, concurrency, retention or measured capacity
  regressions. Local checks establish no RF accuracy or calibration claim.

## References

- Related ADR(s): [ADR-0282](../decisions/ADR-0282-temporal-release-evidence-authority.md)
- Related docs: [execution record](../call-correction-method-execution.md)
- Related tests: runtime/replay temporal freshness and confirmation regressions.

# TSR-0085 - Temporal Event Key Retirement

Status: Monitoring
Date Opened: 2026-10-11
Date Resolved: n/a
Owner: Codex
Technical Area: temporal decoder request/event ownership and capacity
Trigger Source: Chat request
Led To ADR(s): none
Tags: temporal, event eviction, bounded state, delivery

## RCA Summary

- What happened: zero pending requests could retain an event key and refuse a
  new signal at the active-key cap.
- Why: the `event_evicted` terminal branch deleted its request but omitted the
  last-owner key reclamation already present in ordinary terminal paths.
- What fixed it: consistent terminal retirement under the existing decoder
  mutex, reclaiming event state only after its final request leaves.
- How we know: the baseline-compatible regression failed with `pending=0
  keys=1` for each fallback action. Repair and delivered-output checkpoint
  results are recorded in the execution record.
- Operator/support answer: active-key refusal can indicate stranded event
  ownership rather than actual pending load; accepted identity history has a
  separate bound and is not cleared by this repair.

## Triggering Request

- Request date: 2026-10-11.
- Request: approved delivery scope v1, T1 temporal retirement repair.
- Reference: [scope ledger](../call-correction-delivery-scope-ledger-v1.md).

## Symptoms and Impact

With one event per key, an older tied request defers; a newer observation evicts
its event, completes first, and leaves the older request owning the key. When
that older request later finishes through `event_evicted`, the baseline loses
its request but keeps the key. Under `abstain`, subsequent capacity refusal can
deliver an unchanged identity instead of holding it for temporal correction.

## Timeline

1. Baseline `42f0e0f`: deterministic decoder regressions reproduced stranded
   key state under fallback_resolver, abstain and bypass.
2. T1: repair and final evidence follow the ordered execution checkpoint.

## Hypotheses and Tests

1. Event eviction strands final key ownership: supported by the baseline
   assertion, not a compilation or fixture failure.
2. Every completion order leaks: rejected; retiring the older request first
   retains the newer owner and ordinary completion reclaims the key.
3. Clearing confirmed history is necessary: rejected by its separate ownership
   contract in [ADR-0282](../decisions/ADR-0282-temporal-release-evidence-authority.md).

## Findings

- Root cause: `event_evicted` omitted the last-owner key deletion used by
  ordinary terminal branches. Each branch needs the same request/event-state
  deletion invariant.
- The bounded pending scan is sufficient; no side index or collector is needed.

## Decision Linkage

- ADR created/updated: none; this restores existing ownership semantics.
- Contract changes: terminal event state no longer consumes capacity after all
  requests finish. Accepted identity history and confidence rules are unchanged.

## Verification and Monitoring

See [execution evidence](../call-correction-delivery-execution.md) for actual
commands, baseline failures, delivery/storage readbacks and final results.
Monitor temporal overflow, pending load and accepted-history cardinality.
Rollback for lost pending ownership, altered confirmations or capacity regression.
These tests do not establish RF accuracy or original-transmission association.

## References

- Related ADR: [ADR-0282](../decisions/ADR-0282-temporal-release-evidence-authority.md).
- Related tests: temporal_retirement_test.go, runtime/replay reclaim delivery.

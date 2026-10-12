# TSR-0089 - Legacy History Clock and Capacity

Status: Monitoring
Date Opened: 2026-10-11
Date Resolved: n/a
Owner: Codex
Technical Area: legacy/FT history and replay maintenance
Trigger Source: Chat request
Led To ADR(s): ADR-0296
Tags: history, timestamp, replay, capacity, cleanup

## RCA Summary

- What happened: bounded legacy history could satisfy impossible minima, count
  future rows, or let future admission replace valid evidence. Replay's wall
  cleanup could delete rows still eligible at its historical evaluation clock.
- Why: Boolean minima were clamped to capacity; queries lacked an upper bound;
  admission and cleanup did not share the applicable owner clock.
- What fixed it: refuse future admission before mutation, preserve requested
  minima, query an inclusive bounded interval, validate actual consumed gates
  before owner creation, and run replay maintenance at evaluation time.
- How we know: physical-state checks precede queries; actual startup/caller and
  delivered-output fixtures are mapped to results in the execution record.
- Operator/support answer: choose receiver requirements the active owner can
  hold. A future report cannot supply legacy/FT support or replace valid rows.

## Triggering Request

Implement approved follow-up S2 with future rejection and startup failure rather
than weakened requirements. The owner also required replay maintenance to use
the evaluation clock and exact zero-source timestamp normalization.

## Symptoms and Impact

Support and S recognition could disagree with accepted receiver evidence.
Historical replay lasting past a maintenance interval could lose support solely
because machine time was later. CustomSCP has separate timestamp/storage rules;
these findings do not rewrite them or establish call accuracy.

## Timeline

1. Feedback review identified legacy qualification/admission gaps and separate FT
   use of the same store.
2. Source review found replay starting the wall cleanup worker.
3. Approved S2 adds explicit admission/maintenance clocks and actual-owner fences.

## Hypotheses and Tests

1. A filled store can satisfy an above-cap request: rejected; partial counts are
   evidence, but cannot meet a larger Boolean minimum.
2. Query filtering alone protects valid history: rejected; a future admission
   can overwrite or evict valid evidence before a query.
3. Event-time admission/query implies replay clock integrity: rejected; untouched
   keys must also survive maintenance at that event clock.
4. An immediate count proves zero-time normalization: rejected; exact expiry at
   admissionNow+window and one nanosecond later distinguishes wrong clocks.
5. Every configured minimum applies everywhere: rejected; replay lacks a support
   floor, reveng lacks the live stabilizer, and FT has its own 16-receiver owner.

## Findings

- Root cause: legacy qualification lacked an upper timestamp bound and weakened
  above-cap minima. Admission could change valid history before detecting future
  time, while replay started maintenance against machine time.

Reject before state mutation; establish constructor bounds and actual consumers
before validating requirements. Inspect retained primary/reference state before
queries, which may prune and conceal a broken maintenance path. Raw constructor
sentinels and explicit invalid YAML values have different existing contracts.

## Decision Linkage

- [ADR-0296](../decisions/ADR-0296-legacy-history-clock-and-capacity.md) governs
  legacy/FT admission, capacity and replay maintenance.
- [ADR-0288](../decisions/ADR-0288-historical-requirements-respect-storage-capacity.md)
  remains the separate CustomSCP capacity decision.

## Verification and Monitoring

See [execution evidence](../call-correction-followup-execution.md) for observed
checks, failures and checkpoint status. Tests cover before/after-wall clocks,
untouched maintenance, upper/lower nanosecond boundaries, capacity, state coupling
and actual consumers. No production-load or field-accuracy claim follows.

## References

- Scope: [S2 ledger](../call-correction-followup-scope-ledger-v2.md).
- Code: `spot/recent_band.go`, `config/legacy_history_capacity.go`, runtime and
  replay/reveng history-owner construction and recording.

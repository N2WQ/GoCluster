# ADR-0295: Fresh Evidence and Admitted Lanes for Family Display

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Troubleshooting chat

## Context

Family display hiding previously preferred specificity or cached raw support,
including unavailable or contested evidence. Its cache recorded attempted
publication before the bounded broadcast queue could refuse it. One timestamp
and frequency also extended protection across delivery policies that had never
been offered the stronger call.

The owner selected same-population fresh percentages, a slash preference only
for whole-percent slash/base ties, and separately admitted FAST/MED/SLOW history
in the approved delivery ledger.

## Decision

Use one existing final-literal-anchored eligible resolver selection at each
family arbitration. Require usable authority, positive supports and a common
weighted denominator; raw shares are usable only without a weighted total.
Missing identity, incompatible score basis, split, uncertainty or known loss
provides no hiding authority. Preserve dissent, receiver union and existing
whole-percent arithmetic. A separate usable stronger rival remains sufficient
even when another comparison is tied or unavailable.

Higher percentage wins irrespective of specificity. A usable whole-percent
slash/base tie favors the slash call; other ties show both. This is the owner's
display preference, not evidence that a base call was partially copied.

Serialize fresh selection, comparison, nonblocking broadcast admission and
history commitment under the existing family mutex. `TryBroadcastSpotOwned`
reports queue admission only. Existing void broadcast APIs remain supported;
drops, markers and attempted temporal handoffs retain their contracts.

Record only successful ordinary lane handoffs. Each existing capped entry owns
three compact literal/time/frequency witnesses, with no spot, snapshot or
receiver rows retained. FAST refreshes only FAST. A witness protects its lane
through the inclusive window boundary. Hidden/refused handoffs do not refresh,
insert or delete competing witnesses; expiry and global LRU eviction still
remove protection. Repeated calls reevaluate competitors. Retain admitted
competitors until expiry or eviction so changed evidence can reevaluate them.

Family hiding affects ordinary telnet display only. Partial masks retain the
existing single broadcast SELF delivery. With no ordinary lane, use direct
SELF; queue refusal adds no direct SELF fallback. Shared ring/history and MED
archive/original-peer gates, actual holds and timeout policy remain intact.

Existing relation, mode, truncation, frequency, cap and switch configuration
remains active; schema, defaults and configured values do not change.

## Alternatives considered

1. Prefer specificity or saved percentages: rejected because these can hide a
   freshly stronger base/shorter call or compare different populations.
2. Commit attempted output or one timestamp for all policies: rejected because
   queue refusal and a FAST-only offer cannot establish MED/SLOW protection.
3. Track client receipt or wait for more reports: deferred; queue admission is
   the selected bounded witness, and earlier TCP output cannot be retracted.

## Consequences

### Benefits

- Visible hiding decisions have fresh comparable evidence and an actual
  earlier ordinary queue offer for the affected delivery policy.
- Saved history and original peer publication survive display hiding.
- Bounded ownership and deterministic tests cover refusal, expiry and churn.

### Risks

- Fresh reports within a key/window do not prove the same transmission episode.
- Support shares, including overlap, are not correctness probabilities.
- Queue admission does not prove client receipt; eviction conservatively shows
  more output. Dense-history locking costs require operational observation.

### Operational impact

No command, wire, archive or persistence format changes. Existing switches
remain rollback controls. The execution record reports local validation and
cost measurements; production load and RF accuracy remain unverified.

## Links

- [ADR-0297](ADR-0297-core-accepted-observation-learning.md) governs learned
  call/FT history and WhoSpotsMe before output eligibility. Successful family
  lane witnesses and saved-output gates in this record remain accepted.
- Related tests: `internal/cluster/telnet_family_suppressor_test.go`,
  `internal/cluster/family_admission_delivery_test.go`,
  `internal/cluster/family_admission_qualification_test.go`,
  `telnet/server_spot_snapshot_test.go`.
- Related docs: [scope ledger](../call-correction-delivery-scope-ledger-v1.md),
  [execution](../call-correction-delivery-execution.md).
- Related TSRs: [TSR-0087](../troubleshooting/TSR-0087-family-display-admission-authority.md),
  [TSR-0086](../troubleshooting/TSR-0086-family-delivery-destination-isolation.md).
- Partially supersedes the family display clauses of
  [ADR-0014](ADR-0014-call-correction-family-policy.md), edit-neighbor display
  clauses of [ADR-0033](ADR-0033-resolver-neighborhood-and-contested-edit-neighbor-rails.md),
  and clarifies display-only thinning after the shared fanout boundary in
  [ADR-0148](ADR-0148-post-stabilizer-shared-secondary-fanout.md).
  Correction, neighborhood/stabilizer policies and shared secondary gates
  remain accepted.

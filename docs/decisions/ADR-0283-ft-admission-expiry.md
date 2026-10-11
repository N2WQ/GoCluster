# ADR-0283: FT Admission Expiry

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Troubleshooting chat

## Context

A report could join an already expired FT burst when admission ran before the
next timer drain. A late third receiver then promoted the old two-receiver P
output to V. Drain already treated equality as expired; admission did not.

## Decision

Before an FT observation looks up its key or checks capacity, retire every group
due at the arrival clock. Eligibility ends at
`min(last_seen + quiet_gap, first_seen + hard_cap)`; equality and later arrivals
start a new generation. Source timestamps do not replace the arrival clock.

Use the existing due/sequence heap, generation validation and pending accounting.
The controller returns detached groups on every admission/decline path. The
single output owner synchronously releases them through the same remaining
stages used by timer and shutdown retirement. No worker or retired-group backlog
is added. Reclaim expired capacity before checking the unchanged live limits.
Preserve reusable scratch capacity and clear retired key references.

Keep FT2/FT4/FT8 taxonomy, timing defaults and operator settings, count thresholds,
support floors, fail-open behavior and downstream formats unchanged. A later
burst may receive the existing recent-band S floor; it does not borrow the old
burst's receiver count or a resolver percentage.

## Alternatives considered

- Rely on timer draining before admission: does not enforce eligibility when
  processing crosses a deadline or draining is delayed.
- Retire only the matching key: leaves unrelated due state consuming capacity
  and stale heap generations; reuse the existing all-due traversal instead.
- Queue retired groups to another worker: unnecessary ownership and shutdown
  complexity for the existing single-owner pipeline.

## Consequences

Late reports cannot upgrade already expired groups. One arrival can synchronously
release the bounded pending population; capped cost is measured in the execution
record, without a production-capacity claim. Arrival bursts still do not prove
that reports describe one RF transmission episode. Wider association is deferred.

## Links

- Refines [ADR-0060](ADR-0060-source-aware-ft-burst-clustering.md)
- Related [TSR-0082](../troubleshooting/TSR-0082-ft-expired-burst-admission.md)
- [Execution record](../call-correction-method-execution.md)
- Tests: `internal/cluster/ft_expiry_observe_test.go`,
  `internal/cluster/ft_expiry_delivery_test.go`,
  `internal/cluster/ft_expiry_bench_test.go`

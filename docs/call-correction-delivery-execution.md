# Call correction delivery execution

Authority: exact `Approved v1` on 2026-10-11 for
[the scope ledger](call-correction-delivery-scope-ledger-v1.md).
Baseline: `8890e79`; branch: `fix_temporal_family_delivery`.
Unrelated profiling changes were present at branch creation and remain unowned.

## T0 - toxicity summary synchronization

Implemented the test-only worker join before exact terminal counters. The
targeted cluster/toxicity ownership controls passed; the summary assertion also
passed 1,000 ordinary and 1,000 race repetitions. No production classifier
change.

Checkpoint passed on 2026-10-11:

- Stable Windows runner: all 70 packages ordinary (561.1s) and race (943.2s).
- `go vet ./...`, `staticcheck ./...`, and the configured golangci-lint lane.
- Connected CustomSCP producer/consumer regression, ordinary and race; matching
  run identities and independently reopened storage reached actual delivery.
- Troubleshooting-record checker and direct final test/TSR diff review.

Logs: `.tmp/temporal-family-delivery/T0/`; connected ordinary run
`1a4def4f29224005a327c26afcbe2a9b`, race run
`91de346f0eab46ec970a832889dbc636`. The terminal assertion checks exact zero
pending/backlog after the join; receiving/applying still precedes shutdown.
Support documentation impact: existing TSR-0068 follow-up; no runtime or
operator contract change. Codemap generation/check is the final write/check.

## Postapproval test-strategy review

The read-only review found no material scope gap. Its checker refinements are
accepted within v1: baseline counterexamples must compile against the defective
APIs; new admission-API checks follow reproduction; destination absence uses
FIFO barriers and admission/drop observations rather than socket quiet time
alone; concurrent arbitration is checked against actual admission order and
direct inspection of the mutex span. These are validation requirements, not
additional production hooks or behavior.

## Remaining ordered checkpoints

T1 temporal retirement, F1 destination isolation, F2 fresh per-lane family
arbitration remain pending. No implementation of a later checkpoint precedes
the preceding checkpoint's complete validation and commit.

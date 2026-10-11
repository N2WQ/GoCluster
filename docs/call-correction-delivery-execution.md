# Call correction delivery execution

Authority: exact `Approved v1` on 2026-10-11 for
[the scope ledger](call-correction-delivery-scope-ledger-v1.md).
Baseline: `8890e79`; branch: `fix_temporal_family_delivery`.
Unrelated profiling changes were present at branch creation and remain unowned.

## T0 - toxicity summary synchronization

Commit: `42f0e0f`.

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

T0 and T1 are complete. F1 destination isolation and F2 fresh per-lane family
arbitration remain pending. No implementation of a later checkpoint precedes
the preceding checkpoint's complete validation and commit.

## T1 - temporal final-request retirement

Baseline `42f0e0f`: `TestTemporalRetirementReclaimsFinalEvictedRequest` compiled
and failed for fallback_resolver, abstain and bypass when the newer request
completed first. Each asserted `terminal ownership leaked: pending=0 keys=1`.
The reverse completion order passed. Evidence:
`.tmp/temporal-family-delivery/T1-decoder-baseline.log`.

The unchanged runtime/replay baseline also failed. Actual TCP emitted
K1ABD/P/25%, sealed ring and raw persisted bytes retained that identity/glyph,
and reopened archive recovered K1ABD/P. No fresh request was held; the configured
abstain counter incremented. Original-input PC61 K1ABD was preserved. Replay
likewise emitted K1ABD/P without admission. Logs:
`T1-delivery-baseline.log`, `T1-replay-baseline.log` in the same directory.

Implementation: every known terminal request now calls `retireRequestLocked`
under `d.mu`; its existing bounded pending scan reclaims event-key state only
when the final same-key owner leaves. Defer preserves ownership. Separately
bounded accepted identity history is untouched.

Targeted ordinary/race checks passed across correctionflow, cluster and replay:
both retirement orders, fallback actions, force/retry, missing state, key churn,
concurrent requests, confirmation controls and real delivered/replayed output.
The repaired runtime positively held the new key, received an earlier FIFO
barrier, retained ownership before its exact deadline, then emitted K1ABC/C/75%
once. Raw bytes were compared against literal version-six fields before decoding;
reopen recovery and original PC61 were separate checks.

Three guarded terminal-scan benchmark samples: one pending request 244-249ns,
25,000 pending requests 305-312us; both 0 B/op and 0 allocs/op. The cap sample
forces a full other-key scan, not decoder scoring or production throughput.
Evidence: `T1-bench.log`.

Complete T1 checkpoint passed: all 70 packages ordinary (564.3s) and race
(945.8s), vet, Staticcheck, configured lint, and connected CustomSCP ordinary
and race. Logs: `.tmp/temporal-family-delivery/T1/`; connected run identities
`8884fa734f4740749c63f3641d85f523` and
`7b866612e88e47e0bbe3deb1c5e7ac2a`. The post-review concurrent retirement test
also passed ordinary/race (`T1-post-review.log`, `T1-post-review-race.log`).

Support impact: TSR-0085 and source-map routing. Decision disposition: restore
existing request/event ownership; no new ADR or accepted-history policy.

Executed targeted commands (then the same runner command with `-Race`):

```powershell
& .\scripts\test-windows.ps1 -Packages @('./internal/correctionflow','./internal/cluster','./cmd/rbn_replay') -Run 'Temporal.*(Retirement|Reclaim|Confirm|Proposal)' -Timeout 180s
```

Baseline commands used the same runner, ordinary build, 180s timeout, selecting
the exact tests `^TestTemporalRetirementReclaimsFinalEvictedRequest$`,
`^TestRuntimeTemporalReclaimDelivered$` and
`^TestReplayTemporalReclaimFreshAdmission$` in their respective packages.

The benchmark compiled with `go test -c -o` into the canonical correctionflow
`package.test.exe`, acquired the shared runner lock, and executed directly from
`internal/correctionflow` with these arguments:

```text
-test.run=^$ -test.bench=^BenchmarkTemporalRetirementPendingScan$ -test.benchmem -test.benchtime=300ms -test.count=3 -test.paniconexit0
```

Read-only Go quality review found no material defect. Its two refinements are
accepted: exact command selection is recorded above, and concurrent workers now
report errors and return without calling the test goroutine's `FailNow` helper.
The affected targeted check reran and passed after the full checkpoint.

# TSR-0101 - Replay Deadline Scheduling

Status: Monitoring
Date Opened: 2026-10-16
Date Resolved: n/a
Owner: Codex
Technical Area: cmd/rbn_replay event scheduling and temporal retirement
Trigger Source: Chat request
Led To ADR(s): none
Tags: replay, event time, temporal, deadlines, EOF

## RCA Summary

- What happened: unrelated traffic could postpone a held replay decision until
  evidence expired, changing whether the target call was corrected.
- Why: eligible rows drained holds only after advancing all earlier samples;
  skipped classes/modes did not drain them. EOF forced holds before finishing
  the remaining earlier samples.
- What fixed it: merge existing sample and temporal deadlines chronologically
  in one private runner helper, using each event's actual timestamp.
- How we know: actual CSV-loop regressions fail on the unchanged runner and
  pass after the repair; literal target correction and timestamp are required.
- Operator/support answer: rerun affected historical evaluations with the
  updated replay tool. Corrections, latency metrics and interval values may
  change. No live executable/config change is needed for this offline repair.

## Triggering Request

- Request date: 2026-10-16.
- Request: repair replay deadlines with production changes limited to
  `cmd/rbn_replay/runner.go`, focused tests and supporting documentation.
- Authority: owner selected sample, release, incoming-row order and sent exact
  `Approved v1`. Later clarification limits traffic invariance to cases without
  capacity pressure or changed relevant evidence; correction-method changes
  remain outside scope.

## Symptoms and Impact

The fixture reports four K1ABC receivers at seconds 0 through 3, then K1ABD at
second 4. A common unrelated row at second 5 publishes all target evidence
before traffic diverges. The held target is due at second 19. The test uses a
15-second lag/max wait and 300-second evidence window; all-candidate hold scope
makes admission explicit instead of depending on uncertainty classification.
Later traffic uses a separate signal 60 kHz away and a separate receiver/call.
Process-idle time is fixed for reproducibility; loss/overflow controls must stay
zero. These are deterministic software fixtures, not independent RF truth.

On baseline `ce9434dd910c`, continuous eligible traffic corrected the target at
second 19. A one-minute gap corrected it at second 60. A ten-minute gap,
skipped-class traffic, non-correction traffic and no subsequent traffic produced
no target correction. A near-midnight hold also lost the correction at EOF.

## Timeline

1. 2026-10-16: inspected current runner, driver, decoder, fresh-release selector,
   historical-clock fixtures and ADR-0038/0282 contracts.
2. 2026-10-16: obtained ordering selection and exact v1 implementation approval.
3. 2026-10-16: captured clean behavioral baseline failures, then implemented and
   verified chronological scheduling. An initial test-file cleanup-order issue
   was fixed before collecting the clean baseline; it was not a replay defect.

## Hypotheses and Tests

| Hypothesis or failure | Falsifying check | Result |
| --- | --- | --- |
| Identical outcomes alone prove scheduling | `TestReplayTemporalDeadlineTrafficIndependence` requires literal K1ABC at second 19 for every traffic pattern, with no loss/overflow | Baseline fails; repair passes |
| Calling release directly proves integration | The same test streams real parsed CSV through `replayDay`, including no later traffic | Baseline fails; repair passes |
| Early release or a late row supplying its clock | `TestReplayTemporalDeadlineBoundaries` retains ownership at -1 ns and requires release at the deadline for exact/+1s rows | Baseline fails; repair passes |
| Equal-time row processing can run first | `TestReplayTemporalDeadlineEqualTimeOrder` observes pre-release sample counters and an incoming hold acquiring the one slot freed by release | Existing precedence preserved; later skipped-row retirement repaired |
| One queue read suffices after deferral | `TestReplayTemporalDeadlineDeferred` crosses lag/max wait within one gap and across two advances, requiring terminal latency of 5s | Repair passes in ordinary and race runs |
| EOF may backdate after forced evaluation | `TestReplayTemporalDeadlineNearMidnight` requires the real target correction at its scheduled post-midnight timestamp | Baseline fails; repair passes |
| Forced retirement may extend its clock | `TestReplayTemporalDeadlineFutureEOF` and existing future-EOF tests require retirement, the original clock and no duplicate output | Existing force behavior preserved |
| Deadline servicing changes sample cadence | `TestReplayTemporalDeadlineNoHolds` and CSV scenarios require exactly 1441 minute samples through dayEnd, with no post-day samples | Existing cadence preserved |

## Root cause or best current explanation

The scheduling defect preceded ordinary correction gates: later sampling could
prune the publication before a hold was examined. Backdating the subsequent
query cannot restore deleted evidence. The helper instead drains at the actual
deadline before later samples, re-reading the existing bounded heap after each
drain. A deferred request goes to max wait, where the existing decoder makes a
terminal decision; no additional timer, worker, state collection or resolver
step is introduced.

## Decision Linkage

- No new ADR: this restores scheduled event-time handling under
  [ADR-0038](../decisions/ADR-0038-fixed-lag-temporal-decoder-main-replay-parity.md).
  The owner explicitly retained existing equal-time precedence.
- [ADR-0282](../decisions/ADR-0282-temporal-release-evidence-authority.md) remains
  authoritative for fresh evidence, accepted mutation accounting and forced
  retirement at the unchanged EOF clock.
- Decoder scoring, hold/fallback rules, resolver stepping/publication, evidence
  windows, thresholds, capacities, configuration and artifact schemas remain.

## Verification and Monitoring

Targeted ordinary checks ran with:

```powershell
pwsh -NoProfile -File scripts/test-windows.ps1 -Packages ./cmd/rbn_replay -Run 'ReplayTemporal|ReplayLegacyClock' -Timeout 180s
```

The initial repaired checkpoint passed 59 cases with no skips. After adding the
single-gap deferral case, the complete selected lane passed on the final Go
state with Go 1.27.1, Staticcheck 2026.2.1 and golangci-lint 2.14.0:

| Command | Observed result |
| --- | --- |
| `pwsh -NoProfile -File scripts/test-windows.ps1` | PASS: 70 packages; 54 executed, 16 build-only; 10,815 passed cases, 93 skips |
| `pwsh -NoProfile -File scripts/test-windows.ps1 -Race` | PASS: same package/case counts and skips; no race report |
| `go vet ./...` | Exit 0 |
| `staticcheck ./...` | Exit 0 |
| `golangci-lint run ./... --config=.golangci.yaml` | Exit 0; 0 issues |

All new deadline cases passed in both full runs; none were skipped. The 93
skips comprise guarded subprocess helpers, opt-in external/live/performance/
profile/archive fixtures, and H3 mapping unavailable on Windows. They do not
establish external interoperability, live behavior, Linux/cgo mapping or
production resource performance. The source-aware comment and scope review was
lead-owned, not independent. No remaining implementation finding was identified.
Local diagnostic logs are under `.tmp/replay-deadline/`; they are not versioned
artifacts. Final source/documentation writes are followed by code-map generation,
freshness and documentation checks, then direct final-diff review.

Approved v1 traceability: scheduled servicing, exclusions and EOF are in
`runner.go` (`advanceReplayTime`, `processCSVRow`, `replayDay`); deadline,
ordering and retirement evidence is in `temporal_deadline_replay_test.go` and
the existing temporal/history tests. Support-agent docs impact: Required.
`customgpt/source-map.md` and `customgpt/troubleshooting-index.md` route to this
record and the updated `docs/rbn_replay.md` contract.

Monitor replay temporal decision latency and correction/interval changes when
rerunning historical inputs. Reopen this record for unrelated-traffic-dependent
decisions under unchanged relevant evidence and no capacity pressure, late
max-wait servicing, duplicate output or stranded holds. Field accuracy, full
live/replay parity and production-load performance are not established here.

## References

- Production: `cmd/rbn_replay/runner.go`.
- Tests: `cmd/rbn_replay/temporal_deadline_replay_test.go`, existing temporal and
  legacy-history clock fixtures in the same package.
- Contract: [historical replay](../rbn_replay.md).
- Prior evidence authority: [TSR-0081](TSR-0081-temporal-saved-authority.md).
- Prior retirement ownership: [TSR-0085](TSR-0085-temporal-event-key-retirement.md).
- Prior history clock: [TSR-0089](TSR-0089-legacy-history-clock-and-capacity.md).

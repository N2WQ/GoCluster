# TSR-0117 - CI Fixtures and Shadow Run Identity

Status: Monitoring
Date Opened: 2026-11-10
Date Resolved: n/a
Owner: Codex / repository owner
Technical Area: executable integration fixture, secondary-dedupe shadow reporting
Trigger Source: Chat request
Led To ADR(s): none
Tags: CI, Windows, Linux, fixture-isolation, counter-lifetime

## RCA Summary
- What happened: CI failed on Linux and Windows when the main-executable test could not load an ignored reliability table. Windows also reported that a replacement shadow engine inherited the preceding engine's interval counters.
- Why: the executable fixture converted operator data paths into absolute checkout paths instead of owning its input files. The reporter treated a wall-clock creation timestamp as a unique engine identity; equal timestamps allowed subtraction across different counter lifetimes, including unsigned underflow.
- What fixed it: the executable fixture writes one valid private temporary reliability table and uses it for all three configured paths. Enabled frequency-change engines receive immutable process-local run numbers; snapshots carry them, the reporter resets on a different number, and the logged run identifier includes the number.
- How we know: both failures appear in CI at `6897602` and its parent `471bb11`. A deterministic matching-timestamp regression failed against the old reporter with wrapped interval counters, then passed with the repair. The later hosted run and follow-on validation are recorded below.
- Operator/support answer: keep CI enabled. Neither failure was introduced by the workflow-only update. Shadow summaries now use `run=<UTC-start>-<number>`; the suffix distinguishes engines within one process and is not a globally unique cross-process identifier. Spot admission behavior and strict configured-asset validation are unchanged.

## Triggering Request
- Request date: 2026-11-10.
- Request summary: diagnose and fix the failed push-to-main CI without weakening its checks.
- Request reference: owner chat, exact `Approved v1` for the bounded CI repair after read-only diagnosis and scope challenge.

## Symptoms and Impact
- `TestV6MainExecutablePeerLoggingWithoutCompanion` exited before connecting to its local peer because `data/rbn/priors/spotter_reliability.txt` did not exist in the hosted checkout. The directory is ignored; its local presence hid the fixture dependency.
- `TestSecondaryShadowSummaryGoldenAndRestart` reported an inherited interval. Production shadow summaries can likewise mix lifetimes if two engines receive the same creation timestamp. This concerns diagnostic counters, not the forwarding decision.
- The failure in the early unit-test step prevented later Linux validation steps from executing. Repairing the known failures does not by itself establish that hosted CI is green.

## Timeline
1. 2026-11-10 - Compared failed current and parent push runs and their job logs; identified the same test failures on both revisions.
2. 2026-11-10 - Reproduced the Windows reset failure using an existing saved binary, then added a deterministic regression against current source. The latter retained old counters while forcing the prior snapshot's timestamp to match the replacement.
3. 2026-11-10 - Implemented the approved fixture and run-identity repairs. The 11 selected native Windows cases passed, including the real executable's peer/log assertion.

## Hypotheses and Tests
1. Hypothesis: workflow commit `6897602` introduced these failures.
   - Evidence: [current CI](https://github.com/N2WQ/GoCluster/actions/runs/42900668337) and [parent CI](https://github.com/N2WQ/GoCluster/actions/runs/42892321719) contain the same failures. The separate workflow-contract run succeeded.
   - Outcome: Rejected for these two failures.
2. Hypothesis: the executable test needs ignored operator data.
   - Evidence: its pipeline fixture resolved all three reliability paths into the checkout; `git ls-files -- data/rbn/priors` listed no files. Startup validates configured tables even when correction is disabled. The repaired fixture owns a valid `N0AAA 1` table without changing startup validation.
   - Outcome: Supported.
3. Hypothesis: equal start timestamps conceal an engine replacement.
   - Evidence: `ShadowStats` formerly exposed only `StartedAt` as lifetime identity and the reporter compared that value before unsigned subtraction. The forced-collision regression produced `base=18446744073709551615` and `suppressed=18446744073709551613` before the repair, rather than the specified replacement interval `base=1 suppressed=0` with all other fields zero.
   - Outcome: Supported by source and deterministic regression; the original CI failure did not capture its actual timestamps.

## Findings
- Root cause: the executable fixture depended on untracked local inputs, while the shadow reporter used a potentially colliding timestamp as counter-lifetime identity.
- The fixture must own the inputs it configures. Do not skip the executable test, ship private operator tables, clear production validation, or add a companion executable to make this check pass.
- A run number is allocated only in the enabled-engine constructor through an atomic compare-and-swap. The counter saturates with an explicit constructor error instead of wrapping. Off construction remains historical. Run numbers are never restored from disk.
- Retained-state bound: one process-wide scalar, one scalar per enabled engine, and copied values in the existing three prior snapshots. There is no identity registry, retained engine pointer, per-key side table, new goroutine, or per-spot synchronization.
- The existing [ADR-0334](../decisions/ADR-0334-frequency-change-aware-secondary-dedupe.md) already requires per-run counters and run identity. This is a correction of that implementation, not a new policy or architecture decision.

## Decision Linkage
- ADR created/updated: none.
- Decision delta summary: none; retain the existing run/counter contract.
- Contract/behavior changes: shadow log `run=` retains its start timestamp and gains a process-local numeric suffix. Filtering, delivery, configuration, persistence, logger routing, and CI definitions are unchanged.

## Verification and Monitoring

Approved item mapping and falsifying evidence:

| Item | Implementation and source review | Evidence |
| --- | --- | --- |
| Self-contained executable fixture | `internal/cluster/unified_v3_main_binary_test.go`: private table created in the existing temporary directory; all three configured paths use it; actual executable, isolated PATH/cwd, peer and log checks retained | Existing executable integration test on native Windows and clean tracked Linux checkout |
| Reliable engine identity | `dedup/secondary_frequency.go`, `secondary_frequency_stats.go`: constructor-only scalar, immutable snapshot value, no wrap or engine retention | Equal-start and stable-snapshot tests, concurrent constructors under race checking, local-counter exhaustion boundary |
| Correct interval and log identity | `internal/cluster/secondary_shadow_summary.go` and its tests: compare engine run number before subtraction; copy snapshot before logging | Forced-collision regression with every expected interval field; stable/split run-token tests; existing off/on, unlocked emission, actual ticker, and maximum-width 2-KiB tests |

Source review: lead traced constructor validation/off/error/success paths, snapshot ownership, replacement arithmetic, formatter width, and configured temporary-table lifetime. A separate read-only Go code-quality reviewer inspected the six-file diff and startup error propagation and found no material defects. The reviewer did not run tests; validation remains lead-owned.

Observed so far:
- Old-code regression: native rebuilt `TestSecondaryShadowSummaryGoldenAndRestart` failed as expected with unsigned interval underflow (`.tmp/ci-repair-red.log`).
- Repaired targeted native Windows: `scripts/test-windows.ps1 -Packages @('./dedup','./internal/cluster') -Run 'TestFrequencyRunIdentity|TestSecondaryShadow|TestV6MainExecutablePeerLoggingWithoutCompanion' -Timeout 180s` passed 11 cases, zero skips (`.tmp/ci-repair-targeted-windows.log`).
- Clean tracked Linux fixture: archived the baseline, overlaid the approved changes, and verified that `data/rbn/priors` was absent before running tests. The same targeted regex passed, including the actual executable test (`.tmp/ci-repair-targeted-linux.log`).
- Full clean-checkout Linux race: `go test -race -count=1 ./...` passed 59 test packages and 16 build-only packages; `internal/cluster` passed in 341.904 seconds (`.tmp/ci-repair-linux-race.log`). Concurrent engine construction was among the selected regression tests. This is race-detector evidence for exercised paths, not proof of every possible interleaving.
- At the time of this record, the native Windows test/vet/Staticcheck/lint baseline and final documentation/diff checks were not yet claimed as passed. See the follow-on evidence below.

Follow-on evidence for `e960e46`:

- The native full Windows log `.tmp/ci-repair-windows-full.log` failed `TestCWSpeedDisplayNeverTruncatesLabel` with `2209Z` in the rendered line. The [hosted CI run](https://github.com/N2WQ/GoCluster/actions/runs/42912215598) failed the same test on Linux with `2213Z` and Windows with `2226Z`; both maintained-module jobs passed. The later Windows command-oracle step did not run. These failures do not establish that the earlier two defects persisted.
- The assertion searched the entire rendered line for `22`, including the UTC timestamp. Git history places the assertion in `ab7d8da`, before this repair. The follow-on test correction fixes the time at `2222Z` and checks the comment field, while retaining complete-label coverage; it does not change production formatting. Final follow-on validation is recorded separately below.
- Corrected targeted native Windows run: `scripts/test-windows.ps1` selected and required both `TestCWSpeedDisplayNeverTruncatesLabel` and `TestCWSpeedDisplayCacheEquivalence`; 24 cases passed, zero skipped. The former exercises the fixed `2222Z` timestamp and exact comment field; the latter retains the complete-label positive control.
- Complete native Windows baseline: `pwsh -NoProfile -File scripts/test-windows.ps1` passed 12,273 cases across 74 packages (59 tested and 15 build-only), with 93 skips. Skips include opt-in live and external-system cases; this is not live integration qualification. `go vet ./...`, `staticcheck ./...`, and `golangci-lint run ./... --config=.golangci.yaml` passed; lint reported zero issues.
- Clean tracked Linux checkout: an archive of `e960e46` with only the corrected CW test overlaid had no ignored reliability directory. The targeted CW tests and `go test -count=1 ./...` passed across 59 test packages and 16 build-only packages. This is local Linux evidence, not a passed hosted CI run for the follow-on change.

Signals to monitor: the next pushed revision's Linux and Windows jobs; stable run tokens within an engine and clean interval resets on replacement. Local validation does not establish production performance or globally unique identifiers.

Rollback triggers: a demonstrated regression in executable startup, peer logging, diagnostic counter resets, or log record size. Reverting the repair restores the known fixture dependency and timestamp collision; keep the checks enabled while diagnosing.

## References
- Failed workflow commit: `689760246a4ebdfa1242dfb9cda7ed93848cf608`.
- Failed parent commit: `471bb112d28b3f84c72efd2f7e2f8d8522f7ec06`.
- Related ADR: [ADR-0334](../decisions/ADR-0334-frequency-change-aware-secondary-dedupe.md).
- Related docs: [approved dedupe validation](../qsy-dedupe-v6-validation.md), [operator guide](../OPERATOR_GUIDE.md#evaluate-reported-frequency-changes), [validation runbook](../dev-runbook.md).
- Support-agent documentation impact: required for the changed diagnostic identity; updated operator and validation records are already routed by `customgpt/source-map.md` and `customgpt/developer-guide-index.md`. No additional routing change is needed.

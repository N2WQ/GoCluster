# TSR-0127 - Stabilizer Confidence Downgrades and Late Output

Status: Monitoring
Date Opened: 2026-11-11
Date Resolved: n/a
Owner: Repository owner / Codex
Technical Area: live stabilizer, output location, acceptance, telemetry and retry-test ownership
Trigger Source: Operator report
Led To ADR(s): ADR-0341, ADR-0343
Tags: stabilizer, confidence, grid, age, bounded-state, test-ownership

## RCA Summary

- What happened: The operator's transition matrix showed held P spots ending S.
- Why: Each recheck overwrote confidence from fresh evidence; no ordinary-letter
  maximum survived the hold. Fresh evidence can weaken or strengthen as receiver
  rows expire or arrivals republish. Correction also retained the old grid through
  downstream writeback, and age was not enforced at delayed/final output.
- What fixed it: Separate bounded ordinary-letter retention from decision/history
  evidence, refresh corrected location without inherited DX writeback, and enforce
  pre-core/final-offer age checks with two explicit counters.
- How we know: Current-source tracing plus real resolver/scheduler, persistence,
  output-receipt, callback-crossing and race tests in the approved repair. Local
  evidence is recorded in [execution evidence](../stabilizer-live-repair-v2.md).
  Deployment and empirical accuracy are not established by these checks.
- Operator/support answer: A retained P means P was reached during that hold.
  DIAG CONF remains current and can be weaker/unavailable. Read `retained letter`
  and `age drops` alongside the matrix. Under the later v5 amendment, a corrected
  call requires a completed lookup: reported grids win, derived positives use
  current captured CTY, and incomplete lookups leave the DX grid unknown. Watch
  `Corrected output: no grid` for ordinary output admissions with that condition.
- Later qualification: the retention test also used an invalid pending-counter
  oracle. The scheduler can transfer a correct retry to its release channel
  before the assertion. The test now receives and validates the actual retry
  before checking ownership; see the qualification finding below.

## Triggering Request

- Request date: 2026-11-11
- Request summary: Thoroughly audit P-to-S stabilizer transitions and enforce
  upgrade-only call confidence; exact Approved v2 authorized sequential repair.
- Request reference: Local owner conversation and attached transition screenshot.

## Symptoms and Impact

The matrix exposed real terminal downgrades for the same call. Fresh evidence
could also extend low-P waiting to the current unknown/ambiguity budget. A late
accepted call correction could leave stale classification/location caches and
persist the old call's reported grid under the new name. Held work admitted just
inside the age limit could be offered after it expired.

## Timeline

1. 2026-11-11 - Audit traced admission, scheduled retries, correction and output.
2. 2026-11-11 - Owner selected current percentage, final-delivery age, dynamic
   budgets, and same/missing-target grid retention; approved v2 repair.
3. 2026-11-11 - Sequential confidence, location and age slices validated locally;
   telemetry/docs and final-lane evidence tracked in the execution record.
4. 2026-11-11 - Owner clarified reported-versus-derived precedence and required
   completed lookup before CTY, clearing the old grid otherwise. Approved v5
   authorizes the bounded grid amendment and one missing-grid admission counter;
   see [v5 execution evidence](../stabilizer-grid-repair-v5.md).
5. 2026-11-13 - CC11 qualification on `66405747` reproduced a baseline retention
   test failure during a full Linux race run. Approved v1 authorizes a test-only
   ownership repair and this troubleshooting amendment.

## Hypotheses and Tests

1. The matrix invents the downgrade - Rejected. Entry telemetry is immutable and
   terminal glyph was read from the fresh Spot; rechecks really overwrote P.
2. Rechecks can only weaken - Rejected. Opposing evidence expiry can improve the
   surviving call's share, and new arrivals republish. Retain legitimate upgrades.
3. Average 12.0 proves timeout at exactly 60 seconds - Rejected. Rounding can hide
   a small minority of earlier completions; check twelve may clear rather than
   timeout, and elapsed time includes queueing/lock delays.
4. Lookup before acceptance is sufficient for age - Rejected. Store/admission
   contention and location/writeback callbacks can cross expiry. Deterministic
   clocks test these boundaries without changing source times or runtime config.

## Findings

- Root cause: conflating fresh evidence with the user-visible retained letter,
  and missing late identity/age boundaries.

Decision counters count evaluations,
not unique spots; repeated evaluations are not an inflation defect. Original
source grids exist for PSKReporter CW/RTTY. The initial v2 policy retained them
on missing/equal replacement lookups. The owner later selected corrected-call
lookup authority: an old identity's attached grid must not survive an incomplete
lookup. Stored reported grids beat CTY estimates, while stored derived positives
permit recomputation from the correction's CTY snapshot. Lookup provenance and
writeback ownership remain separate from attached output location.

Cold cache is not proof of store absence. Existing synchronous main lookup can
time out; delayed lookup never waits. An unsuccessful result does not authorize
direct CTY fallback. Real blocked-worker tests deliver unknown location before
unblocking, then verify late backfill cannot mutate sealed output. Equal locator
tests also inspect provenance; text alone would miss a reported/derived mistake.

## Decision Linkage

- ADR created: [ADR-0341](../decisions/ADR-0341-stabilizer-live-output.md).
- Grid/counter amendment: [ADR-0343](../decisions/ADR-0343-corrected-grid-precedence.md).
- Contract changes: retained output letter, corrected-grid ownership, age
  admission boundaries and fixed counters. Fresh algorithm/scoring inputs,
  dynamic reason budgets and completed-check counts remain unchanged.
- Earlier path learning, H3 initialization, replay fidelity, existing DB repair,
  deployment and RF accuracy qualification remain outside this repair.

## Verification and Monitoring

Targeted native ordinary/race evidence covers intermediate retention, timeout
release/suppression, retry overflow, B/C/V, current percentage, real source-grid
ingest and reopened grid provenance, no worker wait/repeated path learning,
strict age boundaries, accepted occurrence counts and final output/witness
exclusion. Review [execution evidence](../stabilizer-live-repair-v2.md) for exact
counts, commands and final-lane results, and [v5 evidence](../stabilizer-grid-repair-v5.md)
for corrected-grid precedence/clearing and once-only missing-grid output counts.
The counter excludes pre-admission drops and counts later queue refusal; do not
read it as client receipt or a count of storage errors. Monitor fixed counters and reason counts;
legacy Held includes retries and is not a unique-spot conservation measure.
Unexpected lower ordinary terminal transitions, stale final offers or target
grid corruption warrant stopping rollout and preserving exact revision/evidence.

### Retention-test retry ownership qualification

The full native Windows suite passed on `cc11` at `66405747`. A full Linux race
run passed once, then a second full run failed in
`TestStabilizerRetentionFreshEvidenceAndReasonBudget/floor_false/thinned` at
check three with `prematurely completed or lost its fresh twelve-check hold`.
The failing predicate combined ring rows, terminal-retention count and
`Pending()!=1` without printing their values. This reproduces the checkpoint
failure but does not establish which term failed in that run. The earlier
ARM64 handoff failure has no retained failing assertion available for comparison.

Source review established a distinct test defect: `Pending` counts reserved
scheduler queue slots. The running one-nanosecond scheduler decrements it before
putting a retry on the release channel. A valid released retry therefore need
not leave `Pending()==1`. This legal interleaving needs no data race, and a
passing isolated retry does not repair the oracle or prove production work loss.

The repaired test receives each actual next retry with the existing bounded
helper, validates its key, completed-check count, evidence flag, retained maximum
and fresh confidence, then carries it into the next iteration. At that receipt
boundary the test owns the retry and expects zero pending scheduler slots, zero
ring rows and zero terminal-retention outcomes. Evidence still expires before
handling the third recheck. Twelve-check reason budgets, the three-check overflow
case, suppression, thinning, terminal accounting and the stopped replacement's
filler reservation remain covered. Failure messages expose individual values.
The intermediate-promotion companion test remains unchanged.

This changes only the test's ownership observation. Runtime stabilizer, model,
CC11, configuration and threshold behavior are unchanged; no ADR decision changes.
Support routing already links this record, so no separate support-card change is
needed. Production latency and installed-client interoperability remain separate
qualification obligations.

Post-repair validation used Go 1.27.1 on Windows amd64 and WSL Linux amd64 with
CGO enabled:

- Both retention tests passed ten repetitions on each platform: 100 passing
  parent/subtest events per platform, with no skips. Windows used the stable
  runner's prepared `internal/cluster` binary with
  `-test.run='^TestStabilizerRetention(FreshEvidenceAndReasonBudget|KeepsIntermediatePromotion)$'`
  and `-test.count=10`. Linux used the same selector with
  `go test -race -count=10 -timeout=5m -json ./internal/cluster`.
- `pwsh -NoProfile -File scripts/test-windows.ps1` passed 13,187 cases across
  59 executed packages, with 15 build-only packages and 97 reported skips.
- `go test -race -count=1 -timeout=20m -json ./...` passed 13,058 case events
  across 59 test packages, with 16 packages without tests and 101 reported skips.
  No data-race warnings occurred. All 149 CC11-related case/seed events passed
  in each full lane without skips. The repaired thinning case and the unchanged
  intermediate-promotion companion also passed both full lanes.
- `go vet ./...`, `staticcheck ./...` and
  `golangci-lint run ./... --config=.golangci.yaml` passed on the repaired source.

Reported full-lane skips retain their existing helper, platform or opt-in gates;
no assertion, threshold or selected case was waived for this repair. Full Linux
race JSON and stderr were saved directly to persistent Windows storage after a
prior WSL temporary log did not survive its process. These results validate the
repaired ownership observation; they do not recover the unprinted values from
the earlier combined assertion or establish production qualification.

## References

- [Live output contract](../../spot/README.md#live-stabilizer-output)
- [Matrix/counter guide](../../ui/README.md#stabilizer-matrix)
- Source: `internal/cluster/output_pipeline_delivery.go`, `output_location.go`,
  `output_age.go`, `stabilizer_confidence.go`, `stabilizer.go`, `stats/tracker.go`.
- Tests: `stabilizer_confidence_test.go`, `corrected_grid_delivery_test.go`,
  `output_age_test.go`, `pskreporter/client_test.go`, `ui/stabilizer_matrix_test.go`.

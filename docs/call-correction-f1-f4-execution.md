# Call correction F1-F4 execution

Authorization: owner's exact **Approved v1**, 2026-10-14. Reviewed baseline
`99ab1e7c0c5cc54ec49cd63904b10f1f503eaf3c`; clean implementation checkout
`8f152889548ee5b4baf3059d92d5bcaee9b01346`. The 19 affected pre-existing
implementation/configuration files were unchanged between those revisions.
No commit, push or deployment is part of this execution.

## Approved items and evidence

| Slice | Implementation | Required proof |
| --- | --- | --- |
| A: dissent and raw ties | Immutable pending endpoint metadata; shared exact/neighborhood eligibility and actual-proposal guards; owner/fresh ambiguity consistency | Blocked and recovered exact/neighborhood states, raw membership expiry, admitted .95/1 weights, actual temporal proposal, normal/DIAG TCP, original peer fields, sealed ring and reopened archive |
| B: activity admission and expiry | Shared immutable activity tuple; primary admission sink in both flood topologies; existing adaptive mutex; success-only update; observation-driven pruning and cardinality | Real initialized ingest, duplicate/output-drop retention, flood/validation/refusal exclusions, mutable-spot race, concurrent queries, original timestamp, expiry without queries, measured admission costs |
| C: replacement and final authority | Runtime/replay nonmutating license predicate; replacement precheck; unchanged final gate; frozen per-run inputs/provenance; historical Canadian time and separate cache TTL; cleanup | Missing/found replacement controls, actual retries and temporal releases, later final refusal without rollback, disabled/unavailable/allowlist/portable cases, dated activation/expiry, partial and consecutive cleanup, Windows deletion failure/retry |

Shipped minimum reliability remains **0.95** and base stabilizer budget remains
**12 checks**. Unequal-weight delivery fixtures use admitted 950/1000 weights.
Runtime low-P retry uses literal B4/A1 (20% retained original) to satisfy the
strict shipped `<25%` hold policy, with its unchanged three-check budget.
B3/A1 at exactly 25% is the immediate/temporal retention fixture.

The mathematical review occurred before approval in a separate non-steered
context: equal raw counts can contain unequal weighted evidence. It did not
establish empirical superiority of either policy. Implementation agents and the
final code reviewer received implementation context; they are not described as
non-steered scientific reviewers.

## Defect reproduction and targeted verification

Logs are retained locally under `.tmp/call-correction-f1-f4-20261014/`.
Slice A logs are under `.tmp/slice-a/`: `baseline-evidence.txt`,
`spot-race.log`, `correctionflow-final-race.log`, `cluster-race.log` and
`replay-race.log`. The 24 real TCP/archive protection/recovery cases and 16
replay cases pass ordinary and targeted race checks.
Workers compile unique stable native binaries with `go test -c -o <path>` and
execute from each package directory; no overlapping stable-runner invocation.

- Before repairs, F1 exact A3/B5 became a nearby B3/3 apparent 100% winner;
  F2 disjoint admitted 3:3 unequal weights escaped split protection.
- Existing adaptive `Observe` retained 100 receivers across 99 windows without
  queries, rather than the independently expected two.
- `runtime-f4-baseline-corrected.log`: missing replacement changed the original
  and left ring count zero; independently licensed replacement positive control passed.
- `replay-c-baseline.log`: missing replacement and final-DX absence failed.
- `runtime-f4-paths-final.log`: six missing/found controls across immediate,
  actual scheduler retries and real temporal Apply passed, including sockets,
  peer original, sealed ring and reopened archive. Later actual database
  generation change rejects final output while preserving accepted mutation.
- `replay-c-package.log`, `uls-c-package.log`, `replay-c-race.log`,
  `uls-c-race.log`: full owned packages ordinary and targeted race passed.
- `activity-internal-cluster-final-race3.log`: real initialized flood on/off
  wiring passed three race repetitions. A FIFO duplicate barrier proves the
  processed output-congestion case actually dropped output, while admission
  activity remained counted.

Initial coarse-clock latency samples are superseded. An initial test fixture
expected a hold at exactly 25%; correcting it to actual `<25%` eligibility
changed only the test. Replay's existing no-per-row-percentage output contract
was retained rather than expanding output to satisfy a new fixture.

## Admission performance acceptance

**Local judgment: pass for this correctness repair, with a measured serialization
cost.** Known-receiver operations allocate zero bytes/objects, successful
admission makes progress under concurrent queries, and the churn test stays in
its independently derived retention bound. These are local component results;
no service latency SLA, RF accuracy or production improvement is claimed.

Three quiet repetitions use exact prepatch adaptive source through a Go overlay
versus the final transaction, with identical instrumentation. The reference
performs the old late activity update after successful channel admission; it is
more comparable than raw enqueue, which does no activity accounting. Final
refusal deliberately acquires the adaptive lock before attempting enqueue.

Host: native Windows amd64, Intel Core i9-10900, Go 1.27.1, GOMAXPROCS 20.

At 100,000 retained reporters:

| Workload | Reference median | Final median | Reference throughput | Final throughput |
| --- | ---: | ---: | ---: | ---: |
| Normal | 101.8 ns/op | 142.4 ns/op | 9.82 million/s | 7.02 million/s |
| Forced due prune | 1.719 ms/op | 2.105 ms/op | 582/s | 475/s |
| Concurrent queries | 410.0 ns/op | 351.5 ns/op | 2.44 million/s | 2.84 million/s |
| Refusal attempts | 5.9 ns/op | 58.5 ns/op | 169 million/s | 17.1 million/s |
| Fixed-count churn | 62.813 us/op | 61.870 us/op | 15,920/s | 16,163/s |

All measured rows are 0 B/op, 0 allocs/op. Refusal has zero successful throughput.
The complete shared helper using production `time.Now` measured 180.0-182.5
ns/op, also zero allocations. Forced pruning scans every operation and measures
scan stress, not expected steady throughput at the shipped 60-second cadence.

Windows QueryPerformanceCounter had a minimum observed positive delta of 100 ns;
sub-resolution samples can remain zero. Timing overhead is present in both
reference and final. Median percentile summaries across three repetitions:

| Workload | Reference p50 / p95 / p99 | Final p50 / p95 / p99 | Worst final max |
| --- | --- | --- | ---: |
| Normal | 200 / 300 / 300 ns | 200 / 300 / 300 ns | 18.6 us |
| Due prune | 1.794 / 2.416 / 2.620 ms | 1.657 / 2.041 / 2.134 ms | 2.582 ms |
| Concurrent queries | 200 ns / 6.0 / 17.3 us | 200 ns / 5.0 / 21.2 us | 153.5 us |
| Refusal | 100 / 200 / 200 ns | 200 / 300 / 300 ns | 172.3 us |
| Churn | 300 ns / 81.3 us / 2.153 ms | 200 ns / 80.0 us / 1.750 ms | 7.427 ms |
| Churn plus queries | 300 ns / 87.8 us / 2.389 ms | 300 ns / 134.9 us / 2.239 ms | 7.284 ms |

Throughput and percentile runs are separate samples; their pruning medians need
not agree. Normal final throughput runs ranged 135.0-386.7 ns/op; the outlier is
retained in `activity-performance-summary.json`. Churn admits 2,000 observations
over 200 simulated seconds, using a 60-second window and one-second maintenance
cadence. Final retained receivers: 601 alone, 603-609 with queries, within the
expected 601-610 bound. Seeding/string construction are outside timing.

The mutex can wait during scans: nonblocking enqueue is not wait-free admission.
Accept the measured local cost to make the admission boundary consistent; watch
real admission latency and cardinality when deployed. No live profile was
collected. Complete values/ranges/raw controls are in
`activity-performance-summary.json`, `activity-performance-*-qpc-quiet3.log`,
`activity-churn-*-qpc-quiet3.log` and `activity-helper-final-realclock.log`.

The dense selection stress case uses five buckets, 16 candidates per bucket and
64 receivers per candidate. Baseline three-sample range: 442,402-793,164 ns/op,
median 461,946; final lint-clean source range: 450,393-566,370 ns/op, median
470,414 (about +1.8%). Median 134,539 versus 133,069 B/op, with 247 allocations
unchanged. Local judgment: pass for bounded selection work, with the additional
metadata/evidence cost retained. Samples are short (three x 100 ms), overlap and
do not establish a statistically significant performance change or production
latency. `dense-lintfix-final.log` supersedes earlier intermediate dense results.

## Final selected lane

- Full ordinary: `pwsh -NoProfile -File scripts/test-windows.ps1` passed all 70
  packages (53 executed, 17 build-only), 9,589 passed cases and 77 skips;
  `full-ordinary.log`. Skips include child-only helper entrypoints, opt-in live
  FCC/CC sessions, PC92 qualification, external DXSpider references and optional
  VOACAP/H3 fixtures. Those external/live environments are not validated here.
- `go vet ./...`, `staticcheck ./...` and
  `golangci-lint run ./... --config=.golangci.yaml` passed on final source;
  `vet-final.log`, `staticcheck-final.log`, `lint-final2.log` (`0 issues.`).
- Initial lint found large struct copies, a test switch simplification and
  ignored cleanup errors. Indexed reads/test switch preserve semantics; the
  fixture cleanup callback now reports errors, while production returns them.
  Fresh correctionflow ordinary/race package checks passed after those fixes.
- The final replay package ordinary run also passed after the cleanup callback
  fix (`replay-lead-final-ordinary.log`). Final read-only code-quality review
  reported no material findings, including a refresh of the lint fixes. The lead
  rechecked ownership, bounded publications, actual-proposal guards, clocks and
  final authority. This implementation review used implementation context.
- Full race: `pwsh -NoProfile -File scripts/test-windows.ps1 -Race` passed all
  70 packages, with the same 9,589 passed cases and 77 documented skips;
  `full-race.log`. No race report occurred.
- Connected CustomSCP: `scripts/test-customscp-regression.ps1` and its `-Race`
  variant both passed producer preservation, reopened qualified support, actual
  telnet/ring/archive delivery and the held negative control. Logs:
  `connected-ordinary.log`, `connected-race.log`. Run identities:
  `9d260b85cf2b4ecaa83125b9d3e0e91a` (ordinary),
  `ffb9eb00a6434fe9a000bc3e014d4c6d` (race), under `.tmp/customscp-regression/`.
- Troubleshooting-record checker, 71 local Markdown link targets and
  `git diff --check` passed. `go run ./cmd/codemap generate -all` and
  `go run ./cmd/codemap check -all` passed (`code maps are fresh`); see
  `codemap-generate.log` and `codemap-check.log`. Final generated ingest map
  changes and the complete owned diff were reviewed against the approved scope.

The only later production-source edit was comment wording in the flood sink:
it distinguishes a nonblocking send from admission-mutex waiting. It changes no
behavior and does not invalidate the test/analyzer evidence.

## Decision and limits

Support-agent impact: changed operator-visible correction/replay behavior.
Updated `customgpt/source-map.md` and `customgpt/troubleshooting-index.md` route
support to the current contract, new ADR/TSR and the explicit replay limits.

[TSR-0095](troubleshooting/TSR-0095-call-correction-evidence-admission-and-licensing.md)
records reproduction and troubleshooting;
[ADR-0302](decisions/ADR-0302-correction-evidence-admission-and-license-authority.md)
records the six selected policies. No replacement model or threshold tuning.
Final validation can still reject an accepted correction after authority changes.
Frozen license bytes do not reproduce database updates, omitted production
preprocessing, or independent receiver truth. Cleanup's primary/secondary error
joining was source-reviewed; no artificial combined full-download fault was added.

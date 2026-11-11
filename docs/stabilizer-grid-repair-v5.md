# Corrected-grid repair — Approved v5 execution

## Authority and identity

- Owner token: exact `Approved v5`, after the v5 unattended ledger. Earlier
  `Approved v1` replies did not authorize this ledger; no repair edits preceded v5.
- Workspace: `C:\src\gocluster`, local `main`, clean start at
  `a6064782543e3dde94c1c67393fb853d1ca340d6`.
- The original inspected runtime base was `0536c43d`; intervening commit
  `a6064782` adds the unattended skill/documentation only. Runtime code is unchanged.
- Window: 2026-11-11 16:15:38–20:15:38 UTC (11:15:38–15:15:38 America/New_York).
  Reserve final verification from 19:15:38 UTC / 14:15:38 Eastern. Finish early
  if complete; do not expand scope to fill the window.
- Changes/checkpoints stay as attributable local diffs plus this record and
  ignored `.tmp/stabilizer-v5-*.log` evidence. No commit/push/deployment,
  production-data operations, installation, elevation, or firewall/security edits.

## Approved contract

For an accepted literal call correction, a successful reported target-grid
lookup wins. A successful derived target-grid lookup permits recalculation from
the CTY snapshot used for that correction. Unsuccessful/pending/timed-out lookup
does not authorize CTY fallback: clear the old grid. Unusable CTY also clears.
Origin flags must change even for equal grid text. Clear/rebuild normalized grid,
prefix and fine/coarse geographic caches coherently; preserve DE facts/cells.
Uncorrected nonempty grids and normal empty-grid backfill retain existing rules.

Reuse existing lookup/cache/worker authority. Delayed consumers do not wait for
storage. No new hooks, worker, timer, queue or per-call retained state. Preserve
corrected-DX fanout write suppression and lookup-owned persistence; recomputed
output does not rewrite old stored estimates. Existing cache freshness limits,
legacy DB provenance and lack of individual-feed provenance remain explicit.

Add one fixed, resettable atomic counter for locally corrected output with no
DX grid after final age admission and ordinary shared thinning, once per spot
before destination processing. Exclude uncorrected/rejected/held/age-dropped or
all-ordinary-lanes-thinned/SELF-only work. Later recipient filtering, family
hiding and queue refusal do not undo admission. This is not client receipt.
Expose it in headless and all three Pipeline Quality panes with help and
unsigned-safe large-total presentation. No automatic calendar reset.

Confidence/percent/budgets, age checks, accepted learning, prior path learning,
replay/H3 initialization, runtime configuration and scientific conversion/model
rules remain unchanged. ADR-0342 was taken by the intervening skill commit;
the new grid decision will use the next free ADR number, currently ADR-0343.

## Queue and gates

0. Noninteractive preflight and relevant existing ordinary/race baseline.
1. Grid selection/snapshot/provenance/clearing; targeted ordinary/race gate.
2. Missing-grid output counter; targeted cluster/stats ordinary/race gate.
3. UI/headless/help/documentation/ADR/support; targeted rendering/format gate.
4. Full native ordinary/race, analyzers, connected CustomSCP ordinary/race,
   ordinary-binary allocation benchmarks, records/maps and fresh final review.

A failed mandatory gate blocks the next implementation slice. Diagnose relevant
failures without weakening assertions or adding skips. Unexpected policy/scope
changes are deferred; no new authorization is inferred from the time window.
OS-blocked checks stay unmet, with no prompt approval or security workaround.
At deadline retire owned work safely. Restore only attributable unfinished edits
when separable; never blanket-reset or discard concurrent owner work.

## Detailed falsifiability obligations

All rows are lead-owned fixtures/checks, design-aware reviewed, not independent
scientific accuracy evidence. Existing CTY conversion vectors remain authoritative
for unchanged math; literal fixture coordinates avoid self-derived expected grids.

| Contract | Fault/stimulus | Required observation | Level / checker / false-green guard |
| --- | --- | --- | --- |
| Reported beats derived | Valid CTY plus reported target, full precision | Target full grid, reported flag; target DB/cache unchanged | Real consumer/store: `TestCorrectedGridPolicyAndWriteback`; reopened readback |
| Current CTY for derived positive | Old stored estimate differs; publish CTY B during lookup after capturing A | Output/classification remain on A, derived flag | Both actual paths: policy matrix with snapshot-change cases; no single-snapshot-only proxy |
| Equal text does not preserve old origin | Old reported/new derived and reverse | New origin in output even equal locator | Same consumer matrix, both directions |
| Clear all geographic facts | Old nonzero DX/DE cells and normalized prefixes, lookup miss or invalid/nil CTY | Empty DX grid/norm/prefix, false derived, zero DX fine/coarse; DE intact | Owning helper plus real consumers; mandatory populated positive controls |
| No CTY on incomplete lookup | Real blocked worker; valid caller CTY; main timeout or delayed cold cache | Unknown output before worker release; later owned backfill cannot mutate sealed output | Real worker/scheduler tests; a false-return stub alone is insufficient |
| No unintended changes | Rejected or unchanged identity; known old grid | Existing grid/provenance/DE behavior | Existing no-correction and persistence controls |
| Counter once at final admission | Actual corrections, immediate/delayed, multiple destinations | One increment, not per sink | Real output tests; no direct counter-only positive control |
| Counter excludes prior termination | Expire during final writeback after core acceptance; fully thin ordinary lanes | Zero increment; existing learning/age accounting unchanged | Existing age seam and real secondary dedupe controls |
| Count despite later refusal | Family hiding or actual broadcast-queue refusal | One increment | Final routing tests; cannot gate on emittedNow/broadcast success |
| Counter reset/presentation | Concurrent increments/read/reset; zero and full unsigned formatting range | Joined reset zero; no negative/truncated row in all panes | Stats tests + actual formatter oracle and 14-row simulated-screen rendering |

## Execution status

Slice 0 passed: native ordinary and race preflight each executed 35 cases in
cluster/CTY/stats, zero skipped. Exact required identities included the real
TCP age fixture, corrected-grid policy, shipped CTY vectors and tracker reset.
Logs: `.tmp/stabilizer-v5-preflight.log`, `.tmp/stabilizer-v5-preflight-race.log`.
No elevation/interactive action was requested by the agent.

Detailed design-aware post-approval review passed with checker refinements:
explicit CTY coordinates; snapshot-change cases in both actual policy paths;
populated-cell controls and empty-safe prefixes; separate worker and pipeline
CTY providers; real main timeout; empty-grid uncorrected/B negative counter
controls; late-age-after-acceptance stimulus; unsigned formatter and pane tests.
These refine evidence, not policy/scope. Slice 1's required case
identities include both policy `captured_snapshot` subcases and the named
`TestCorrectedGridMainLookupTimeoutDoesNotUseCTY` / delayed nonwait checks.

Slice 1 passed 33 targeted cases in each native ordinary/race run, zero skips.
The regression matrix was observed failing before production changes. Logs:
`.tmp/stabilizer-v5-grid-before.log`, `-grid.log`, `-grid-race.log`.
Design-aware read-only source review found no material defect or blocking gap.
A fixture cleanup refinement joins both delayed/main output goroutines on failure;
its affected ordinary/race checks are rerun before slice 2.

The fixture cleanup reruns passed (one required case in each lane). Slice 2
passed 49 cases in both ordinary/race across cluster and stats, zero skipped;
logs: `.tmp/stabilizer-v5-counter.log` and `-counter-race.log`. Focused
design-aware read-only review found no blocker: final admission predicates,
once-only multi-destination counting, real refusal/thinning/late-age controls,
empty-grid unchanged/B controls and atomic/reset behavior are covered. Slice 3
adds presentation and updates the durable/current support contract.

Slice 3 passed 48 targeted cases in each ordinary/race lane, zero skipped, plus
the additional required overview-wiring case in both lanes. Producer formatting
covers zero, comma boundaries, 2^63 and MaxUint64; 60x14 simulation renders the
complete row in Overview/Pipeline/Events. The troubleshooting checker passed.
Read-only design-aware review found no production defect/scope drift; an
ambiguous 'no correction' documentation sentence was clarified. Logs:
`.tmp/stabilizer-v5-presentation*.log`, `-overview*.log`, `-records.log`.
Support impact: updated spot/UI/domain contracts, confidence support-card route,
TSR-0127 and new ADR-0343 with partial supersession/index links. Original v2
execution evidence remains historical. Slice 4 now runs the integrated lane.

## Integrated validation progress

- `go vet ./...` and `staticcheck ./...` passed. The first golangci-lint run
  found QF1003 in the new test's disposition if/else chain. Replacing only that
  chain with a tagged switch preserved its oracle; the lint rerun reported
  zero issues. The ordinary full run compiled cluster after this test-only fix;
  the full race run will also use it. Logs: `.tmp/stabilizer-v5-{vet,staticcheck,lint,lint-rerun}.log`.
- Three 300ms samples on the ordinary saved cluster binary:

  | Workload | ns/op range | B/op | allocs/op |
  | --- | --- | --- | --- |
  | Unchanged nonempty grid | 8.842–8.887 | 0 | 0 |
  | Corrected reported lookup | 6,227–6,508 | 0 | 0 |
  | Corrected derived lookup/current CTY | 6,456–6,853 | 4 | 1 |
  | Corrected incomplete lookup/clear | 3,248–3,686 | 0 | 0 |
  | Counter increment | 5.290–5.431 | 0 | 0 |

  `BenchmarkCorrectedOutputLocation` resets old populated location fields on
  each iteration, exercises real cache lookup and cell hydration, and checks
  call counts/results. The derived allocation is the four-character conversion
  result. These warm helper measurements exclude storage waits, full pipeline
  cost and production tail latency; no pre/post net performance claim is made.
  Build: `scripts/test-windows.ps1 -PrepareOnly -Packages ./internal/cluster`.
  Saved binary flags: `-test.run=^$`,
  `-test.bench=^BenchmarkCorrectedOutput(Location|WithoutGridCounter)$`,
  `-test.benchmem`, `-test.count=3`, `-test.benchtime=300ms`. Logs:
  `.tmp/stabilizer-v5-bench-build.log`, `-bench.log`.
- A scan found all 111 relative Markdown file targets in the ten changed/new
  documents; it did not check fragment anchors. Troubleshooting record checks
  passed. No deployed support/Cloudflare behavior is part of these checks.
- Fresh design-aware read-only Go review covered all five production Go files
  and six changed/new test/benchmark files, the actual diff, contracts and bench
  workload. No material findings remained. The lead also reviewed the source;
  neither pass is independent scientific evidence. Existing CTY conversion/model
  semantics remain unchanged, and no empirical accuracy verdict is issued.
- `gopls` references confirmed the two production location callers and the one
  common counter increment owner; `rg` and compiled tests supplement test-variant
  references. `go list -deps -test -json` plus `jq` confirmed the cluster/UI/stats
  package impact. There is no changed interface mode, parser, config or schema.

- Full native ordinary: `scripts/test-windows.ps1` passed 12,517 cases;
  59 executed and 15 build-only packages (74 total), 93 skipped cases. Exact
  skipped identities match the existing `.tmp/stabilizer-v2-full-race.log`
  baseline; no new repair case skipped. These existing skips include external
  interoperability, helper-only and opt-in live/profile/soak qualifications and
  the unavailable Windows/non-CGO H3Mapper backend. They are not waived passes
  or evidence for external/backend/production behavior. Log:
  `.tmp/stabilizer-v5-full.log`.

- Full native race: `scripts/test-windows.ps1 -Race -Timeout 20m` passed the same
  12,517 cases, 59 executed / 15 build-only packages and 93 skips. Ordinary/race
  skipped identities match exactly. No race report, panic or failure occurred.
  The longer per-package instrumented-test allowance changes no runtime setting.
  Log: `.tmp/stabilizer-v5-full-race.log`.
- Connected CustomSCP: `scripts/test-customscp-regression.ps1` and its `-Race`
  run both passed the real producer/database/consumer handoff and exact TCP
  receipts. Evidence IDs: `e7ca497bf6784177b7463903efa7f0fa` (ordinary) and
  `d0d3dc5ede304845b58193c631fe686a` (race), under
  `.tmp/customscp-regression/`. Logs: `.tmp/stabilizer-v5-connected.log`,
  `-connected-race.log`.
- All 11 changed/new Go files are gofmt-clean. Repository-configured
  `git diff --check` passed. A supplemental invocation disabling automatic
  line-ending conversion produced false CRLF whitespace findings; it changed
  no files and was replaced by the normal check, not by code/assertion changes.
  Log: `.tmp/stabilizer-v5-diff-check.log`.

- `scripts/check-troubleshooting-records.ps1` passed again. Both maps were
  regenerated and `go run ./cmd/codemap check -all` reported fresh maps. The
  direct map diff contains only the runtime source fingerprint and two new
  cluster test-file inventory entries; the path map is unchanged. Generation
  and freshness checking are repeated after this record's final write. Logs:
  `.tmp/stabilizer-v5-maps-generate.log`, `-maps-check.log`,
  `-maps-generate-final.log`, `-maps-check-final.log`.
- The authored-document scan passed all 111 relative file targets. Extending
  that supplemental scan to the generated map flagged its existing repository-
  root ADR URLs when interpreted relative to `docs/code-maps/`. All 190 URL
  strings are identical to HEAD and their root targets exist; this is a
  pre-existing generated-link/rendering limitation, not an introduced link.
  Generator repair is outside v5 and was not attempted. Map freshness does not
  establish rendered-link correctness. No new link assertion was weakened.

## Approved-item traceability

| Approved item | Implementation | Distinguishing evidence |
| --- | --- | --- |
| Corrected grid authority, equal-text origin, coherent clearing | `output_location.go`; release context captures the correction's CTY image in `output_pipeline_delivery.go` | Actual immediate/delayed policy matrix, snapshot A/B rotation, populated geographic-cache assertions, invalid/nil CTY, reopened store/cache provenance |
| Completed lookup, nonwaiting delayed consumer, immutable late completion | Existing hooks/worker remain unchanged; no direct fallback on `ok=false` | Real blocked-worker main timeout and delayed release, concurrent cached main output, late backfill readback, joined fixture cleanup |
| Fixed missing-grid output total | `stats/tracker.go` atomic/reset/getter; one common `emitSpot` increment after final age/seal and ordinary-lane decision | Real corrections on both paths, multi-sink once, empty unchanged/B controls, true queue refusal and three-rail thinning, pre/post-acceptance age crossings, concurrent reset/read/increment |
| Headless/console/help/support | Shared summary in `bootstrap.go`, unsigned row formatter, `ui/dashboard_v2.go` 14-row panes/help; current docs, support card, ADR-0343/TSR-0127/indexes | Exact full-range formatter, real overview wiring, all three 60x14 pane renders, tracker reset, record/link checks and final map freshness |

The existing corrected-DX write suppression is preserved. No confidence,
percentage, budget/check-count, accepted-learning or age policy was changed.
The source review and complete selected lane protect those surrounding owners;
they do not qualify replay parity, prior path learning, global cache freshness,
legacy data repair, external feeds or production/RF behavior.

## Closeout and remaining limits

All approved implementation slices and their ordinary/race gates are complete.
The complete native lanes, connected handoffs and analyzers passed on the final
relevant Go state; no required repair case skipped and no behavior check was
waived. Final source/diff review found no remaining runtime defect within v5.
The read-only reviewer was design-aware; the lead retains scope and validation
authority. No independent scientific-accuracy conclusion is claimed.

Cold delayed lookup and main timeout can still produce unknown location by the
owner-selected policy; the new total measures ordinary admissions with that
condition. Existing cache freshness/late hydration, historical provenance,
per-feed attribution, earlier path learning, H3 startup dependency and replay
parity remain outside this amendment. Production rates, station/RF accuracy,
whole-process performance and deployment have not been measured or established.
The pre-existing generated-map URL limitation above is a documentation followup,
not a pending approved runtime slice.

Changes remain attributable uncommitted diffs on local `main`, at unchanged base
`a6064782543e3dde94c1c67393fb853d1ca340d6`. No commit, merge, push, deployment,
production-data operation, install, elevation, UAC approval or firewall/security
change was performed. Work completed inside the approved four-hour window;
remaining final artifact command evidence is retained in the log paths above.

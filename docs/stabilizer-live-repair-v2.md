# Stabilizer Live Output Repair — Approved v2

Owner authorization: exact `Approved v2`, followed by instructions to execute
one slice at a time with validation between slices and without routine owner
involvement. Base: `93a48e5d56c700a43902b2a026a313433782dcae`, local `main`.
Commits, pushes and deployment are not authorized by this repair.

## Contract and boundaries

- Keep the strongest ordinary letter reached by an unchanged held identity,
  separately from immutable entry telemetry and fresh decision evidence.
- Percentages/availability, correction, licensing, holding and accepted learning
  use fresh evidence. C/B remain separate outcomes. Dynamic reason budgets and
  the completed-check count do not reset or become fixed at admission.
- An existing grid stays unless an accepted replacement's lookup supplies a
  different complete normalized locator. Same/missing results retain the old
  grid. Corrected observations cannot write inherited DX grid/provenance into
  the replacement cache; DE updates remain unchanged. Refresh classification
  independently and rebuild geographic caches coherently.
- Delayed lookup is cache-first/asynchronous: no disk/worker-response wait or
  additional worker/timer. Ordinary lock contention is not a hard latency bound.
- Enforce age after location preparation before acceptance, and again before
  final offers. Pre-core expiry learns/counts no terminal transition; post-core
  expiry retains once-only learning/terminal observation but offers no output.
- Add fixed counters for held age drops and terminal ordinary letters retained
  above fresh. Counts are once per qualifying path, not once per retry. Expose
  both counters in headless and all three Pipeline Quality panes, with reset.
- H3 startup initialization, replay fidelity, repair of existing databases,
  relocation/rollback of earlier path observations, deployment and RF accuracy
  qualification remain outside this first repair.

## Test-strategy disposition

A separate design-aware post-approval reviewer applied the detailed
contract-to-test review. This was not a non-steered independent model review.
No material scope blocker remained. The lead accepted these checker refinements:

1. Compute terminal telemetry without projecting the letter into the working
   Spot before accepted learning.
2. Prove PSKReporter CW/RTTY payload conversion within its existing package;
   compose with source-bearing cluster consumers without exporting a test API.
3. Assert real target cache and reopened-disk provenance, including same-grid
   derived-to-reported promotion and missing-target controls.
4. Block worker work outside cache/admission locks to prove no worker wait;
   do not claim a hard wall-clock deadline or physically stalled disk evidence.
5. Test actual pre-core and final-offer clocks with callback-crossing expiry,
   WhoSpotsMe occurrence evidence and positive controls.
6. Test concurrent counter/reset safety and quiescent zero; do not claim a
   conservation equation across legacy hold counts and terminal observations.

## Sequential execution

### Slice 1 — confidence transport and terminal retention

Implemented the finite private scalar in the existing input/heap/release/retry
ownership, with unchanged entry glyph and no retained callsign/history. Fresh
Spot confidence remains in use through delay decisions and accepted learning.
Terminal release, suppression, retry overflow and downstream thinning use the
retained projection and its once-only fixed counter. Accepted identity changes
cannot inherit the original maximum; C/B remain separate.

Evidence:

- Native Windows targeted ordinary and race runs both passed: 125 cases across
  `internal/cluster` and `stats`, zero skipped. Selection:
  `Stabilizer|CallCorrectionDeliveredRejectedConfidenceAttribution` through
  `scripts/test-windows.ps1 -Timeout 180s`, with `-Race` for the second run.
- `TestStabilizerRetentionFreshEvidenceAndReasonBudget` drives real owner
  publications and actual scheduler transfers. Two 20%-P checks are followed by
  genuine expiry and a twelve-check budget without reset. It covers fresh ?/S,
  release, suppression, retry pressure, actual thinning, TCP, sealed ring and
  reopened archive output. `TestStabilizerRetentionKeepsIntermediatePromotion`
  proves an intermediate S-to-P promotion survives a later fresh S.
- Existing attribution integration now asserts that a real retained-P retry
  ends as B/C without increasing the retention counter. Counter concurrency and
  quiescent-reset tests passed.
- On this 64-bit build the scalar fits existing padding: envelope <=120 bytes,
  heap item <=152 bytes. No new resource owner was introduced.
- Three correctness-guarded `BenchmarkStabilizerTerminalConfidence` samples:
  8.6–9.8 ns/op, 0 B/op, 0 allocs/op. This is a local helper result, not a
  production latency or memory-ceiling claim.
- Read-only source/lifecycle review found no production blocker; requested
  retained-input overflow/thinning and C/B checks were added before this gate.

Initial new-fixture failures were investigated rather than weakening runtime
freshness: backward observation clocks did not force a new publication, and
Windows UTC could remain equal across fast scheduler checks. Fixtures now wait
for the independent observation-clock boundary before publishing/expiring real
rows. A reused wire oracle also required its literal N0AAA receiver; the
population was corrected to preserve independent expected source fields.

Logs: ignored `.tmp/stabilizer-v2-slice1-pass.log` and
`.tmp/stabilizer-v2-slice1-race.log`. Earlier failing diagnostic logs remain
under `.tmp/stabilizer-v2-slice1*.log`.

### Slice 2 — corrected location and persistence

Both immediate and delayed accepted replacements now refresh classification,
retain an existing grid on same/missing lookup, and replace it only on a different
full locator. Geographic caches are invalidated/rebuilt. Fanout cannot write an
inherited DX grid or reported provenance under the replacement identity; DE
writeback remains active. Delayed preparation uses only cache-first asynchronous
lookup, with no worker response wait and no repeated path learning.

Native Windows ordinary and race selections both passed 49 cases across
`internal/cluster` and `pskreporter`, zero skipped. Selection: `CorrectedGrid`,
`HandlePayloadCorrectableModesCarrySourceGrids`, `FCCResolverCorrectionDeliveryArchive`,
`GridRetirement`, `GridWriterPendingProvenancePersistence`, `HydrateSpotPathCells`.
Tests cover real owner/scheduler correction, PSK CW/RTTY source grids, reopened
grid persistence/provenance, actual queue/archive location and country/zone/H3
fields, nil CTY, no correction, a blocked lookup worker with concurrent main
output, and an enabled predictor/report positive control proving one original
observation and no repeat on delayed preparation. Logs:
`.tmp/stabilizer-v2-slice2.log`, `.tmp/stabilizer-v2-slice2-race.log`.

### Slice 3 — age boundaries and held-drop accounting

Added strict `>` age checks before delayed evaluation, after fresh delay
evaluation, after location preparation before acceptance, and immediately before
final offers after grid writeback. Immediate decisions also recheck after
support evaluation. Instance-local clock seams do not alter source timestamps
or resolver/path clocks. A fixed held-age counter excludes unheld and initial
overflow paths. Final expiry preserves earlier accepted learning and terminal
metrics but creates no output/family witness or delayed-release count.

Native ordinary/race selections both passed 156 cases across `internal/cluster`
and `stats`, zero skipped: `OutputAge|Stabilizer|CorrectedGrid`. Focused ordinary
checks additionally passed 27 cases including the actual runtime release-consumer
shutdown barrier and the enabled path-learning test. Earlier broad history
checks passed their cases but exposed a stale fixture expectation that delayed
release counted before the final write callback; it now expects zero there and
one after final admission. New test-fixture failures were corrected by supplying
the actual telnet resolver gate and overriding the store's actual boolean query.
The enabled-path fixture also uses Spot.Clone rather than copying its sync.Once.

Read-only review found a support-query contention gap; a post-evaluation guard
and crossed/uncrossed retry/suppression controls now cover it. Exact +/-1ns,
disabled/zero/future sentinels, callback crossings, genuine P retention on final
expiry, occurrence-based history, family witnesses, real TCP/peer FIFO receipts
and reopened archive contents are covered. Logs:
`.tmp/stabilizer-v2-slice3-focused.log`, `.tmp/stabilizer-v2-slice3-pass.log`,
`.tmp/stabilizer-v2-slice3-race.log`.

A second source pass identified initial admission-lock contention on failed
enqueue. The refusal branch now samples age before learning, with a real locked
scheduler/full-queue clock-crossing test (no held count). The final focused
ordinary and race selections each passed 27 cases, including runtime shutdown,
in `.tmp/stabilizer-v2-slice3-final-edge*.log`.

### Slice 4 — diagnostics and documentation

Both fixed counters are exposed through one shared formatter in headless stats
and Overview/Pipeline/Events. Two rows survive the console's legacy-row
filter, and the default pane grows to 13 rows. Actual simulated-screen refresh
checks cover all three panes. HELP SYMBOLS, user/filter, location, age and metric
contracts are updated alongside support routing, ADR-0341 and TSR-0127.

Initial targeted ordinary validation passed seven cases across cluster, UI,
commands and stats, zero skipped. Render review then found that million-scale
counts could clip a combined row in a bordered 60-column pane. The final layout
uses two rows, and tests render large totals through 9,223,372,036,854,775,807
and back to zero in all three panes. Console `?` help defines the counters.
The final affected ordinary rerun passed 42 cases across all four packages,
including retention and age cases. Logs: `.tmp/stabilizer-v2-slice4.log` and
`.tmp/stabilizer-v2-final-targeted.log`.

## Approved-item traceability

| Item | Implementation | Falsifying evidence |
| --- | --- | --- |
| Ordinary retention, fresh decisions/learning, C/B identity | `stabilizer_confidence.go`, `stabilizer.go`, `output_pipeline_delivery.go` | Real 12-turn owner/scheduler expiry, intermediate promotion, timeout/retry overflow/thinning, B/C attribution, transport/archive output, scalar size assertions |
| Current percentage, dynamic budgets and accumulated checks | Same live handler; existing correction/delay policy retained | Genuine 20% P then expiry, no count reset, accepted correction's own share; `stabilizer_confidence_test.go`, existing attribution tests |
| Corrected location/provenance, fresh classification | `output_location.go`, accepted mutation in `bootstrap.go`, stage/context flags, `updateGridCache` | PSK CW/RTTY producer plus both consumer paths, same/different/missing/full locators, nil CTY, reopened target cache/disk provenance and DE positive control |
| Nonblocking worker-response ownership | Async-only delayed location helper | Real worker blocked while delayed and concurrent main output finish; enabled predictor/report ingress1/release0 control |
| Age and once-only held-drop counting | `output_age.go`, acceptance/retry/final boundaries, tracker atomic | Exact boundaries, real support/admission-lock/location/writeback crossings, actual held vs unheld/initial-overflow controls, occurrence learning, TCP/peer FIFO and archive/family exclusions |
| Fixed telemetry and reset | Two tracker atomics, shared headless/overview formatter, UI row routing | Counter/reset concurrency, formatter reset/nil, actual refresh/render across all panes with large totals/zero |
| User/support and durable contract | README, spot/UI/commands/domain docs, support routes/card, ADR-0341 and TSR-0127/indexes | Actual GO/CC HELP routing/width, record checker, local reference inspection, final generated-map freshness |

## Final verification

- Full native ordinary: `scripts/test-windows.ps1` passed 12,493 cases across
  59 executed packages plus 15 build-only packages (74 total), with 93 skips.
  The display-only review adjustment received the separate 42-case final
  affected rerun above; full race uses the final two-row source.
- Skips were audited: 56 external DXSpider sender/receiver/interop observations;
  12 child/subprocess helpers; 2 opt-in cache qualifications; 1 unavailable
  Windows/non-CGO H3Mapper test; 22 opt-in live, archive/sample, retained-heap,
  profile, resource, connected-performance or soak qualifications. None of the
  new targeted repair cases skipped. No external interoperability/live/soak or
  platform-backend qualification is claimed.
- `go vet ./...`, `staticcheck ./...`, and
  `golangci-lint run ./... --config=.golangci.yaml` passed. Lint's two initial
  test-only style findings (tagged switch and redundant conversion) were fixed;
  analyzers reran clean. Tools: Go 1.27.1 windows/amd64, Staticcheck 2026.2.1,
  golangci-lint 2.14.0. Logs: `.tmp/stabilizer-v2-{vet,staticcheck,lint}.log`.
- Three 300ms samples on the normal saved cluster binary: new terminal helper
  8.69-9.12 ns/op and age boundary 17.09-17.83 ns/op, both 0 B/op and 0 allocs/op.
  Existing context preparation and grid hook hit/unchanged workloads also used
  0 allocations. Existing final-delivery-plan workload measured 8 B/op and one
  allocation (130-135 ns/op); it is not claimed allocation-free. No pre/post
  whole-pipeline comparison or production p99/memory ceiling is claimed. Exact
  selection/results: `.tmp/stabilizer-v2-bench.log`.
- Troubleshooting record consistency and 11 new-document local targets passed;
  changed Go files are formatted and `git diff --check` is clean.
- Final `go run ./cmd/codemap generate -all` regenerated both checked-in maps;
  `go run ./cmd/codemap check -all` reported fresh maps. Their direct diff is
  limited to source fingerprints/file inventory and relevant ADR references.
- Full native race: `scripts/test-windows.ps1 -Race -Timeout 20m` passed the same
  12,493 cases, 59 executed / 15 build-only packages and 93 skips. The longer
  per-package allowance accommodates instrumentation on this large native lane;
  runtime configuration and timeouts were not changed. Log:
  `.tmp/stabilizer-v2-full-race.log`.
- Connected CustomSCP: `scripts/test-customscp-regression.ps1` and its `-Race`
  run both passed the actual producer/database/consumer handoff. Evidence IDs:
  `423941617e094acdb1aeac0eb0157508` (ordinary) and
  `69b3548823a44986950597eca5dfb715` (race), under
  `.tmp/customscp-regression/`; wrapper logs are
  `.tmp/stabilizer-v2-connected.log` and `.tmp/stabilizer-v2-connected-race.log`.

## Final source and scope review

The lead's fresh pass compared the approved contract, source and direct diff,
meaningful units, actual command evidence, docs, and claim limits. Separate
read-only design-aware reviews found support-query/admission-lock expiry gaps,
Spot copylocks in a fixture, and counter clipping. All were dispositioned and
verified above. This is not a claim of non-steered independent scientific
review. Scoring/model assets were unchanged and no empirical accuracy verdict
was issued.

Production Go reviewed: `internal/cluster/bootstrap.go`, `output_pipeline.go`,
`output_pipeline_delivery.go`, `output_pipeline_stages.go`, `stabilizer.go`,
`stabilizer_confidence.go`, `output_age.go`, `output_location.go`, plus
`stats/tracker.go`, `commands/processor.go`, and `ui/dashboard_v2.go`.

Tests reviewed: cluster `confidence_attribution_delivery_test.go`,
`correction_loss_delivery_test.go`, `main_runtime_output_retirement_test.go`,
`peer_original_hold_delivery_test.go`, `stabilizer_matrix_test.go`,
`stabilizer_confidence_test.go`, `corrected_grid_delivery_test.go`,
`output_age_test.go`; `pskreporter/client_test.go`,
`stats/stabilizer_retention_test.go`, `commands/help_overview_test.go`, and
`ui/stabilizer_matrix_test.go`. Runtime fixtures preserve their original
positive controls, source times, real queue ownership and persisted readback.

Documentation reviewed: README, spot/commands/UI guides, domain contract,
support source map/card/troubleshooting index, this execution record,
ADR-0341/TSR-0127 and their indexes, and the ADR-0311 refinement link. Generated
maps are checked after the final documentation write.

No runtime config/schema, persisted Spot representation, queue capacity,
shutdown owner or additional worker/timer/goroutine was introduced. The new
retained scheduler/telemetry state is two fixed atomics plus the finite scalar
carried by existing bounded ownership. Output contexts also carry transient
identity/held flags and the pipeline has one nil-by-default instance age clock.
No map/history/cleanup owner was added. No speculative refactor or
unapproved slice remains. Whole-process performance, production operation,
Linux/backend-specific qualifications, replay fidelity, existing DB repair and
earlier path-observation relocation remain unverified/outside scope as stated.

Changes remain uncommitted on local `main` at the stated base. No push or
deployment was performed.

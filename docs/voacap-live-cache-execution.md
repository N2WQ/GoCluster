# Live VOACAP cache qualification

## V3 correctness-only extraction

Exact owner authorization: `Approved v3`, 2026-10-16. Retain replacement-only
refresh and greatest typed-key equal-time eviction, using the original value
maps and linear capacity/expiry scans from `260cc74`. Incoming entries remain
unconditionally admitted. The indexed insertion-speedup targets are abandoned;
the protected 1.10 timing guard and symmetric same-binary controls remain.
No main-branch merge, push, deployment or restart is authorized by this slice.

Before extraction, all 22 changed tracked/untracked V2 files were copied and
SHA-256 verified under `.tmp/voacap-live-cache-v3/v2-source/`, with a manifest
beside that directory. Original V1/V2 binaries, raw observations and controllers
remain unchanged. Only this task's indexed implementation and diagnostics are
removed; earlier committed work on `voacap` is preserved.

The post-approval test-strategy review was read-only in inherited context, not
independent scientific review. It identified two false-green risks: old-code
nonoldest refresh shrinks the population, and old tied eviction is intentionally
nondeterministic. Timing therefore uses comparable oldest-key refresh;
candidate-only literal survivor tests prove nonoldest replacement and exact
tie victims. Common tied-insertion witnesses establish cardinality, admission
and timestamp class without asserting the new victim policy on the baseline.
Reader-blocking evidence must include tied capacity insertion, not just expiry.

| Contract | Failure stimulus and observable oracle | Validation ownership |
| --- | --- | --- |
| Replacement preserves other owners | oldest/nonoldest refresh at/below cap with older/equal/newer time; literal full survivors and incoming payload | focused policy tests |
| Greatest typed tie victim | every field, numeric 2/10, opposing later fields, reversed insertion order; literal surviving keys | focused policy tests |
| Admission and hard caps | incoming older than all existing owners, caps 1/2, repeated churn; incoming present and exact bounded size | focused policy tests |
| Original clocks and deletion | strict TTL boundary, minute throttle, backward time, firstSeen preservation, hydration and public paths | policy and existing fallback tests |
| No new warm allocations | observable comparisons and complete refresh batch; one escaping-allocation negative witness | policy allocation tests |
| No transferred performance regression | fresh paired original-baseline timing, equal work/population, raw reader intervals and AA/BB controls | common qualification fixtures/controller |

All findings are covered by the validation plan without expanding behavior.
Historical V1/V2 sections remain evidence for those exact rejected candidates.

### V3 implementation and functional validation

Production changes are confined to the two capacity-add helpers and the private
typed comparators in `voacap_live_cache_order.go`. Replacement detection occurs
only in the at-capacity branch; below capacity retains the original assignment.
Comparators read local key addresses without retaining them. No live index,
extra ownership structure, expiry change or diagnostic change remains.

The complete native Windows ordinary and race lanes each passed 70 packages
(54 executed, 16 build-only), 10,774 cases and 93 skips. Vet and Staticcheck
passed. Lint initially found three qualification-fixture style issues: context
argument order and two tagged-switch suggestions. After these local,
behavior-preserving fixes, the affected fixture ordinary and race checks each
passed 42 cases with no skips, and full lint passed with zero issues. The full
lanes remain applicable to unchanged production behavior; the affected fixture
checks cover the final style revision. Toolchain: Go 1.27.1, Staticcheck
2026.2.1, golangci-lint 2.14.0.

The original-source overlay negative check failed exactly the three at-capacity
nonoldest-replacement cases, demonstrating detection of the original unrelated
eviction defect. Current policy tests pass, including full-value replacement,
every typed tie field, unconditional admission, bounded churn, original time
boundaries and whole-batch zero allocation with a one-allocation negative
witness. The final inherited-context read-only source review found no material
correctness or scope issue; it was not an independent scientific review.

Both worker-profile fixtures completed 256 forecasts after restoring 50,000
entries. Their short, single captures are diagnostic only and do not establish
a paired performance result or a production effect. Raw profiles and manifests
are retained under `.tmp/voacap-live-cache-v3/profiles/`.

Fresh performance qualification is incomplete and invalid. Its baseline is original `260cc74`,
with byte-identical common fixtures. Build manifests bind both binaries to their
complete Go/module input sets. The bounded pilot selects work using duration,
actual overlap yield and runtime only; it cannot select on candidate/baseline
ratios. Fixed controller overhead is recorded separately from native process
time. Pilot selection was reviewed and frozen before scored execution.

### V3 timing disposition

All 116 pilot observations were valid. The preset selection chose 256 blocking
passes (minimum projected overlaps 2,012) and serial scales from the fixed
ladder (minimum projected measured duration 2.005 ms). Projected campaign
runtime was 11,947 seconds. Actual scored observations still had to meet the
1 ms serial floor and 1,000-overlap floor individually.

The first candidate serial matrix failed that validity gate:

| Case | Population | Frozen scale | Actual measured duration | Required |
| --- | ---: | ---: | ---: | ---: |
| Oldest-key replacement | 64 | 1 | 662,800 ns | at least 1,000,000 ns |
| Oldest-key replacement | 10,000 | 64 | 949,000 ns | at least 1,000,000 ns |

The pilot margin did not guarantee adequate measured duration in the scored
run. This is a measurement-validity failure, not evidence of a code regression
or a performance improvement. No paired/control verdict can be drawn from the
incomplete schedule. All-required-pass acceptance was already impossible, so
the remaining campaign was stopped rather than spending hours on observations
that could not repair the invalid matrix. No retry, top-up, threshold change,
rescore or exclusion was performed.

Four receipts completed: three valid and one invalid. Observation 004 (baseline
full-expiry overlap) retains raw output but lacks a completed receipt because
termination interrupted the controller. It is not counted complete. The
campaign process tree was stopped, its native-runner lock released, and all
frozen source/binary/controller identities reverified unchanged. See
`.tmp/voacap-live-cache-v3/timing/termination.json` for the inventory and hashes.

V3 remains uncommitted and unaccepted on `voacap`; no merge, push, deployment or
restart occurred. ADR-0310 remains Proposed and TSR-0100 remains Open. A new
qualification attempt would require a separately approved validation revision
with longer refresh batches, unchanged regression/control limits, and preserved
V3 evidence. The production extraction itself need not change to address this
measurement limitation. No production performance claim is made.

Support-agent documentation impact: Required. Package, operator and support
documentation describe the two local candidate policies and explicitly state
their unaccepted status. No YAML values, schema, model inputs or persistence
format changed.

Final troubleshooting-record, support-agent and changed-file crawler-comment
checks passed. Raw interval sums were separately recomputed for both invalid
refresh batches and exactly matched the reported durations. Final code-map and
diff checks are retained beside the other V3 validation logs. The final review
is lead-owned/inherited-context, not independent scientific evidence; the
unresolved timing qualification prevents acceptance regardless of these checks.

## V2 authority and evidence

The owner authorized Scope Ledger v2 with `Approved v2` after the failed v1
qualification below. V1 source, results and binaries remain unchanged under
`.tmp/voacap-live-cache-v1/`; the pre-revision worktree files and their hashes
were additionally archived under `.tmp/voacap-live-cache-v2/v1-source/`.

V2 defers forward delay-index updates using one conservative indexed timestamp
per existing node. A backward update below that bound repairs the heap at once.
Capacity eviction reconciles at most 32 stale roots before one linear rebuild.
Expiry first counts conservative due candidates through a bounded read-only
heap traversal. More than the existing N/log N budget selects bulk filtering
before any destructive removal. Actual timestamps still decide expiry; fresh
survivors receive reconciled index times. No retained candidate list is added.

The detailed test-strategy review requires a fresh actual-time scan oracle,
stale actual-fresh parents above expired children, all-dirty but unexpired
populations, backward/tie cases, 31/32/33 repair boundaries, B-1/B/B+1 expiry
boundaries, zero destructive dense prefix, and warmed deferred-repayment
allocation checks. Existing ownership, hydration, Wait and time-arithmetic
contracts remain required.

The common concurrent fixture uses the existing `RunFixedBenchmark` shared
primed clock, immutable timer state, explicit measured intervals and complete
reader retirement before return. Deferred-debt cases measure the complete
touch-plus-trigger cycle and the triggering request from before lock acquisition
through completion. Correctness witnesses remain outside measured intervals.
The original baseline is still `260cc74`, not the failed v1 candidate.

One unscored 64-pass pilot per variant and overlap case selected a common
scored pass count from 256, 512 or 1024 using only overlap yield and runtime.
The selection rule requires projected yield of 1,250 samples in every pilot
shape and projected process duration within the frozen timeout. Scored runs
require at least 1,000 actual overlaps individually; shortages block acceptance
without top-ups, pooling, retries or manufactured lock holds. All pilot evidence
is retained. Original timing/memory limits and symmetric same-binary controls
remain in force. Protocol, fixture, controller and binary identities are frozen
before scored observations.

V2 focused tests passed 85 cases with two opt-in qualification skips. Vet and
Staticcheck passed. Lint identified fixture spelling and parameter-order issues;
after correction the focused tests and lint passed again (zero lint issues).
The observer's independent Python checks accept valid debt/mixed receipts and
reject 15 malformed, incomplete, wrong-case, missing-pilot and measurement faults.
The ordinary serial-controller positive/negative checks also pass. The full
ordinary Windows lane passed 70 packages (54 executed, 16 build-only), with
10,773 passed cases and 94 skips. The full race lane passed the same package and
case counts (`scripts/test-windows.ps1 -Race`). Full `go vet ./...`, `staticcheck ./...`, and
`golangci-lint run ./... --config=.golangci.yaml` passed; lint reported zero
issues. These commands used Go 1.27.1, Staticcheck 2026.2.1 and golangci-lint
2.14.0. Opt-in measurement and live-engine skips do not certify those paths.

All ten pilot processes exited zero with valid witnesses. Each reported 502 to
512 actual overlaps across 64 passes. The fixed yield/runtime rule selected 256
scored passes. Original 44 serial shapes run in one process, and each of the six
concurrent shapes runs separately to bound raw-data memory and process runtime.
The frozen schedule contains 504 timing processes and 73 retained-heap processes.
Lossless streaming gzip retains every original raw interval; the summarizer
releases decoded samples after checking each report. Pilot acceptance does not
establish any performance gate. The observed v2 qualification is recorded below.

The final fixture/controller review found no blocking source defect. It was an
inherited-context review, including review of reviewer-authored fixtures, not an
independent scientific review. A later read-only production/coverage pass also
found no new correctness, ownership or scope blocker; it inherited prior
context and included reviewer-authored tests. The frozen protocol's extra fresh owner applies
to the three legacy concurrent cases; debt cases deliberately use exactly N
entries, as both producer and controller enforce. Concurrent ns/operation is
whole-workload elapsed time divided by lookup count; individual lookup latency
is reported separately from raw request intervals.

The common qualification fixture is split into serial/serialization,
concurrent/debt, and refusal-test files. Its longer pass routine keeps setup,
reader release, touch/trigger timing, joining and post-state capture in one
visible ownership sequence. That test-only size exception avoids distributing
the clock lifetime and cancellation obligations across unrelated helpers;
production retention helpers remain short. Serialization and reader lifecycle
have separately exercised helpers.

V2 implementation, validation and disposition are recorded below. The historical
v1 evidence remains intact in its own section and artifact directory.

### V2 requirement traceability

| Approved item | Implementation | Falsifiable evidence |
| --- | --- | --- |
| Cheap forward delay updates with exact eviction | `touchDelayLocked`, `normalizeDelayRootLocked`, `indexedAt` | backward/tie and 31/32/33 repair cases; actual-time scan survivor oracle |
| Select dense cleanup before destructive removal | candidate-count traversal, `expireCacheLocked`, `expireDelaysLocked` | B-1/B/B+1 cases, unchanged preflight state, zero dense-prefix removals, hidden expired-child and all-fresh debt cases |
| Preserve bounded ownership and warm allocation contract | existing node reuse, cleared removals/tails, one added timestamp per delay node | map/heap identity checks, whole-batch deferred-repayment allocation checks, retained-heap campaign |
| Repair concurrent timing evidence | common primed clock, raw request/acquisition/hold/work intervals, joined readers | lifecycle/refusal/serialization tests, independent Python receipt checks, ten-process pilot, per-observation overlap floor |
| Include transferred work and its reader impact | three touch-plus-trigger workloads | whole-cycle and trigger totals/p99, lookup and actual-overlap p99, unchanged baseline/control gates |
| Preserve external contracts | same fallback mutex, callers, TTL expressions and minute gate | ordinary/race lanes; hydration, Wait, cache-only, strict-time and oversized-duration fixtures |

### V2 observed outcome

V2 remains **unaccepted**. A separate arithmetic cross-check of all 72 serial
matrix artifacts and receipts found 26 passing, six failing and 12 inconclusive
serial metrics. It reproduced all 12 B/A, AA and BB pairs without importing the
controller, checked artifact hashes and identities, and found no discrepancy in
elapsed-time/work arithmetic. This was a separate calculation in inherited
review context, not an independent design or scientific review.

At 50,000 entries, the candidate/baseline geometric-mean ratios are:

| Operation | B/A | AA | BB | Gate |
| --- | ---: | ---: | ---: | --- |
| Rotating delay updates | 0.706891 | 0.970981 | 0.954646 | PASS |
| Forecast capacity insertion | 0.001842 | 1.027356 | 1.029195 | PASS |
| Delay capacity insertion | 0.002194 | 1.040753 | 1.065255 | PASS |
| No-expiry sweep | 0.000334 | 1.020183 | 0.918586 | PASS |
| Expire 10% | 1.654071 | 0.964417 | 0.982529 | FAIL |
| Expire 50% | 1.792782 | 0.995386 | 1.000907 | FAIL |
| Expire 100% | 2.080563 | 1.019959 | 1.007879 | FAIL |

The three insertion/no-expiry target gates require B/A at most 0.50. Protected
costs require at most 1.10, with both same-binary controls inside
`[1/1.10, 1.10]`. The dense-expiry controls pass: those regressions cannot be
classified as merely inconclusive. Other serial failures are full expiry at
N=1 (1.344414), 10% expiry at N=10,000 (1.168553), and full expiry at N=10,000
(1.310723), also with passing controls. Twelve other serial metrics remain
inconclusive and are not counted as passes.

Forward delay updates now meet their large-cache guard, and the three required
large-cache improvement targets pass. Dense expiry still fails despite choosing
the linear route before any destructive sparse prefix. The tests establish the
work bound and correct survivors; they do not establish acceptable elapsed cost.
No additional redesign or relaxation of the gates is authorized by this result.

All 504 timing processes exited successfully. Full raw-archive revalidation
found no invalid observations. The frozen timing controller reports **45 pass,
12 fail, 16 inconclusive and zero invalid** across 73 metrics. Its 44 serial
results agree with the separate arithmetic cross-check above. Every scored
overlap/debt observation met the required 1,000 actual-overlap floor; none was
retried, topped up, pooled to rescue a shortage, or dropped.

The concurrent results also block acceptance. All values below are geometric
mean candidate/baseline ratios; all use the protected 1.10 limit.

| Workload / metric | B/A | AA | BB | Gate |
| --- | ---: | ---: | ---: | --- |
| Half expiry, workload time per lookup | 1.567174 | 0.995404 | 0.996290 | FAIL |
| Half expiry, actual-overlap lookup p99 | 1.961744 | 1.036697 | 0.981365 | FAIL |
| Full expiry, workload time per lookup | 1.783795 | 1.019202 | 0.997535 | FAIL |
| Full expiry, actual-overlap lookup p99 | 2.173012 | 1.094319 | 0.914355 | FAIL |
| Touch-all then insert, complete cycle | 0.816885 | 0.999381 | 0.998188 | PASS |
| Touch-all then insert, triggering request total | 1.302333 | 0.989568 | 1.000841 | FAIL |
| Touch-all then insert, actual-overlap lookup p99 | 1.320740 | 1.025691 | 0.984889 | FAIL |
| Touch-all then fresh sweep, complete cycle | 0.736101 | 1.015263 | 1.002313 | PASS |
| Touch-all then fresh sweep, triggering request total | 0.651899 | 1.011207 | 1.005953 | PASS |
| Touch-half then mixed sweep, complete cycle | 0.772255 | 0.969565 | 0.997560 | PASS |
| Touch-half then mixed sweep, triggering request total | 0.782288 | 0.973208 | 0.995783 | PASS |
| Touch-half then mixed sweep, actual-overlap lookup p99 | 0.833158 | 0.937182 | 1.001064 | PASS |

All six overall lookup-p99 comparisons pass, including half/full expiry at
1.038074/1.096696. That does not negate the separately witnessed overlap-tail
failures: the rare blocked requests can be obscured in the overall percentile.
Touch-all/insert cycle p99 and trigger p99 are inconclusive (AA 1.111981 and
0.907353). Fresh-sweep overlap p99 and trigger p99 are inconclusive (AA 0.879825
and 0.869737). Mixed-sweep cycle p99 and trigger p99 pass. The mixed lookup
workload and its overall lookup p99 pass at 0.034849 and 0.067442. Every metric,
pair and control remains available in the canonical timing summary.

Thus a cheaper complete touch-plus-insert cycle still produces a slower
triggering request and worse overlap tail. The v2 deferred-debt checks expose
the transferred cost rather than treating the cheaper updates as sufficient.
The revised clock removes v1's invalid tail observations but does not remove
the measured regression.

All 73 retained-heap processes exited successfully with valid witnesses. A
separate read-only arithmetic check of all receipts and raw artifacts verified
identities, hashes, cardinality, payload shape and paired increments. Maximum
additional retained Go heap was 7,619,648 / 7,619,912 / 7,619,328 bytes for
1 / 8 / 24 forecast rows, respectively. The overall maximum is **7.266914 MiB**,
below the unchanged 20 MiB limit. The retained-payload fault exceeded its
baseline reference by 167,987,240 bytes (160.205116 MiB) and was rejected.
These measurements are post-GC live Go heap, not process working set or
cumulative allocations. Warm bookkeeping allocation tests passed separately.
The combined canonical summary agrees with the separate memory calculation:
all three payload shapes and the negative witness pass, with zero invalid
observations. Both timing and heap controllers completed successfully; their
process exit codes do not turn the failed timing gates into acceptance.

### V2 matched profiles and final disposition

After measurement and raw-data reconciliation finished, `profiles.ps1` and
`profile-retention.ps1` ran sequentially under the native runner lock. Both
worker fixtures and all six retention fixtures passed. The latter reports
also passed the frozen controller's workload checks: 100,000 delay updates or
eight half/full expiry sweeps per executable, at N=50,000. These diagnostic
runs are separate from the scored observations and never replace them.

The actual-worker fixture restores 50,000 forecasts, completes 256 jobs with
one worker, and verifies bounded live/disk populations and empty queues.
Baseline/candidate reported elapsed times were 0.2330424 / 0.022622 seconds.
Neither crossed a UTC hour. Baseline CPU sampling attributed 190 ms inclusive
to `addCacheLocked`, out of 210 ms total. Candidate sampling captured only
10 ms total, in the persistence path. This supports the narrow capacity-insert
benefit but cannot certify dense expiry, tail latency, or production effects.
Timed global allocation deltas were 7,369,288 / 7,371,912 bytes and 8,581 /
8,592 objects; these include the complete worker workload and are not the
isolated zero-allocation bookkeeping oracle. Allocation-profile differences
also include profiler output and must not be treated as bookkeeping deltas.

Retention CPU profiles include fixture seeding, unlike the timed qualification
intervals. Total samples were 110/110 ms for baseline/candidate delay updates,
610/760 ms for half expiry, and 460/900 ms for full expiry. Sampled inclusive
`pruneLocked` CPU was 40/70 ms for half expiry and 40/150 ms for full expiry.
In the full-expiry candidate, forecast filtering accounted for 80 ms inclusive
(60 ms attributed to map deletion), and delay filtering for 60 ms inclusive
(50 ms flat attributed to the expiry-test line). Those inclusive amounts
overlap their callers and are not additive independent totals. Full expiry
leaves no survivors, so survivor heap rebuilding cannot explain that failure.
The evidence points toward dense traversal/deletion costs, but sampling and
line attribution do not isolate one machine-level cause or establish exact
latency ratios. Allocation profiles include seeding and mutex profiles include
runtime/profiler activity; neither substitutes for the retained-heap, warmed
allocation or actual-overlap timing checks.

All artifacts are retained under `.tmp/voacap-live-cache-v2/`: pilot selection
and raw receipts; frozen timing/heap manifests and `.json.gz` observations;
canonical `summary.json`; worker and retention CPU/allocation/mutex profiles;
profile manifests/work reports; analyzer, ordinary/race and documentation logs;
and build/source identity records. Archived baseline/candidate qualification
SHA-256 values are respectively
`E93A5F68772390292CD5E5DD3F7075A35559A6F7768327BB29E56EAD1DD8D9AC` and
`980967DD721D39F847F149FFF875535E056B2DE457C19BD986596267E9185802`.
Worker binary hashes remain those recorded in `binary-hashes.txt`. All four
binary hashes were rechecked after profiling; all 103 path-reliability Go
source hashes still match the premeasurement manifest.

The fresh final pass covered the approved scope, production diff, bounded
ownership, fixtures, saved results and claim wording. No concrete production
correctness defect was found. Review was lead-owned and inherited-context,
including reviewer-authored tests; it is not independent scientific assurance.
Troubleshooting-record, support-agent and changed crawler-entry checks passed.
New untracked production-file headers were reviewed directly. Code maps were
regenerated with `go run ./cmd/codemap generate -all`; `check -all` passed.
The support impact is material: the package/operator/support guides document
the selected retention policies, live map/index diagnostic and failed v2
performance qualification.

**V2 performance acceptance and change closeout fail.** ADR-0310 stays Proposed
and TSR-0100 stays Open. Dense expiry and deferred insertion need further design
work before acceptance. No thresholds were relaxed, observations retried or
discarded, or later design silently substituted. The change remains uncommitted
on `voacap` at `260cc7437e863874894a956de4feb78b3ddbb71c`; nothing was pushed,
deployed or restarted. The historical v1 record follows.

## V1 authority and boundary (historical)

The owner selected both live retention policies and authorized Scope Ledger v1
with `Approved v1` on 2026-10-15. The starting checkout was clean, branch
`voacap`, commit `260cc7437e863874894a956de4feb78b3ddbb71c`.

The approved change covers private live forecast/delay retention, coupled
bounded indexes, existing overview diagnostics, regression/qualification
fixtures and affected documentation. It preserves disk retention and schema,
configuration, forecast calculations, caller-specific hour validity, delay
timing, request-driven cleanup cadence, worker lifecycle and memory publication
before persistence. Deployment, restart and push are outside this authority.

## Approved requirements and evidence

| Requirement | Implementation | Evidence |
| --- | --- | --- |
| Existing-key replacement cannot evict another entry | `addCacheLocked`, `addDelayLocked` | replacement, node reuse, public refresh fixtures |
| Equal-age victims are deterministic; incoming new entries are always admitted | typed key comparators and existing-root eviction | literal vectors in both insertion orders; older incoming fixture |
| One bounded index node per map entry | coupled add/remove helpers and typed heaps | identity/back-reference/capacity/tail checks, churn, negative stale-tail witness |
| Preserve TTL, minute gate, backward clocks and validity distinctions | original expiry expressions and fallback callers | strict-boundary, oversized-duration, partial-window, cache-only, hydration and Wait fixtures |
| Sparse expiry avoids unrelated scanning; dense expiry stays linear | bounded root removals followed by in-place filtering/rebuild | crossover work counts and payload-release checks |
| Operator can compare live map and index populations | snapshot and separate overview line | unequal nonzero diagnostic fixture |
| No added warm bookkeeping allocations | node reuse, in-place repairs and filtering | whole-batch zero-allocation checks with an escaping-allocation negative witness |

## Acceptance measurements

The exact baseline source is archived locally. A common qualification fixture
must compile against both baseline and candidate without changing baseline
production code. Measurements use native Windows stable executable paths,
recorded executable and fixture hashes, fixed workloads and fixed process
settings. The detailed protocol and all raw results are retained under
`.tmp/voacap-live-cache-v1/`; failed and invalid observations are retained.

The approved gates are:

- At 50,000 entries, full-cache insertion and genuine no-expiry sweeps must be
  at least twice as fast (candidate/baseline ratio at most 0.50).
- Protected small-cache operations, warm hits, delay updates and large expiry
  must stay within a 1.10 paired timing ratio.
- Concurrent lookup p99, including the separately witnessed requests actually
  overlapping expiry, must stay within a 1.10 ratio.
- Warm bookkeeping must add no allocations for hits, replacement or delay
  updates.
- With both maps full at 50,000 and identical payloads, net additional retained
  Go heap must be at most 20 MiB.
- Measurements use 12 balanced AB/BA pairs, symmetric same-binary controls and
  explicit invalid-run rules. No silent retry, outlier deletion or gate change.
- Controlled worker/lookup profiles complement microbenchmarks. External
  VOACAP execution and production client latency are not established by these
  local fixtures.

## Review and validation status

Implementation is present but **v1 performance acceptance failed**. Targeted correctness
and allocation tests passed. A separate inherited-context code review found no
production correctness defect and requested additional hydration/Wait,
oversized-duration, insertion-order and warm-hit allocation coverage; these
test refinements were accepted. This was not a non-steered scientific review.

The complete ordinary Windows lane passed: 70 packages (54 executed, 16 build
only), 10,718 passed cases and 94 skipped cases. Command:
`scripts/test-windows.ps1`. Opt-in live-engine and measurement fixtures are not
certified by ordinary skipped cases. The new qualification fixtures are run
separately below. Vet, Staticcheck 2026.2.1 and golangci-lint 2.14.0 passed under
Go 1.27.1; lint reported zero issues. The support-agent checker and changed-file
crawler-comment check passed. New untracked owner/heap file headers were also
reviewed directly because that checker enumerates tracked files.

The common qualification test file exceeds the usual review-size target. It
keeps the shared old/new fixture, operation witnesses, timed boundaries and
raw-interval schema together so neither executable can silently use a different
oracle. Its performance, overlap and retained-heap routines are separate; the
external controller owns process orchestration and acceptance arithmetic.

The harness review added an actual pre-sweep throttle witness and isolated
malformed per-case reports before measurements. Pure controller checks accept a
valid synthetic matrix and reject a nonzero pre-sweep clock and malformed case
while preserving other cases. No performance observations were collected before
these corrections. The final protocol freezes 47 workloads and 52 timing
metrics, 12 AB/BA pairs, 12 AA pairs and 12 BB pairs. Process group order rotates;
workload order reverses on alternating pairs. Timing ratios use paired geometric
means; both same-binary controls must remain between 1/1.10 and 1.10. Otherwise
the metric is inconclusive. Whole-process failures block campaign acceptance,
even when an independently valid case remains diagnostically useful.

The complete race lane also passed: 70 packages, 10,718 passed cases and 94
skipped cases (`scripts/test-windows.ps1 -Race`). The final targeted ordinary
cache check passed 30 cases with two opt-in qualification skips. Targeted vet,
Staticcheck and lint passed after the final test-witness changes. Warm public
hits, replacements, delay touches and expiry batches passed the exact-zero
allocation checks in ordinary and race runs; their escaping-allocation negative
witnesses were detected.

## Qualification outcome

All 72 timing processes and 73 heap processes exited successfully and passed
their workload witnesses. No observation was retried or dropped. Across the 52
timing metrics, 18 passed, 15 failed, 16 were inconclusive and three were invalid.
An invalid or inconclusive guard is not a pass. Overall acceptance is blocked.

The following are paired geometric-mean candidate/baseline ratios at 50,000
entries. Smaller is faster. AA and BB are second/first ratios for unchanged
baseline and unchanged candidate binaries, respectively.

| Workload | Ratio | AA | BB | Result |
| --- | ---: | ---: | ---: | --- |
| Warm cache hit | 0.96585 | 0.97069 | 0.98299 | Pass, limit 1.10 |
| Rotating delay updates | 2.02524 | 0.92129 | 1.06556 | Fail, limit 1.10 |
| Repeated update of one delay | 0.71117 | 1.00458 | 0.92747 | Pass, limit 1.10 |
| Replace oldest existing entries | 0.00173 | 1.01543 | 0.91531 | Pass, limit 1.10 |
| New forecast at capacity | 0.00228 | 1.06276 | 1.12477 | Inconclusive control, target 0.50 |
| New delay at capacity | 0.00254 | 1.06009 | 0.84276 | Inconclusive control, target 0.50 |
| Actual no-expiry sweep | 0.00031 | 1.00149 | 0.84887 | Inconclusive control, target 0.50 |
| One expired entry per map | 0.00327 | 1.02017 | 1.09463 | Pass, limit 1.10 |
| 10% expiry | 2.65727 | 1.00117 | 1.02431 | Fail, limit 1.10 |
| 50% expiry | 2.29051 | 1.09043 | 1.01709 | Fail, limit 1.10 |
| 100% expiry | 2.31444 | 0.97348 | 1.03737 | Fail, limit 1.10 |
| Mixed replacement/lookup total time | 0.02704 | 1.01226 | 1.06716 | Pass, limit 1.10 |
| Lookup with 50% expiry, total time | 1.94499 | 0.99622 | 0.97556 | Fail, limit 1.10 |
| Lookup with 100% expiry, total time | 2.05921 | 0.98238 | 1.00679 | Fail, limit 1.10 |

Other failed guards include one-entry cache/delay insertion, the 64-entry warm
hit and rotating delay updates, 64-entry half expiry, and 10,000-entry rotating
delay updates and 10%/50%/100% expiry. The full machine-readable summary retains
every ratio and control, including near-boundary results.

All three overall concurrent-lookup p99 comparisons are invalid because some
processes recorded a zero p99. Raw intervals retain identical start/acquired/end
readings for unresolved fast operations. Zero means the observer could not
resolve latency, not that the operation took no time. The fixture uses
`time.Since`; inspected Go 1.27.1 Windows amd64 source reads shared interrupt time.
Its supported `testing.B.Elapsed` precision path differs (see TSR-0097).
The actual-expiry-overlap p99 comparisons are inconclusive: half-expiry ratio
1.99961 has BB control 0.90401, and full-expiry ratio 2.33693 has AA control
0.87739. These results do not certify the 1.10 tail guard.

Retained heap passed all three payload shapes at 50,000 entries in each map:

| Forecast rows | Largest paired additional HeapAlloc | Limit |
| --- | ---: | ---: |
| 1 | 6,819,648 bytes | 20,971,520 bytes |
| 8 | 6,814,664 bytes | 20,971,520 bytes |
| 24 | 6,819,544 bytes | 20,971,520 bytes |

The largest observed increase is 6.504 MiB. The deliberate uncoupled-deletion
fault exceeded the paired baseline reference by 166,387,572 bytes and was
correctly rejected by the same 20 MiB budget observer. These are retained Go
heap results under the controlled fixture, not process RSS or production peaks.

## Controlled worker and caller lookup profiles

The existing `TestVOACAPPersistenceWorkerProfile` ran once per frozen variant at
the stable cluster test-binary path, with GOMAXPROCS=4 and GOGC=100. Each restored
50,000 forecasts, completed 256 actual worker jobs through the Wait API, checked
each published cache hit, retained 50,000 live/persisted entries, and reported
zero failed jobs or persistence warnings. It used the fake forecaster rather
than the external VOACAP program.

The baseline manifest reports 0.2514487 seconds and 220 ms sampled CPU;
`addCacheLocked` accounts for 190 ms inclusive (100 ms flat). The candidate
manifest reports 0.0137678 seconds and only 10 ms sampled CPU in total. That
short candidate sample supports no fine-grained CPU attribution. The controlled
completion workload improved, but it does not supersede the failed delay and
expiry gates, establish production latency, or repair the invalid p99 observer.
Allocation, heap, mutex and block profiles are retained beside both manifests.

## Disposition and provenance

Keep ADR-0310 Proposed and the candidate uncommitted. Both selected policies have
correctness coverage, but this indexed implementation does not meet the approved
performance contract. The next design investigation should reduce routine delay
repair and dense-expiry work, and validate a sufficiently precise tail observer
before any new frozen campaign. No limit has been relaxed; no merge, push,
deployment or service restart occurred.

Durable learning is recorded in [TSR-0100](troubleshooting/TSR-0100-live-cache-qualification.md).
Fresh source/evidence review was lead-owned with a separate inherited-context
reviewer; neither is described as an independent scientific oracle.

Artifacts: `.tmp/voacap-live-cache-v1/source-manifest.json`, `protocol.json`,
`timing/manifest.json`, `heap/manifest.json`, `summary.json`, all raw JSON and
receipts, ordinary/race/analyzer logs, and `profiles/{baseline,candidate}`.
The exact tracked baseline archive and common fixture are retained. Final
qualification executable SHA256 identities are:

- Baseline: `02bd185b93a11cba44f112dd441dc94d440200e12bfcb329e0f21d0457bb46c4`.
- Candidate: `46546338df61aca8191c252e0912f451099578764ce6466c2d5acab1f4022036`.

Documentation closeout and generated-map checks cannot change this failed
performance acceptance verdict.

Support documentation impact: required. The new live ownership diagnostic,
replacement policy, tied eviction and failed qualification status are reflected
in `customgpt/support-cards/path-reliability.md`, the operator guide and the
package README. No support action schema or routing change is needed.

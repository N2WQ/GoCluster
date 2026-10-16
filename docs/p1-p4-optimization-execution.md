# Archive and resolver optimization execution

## Approved Scope Ledger v2

Owner authorization: `Approved v2`, 2026-10-16, on branch `voacap` at the
unchanged baseline below. This section supersedes v1's P1-before-P4 gate and
statistical performance prerequisites. The v1 sections below remain historical
evidence, not current blocking instructions. P1 remains a separate implementation
checkpoint with incomplete original qualification. The owner selected P4 next,
followed by whole-cluster production profiling to judge practical benefit.

P4 production scope remains solely `spot/signal_resolver.go`: sweep retains its
first prune, retirement/empty/loss checks and evaluation scheduling; attach the
generation and call `evaluatePrunedKey` when evaluation is due. Standalone
`evaluateKey` retains nil/attach/prune preparation. Move the scoring/publication
body unchanged. Preserve receiver references, hysteresis, generations, immutable
publication, cleanup and caps. No retained state, locks or configuration is added.

Tests/benchmarks are confined to `spot/resolver_sweep_test.go` and
`spot/resolver_sweep_bench_test.go`; update `spot/README.md`, this record and
affected generated maps. Preserve the P1 checkpoint. Clock repair, VOACAP,
dependencies, model changes, production instrumentation, commits, deployment
and service restarts are excluded.

Validation: literal expiry/evaluation boundaries, due/not-due scheduling,
loss-control publication, generation attachment/retirement, captured-snapshot
immutability, standalone pruning, direct scoring-body preservation, guarded
representative benchmarks and allocation comparison. Benchmarks must evaluate
the intended intact population each iteration. No AB/AA/BB confidence-interval
campaign is required. Target: no additional sweep allocation, retained state or
lock; remove only the redundant prune traversal. Timing is diagnostic, not
production acceptance.

Run targeted tests, full native ordinary/race and static checks, affected Linux
ordinary/race, final direct diff review and fresh verification. If the native
runner stops early, run all remaining selected packages. The existing unresolved
baseline clock-test failure is a named exception allowing progression, never a
passed check. Its generic callback-failed message does not prove that every
recurrence has the same underlying cause. Investigate a changed signature or
any additional failure; no other failure is waived.

A separate read-only, design-informed scope/test-strategy challenge required
complete remaining-package coverage, intact benchmark populations, literal
boundaries, not-due loss controls and immutable generation checks; all are in
scope. This is not a non-steered scientific review. No model interpretation or
scientific claim changes. Existing ADRs remain authoritative; no new durable
architecture is selected. Support-agent documentation impact: Not required;
operator commands, configuration, diagnostics and troubleshooting contracts do
not change.

Production acceptance remains pending after local implementation. Compare
identified binaries under comparable warmed workloads/settings, examining CPU,
allocation rates, live heap, GC, throughput and queue/drop behavior. Profile
comparison alone does not establish actual delivery latency; that requires
delivery measurements. This ledger does not authorize a service launch/restart.

### V2 execution

Approval was received before P4 mutation. P4 is implemented; current v2 results
and remaining limits are recorded at the end of this document. Original P1
evidence remains below.

Before implementation, detailed read-only test-strategy review found no need for
a production seam. The lead adopted each refinement: use Sweep rather than
Step, cover both scheduling gates and the distinct loss-control branch, use
coherent shared-receiver references, compare literal outputs, reset benchmark
evaluation markers each iteration, and preserve the scoring-body proof.

| Contract | Falsifying stimulus | Observable check |
| --- | --- | --- |
| Preparation preserved | Direct and sweep evaluation with cutoff-equal and one-nanosecond-old rows; expired stable winner and shared receiver | Literal ranks, scalar support, complete rows, reference counts, repaired winner/pending state and attached generation |
| Scheduling preserved | nextEvalAt equality/future; clean elapsed interval just below/equal boundary; dirty due key | Exact lastEvalAt/nextEvalAt/dirty state and whether publication changes |
| Loss controls preserve authority | Pruned loss-affected population while not due versus due-but-clean | Coherent control only on the existing not-due branch; ordinary time/hysteresis unchanged |
| Retirement/recreation preserved | Nil/empty, TTL equality/older, process-idle just before/equal deadline | Owner and snapshot index removal, terminal captured cell, exact new single-key generation and immutable captured rows |
| One evaluation per due sweep | Challenger requires two genuine windows | First sweep advances one pending win; second settles |
| Complete benchmark work | Forced due/dirty, distinct evaluation marker, prior snapshot captured every iteration | Entire key/candidate/receiver population, exact support totals, current evaluation and fresh publication; expiry replenished every iteration |

Post-implementation read-only Go review found no production correctness or scope
issue. Three checker/documentation findings were resolved before final native
lanes: copy captured receiver rows by value (and prior ranks), require fresh
snapshot pointers per benchmark iteration, and describe pruning after the early
retirement checks accurately. Review verified these changes. Source-aware lead
review and the mechanical inverse-change proof confirm no scoring/publication
statement changed. The existing linear scoring body remains together to avoid
an unrelated state-machine rewrite. No retained-state owner, allocation site,
lock, lifecycle mechanism or cap was added to production code.

### Production comparison after local validation

Preserve the current production executable and baseline captures before upgrading.
Record executable hash/build metadata, process identity/start time, configuration
and Go runtime settings for each run. Use the existing profiling collector with
comparable profiling overhead and warmed observation windows; do not compare a
fresh process directly with a long-running warmed cache. Record input rates and
active clients/keys to explain workload differences. Compare CPU, allocation
rates over equal intervals (not cumulative totals from different process ages),
live heap and GC, together with existing throughput/queue/drop evidence. Inspect
the archive encoder and resolver pruning stacks as well as whole-process costs.
Attribute any observed total change to the combined candidate and state/load
differences; this rollout alone cannot separate P1 from P4 effects. Delivery
latency remains unmeasured unless actual delivery observations are available.
No service was launched or restarted as part of this work.

## Approved Scope Ledger v1

Owner authorization: `Approved v1`, 2026-10-16. Original branch `voacap`,
baseline `c0a27149d1445adaa8b456ce0402b2f11e659f87`; initially clean.
Authorization covers two sequential local slices. P1 must pass its complete
local qualification and fresh final review before P4 implementation begins.
No commit, merge, push, deployment, service restart or production acceptance is
authorized. Source checkpoints and separate patches preserve each boundary.

Objective: remove archive record-buffer allocation and duplicate resolver sweep
pruning while preserving stored bytes, shared input ownership, resolver
decisions, bounded resources and operational behavior.

### P1 production boundary

Only `archive/archive.go`: one flush-local byte buffer; private
`encodeRecordInto`; existing `encodeRecord` remains an independently owned
result wrapper. Retain per-row `Spot.Clone` and `EnsureNormalized`. Keep
Pebble `Batch.Set`, complete version-6 encoding, key/sequence generation,
logging, commit options, queue/drop/batching/retention and shutdown semantics.
No writer field, pool, cache, goroutine, timer or lock is added. The largest
scratch allocation belongs to one flush and cannot survive through a writer
field. Pinned Pebble v1.1.5 copies supplied key/value bytes before Set returns.

The linear encoder remains together to preserve field ordering and avoid a
style-driven codec rewrite. ADR-0281 and TSR-0080 remain authoritative.

### P4 production boundary

Only `spot/signal_resolver.go`, after the P1 gate: extract the evaluation body
following preparation into `evaluatePrunedKey`. Standalone `evaluateKey`
retains nil/attach/prune preparation. Sweep keeps its first prune, all
retirement/empty/loss checks, then attaches the generation and evaluates the
prepared key. Move scoring/publication without modification. Preserve
scheduling, dirty state, EvaluatedAt, hysteresis, authority, receiver references,
loss controls, publication and cleanup. Existing caps and retained ownership
remain unchanged. ADR-0117, ADR-0280 and ADR-0292 remain authoritative.

### Documentation and exclusions

New tests and benchmarks are confined to `archive/encoding_scratch_test.go`,
`archive/encoding_scratch_bench_test.go`, `spot/resolver_sweep_test.go` and
`spot/resolver_sweep_bench_test.go`. This record holds authority and evidence;
P4 also updates the resolver README. Regenerate affected code maps from source.
Ignored artifacts may contain frozen overlays, measurements and orchestration.

P2, VOACAP, configuration, dependencies, model changes, new retained state,
production instrumentation and unrelated cleanup are excluded. Preserve
unrelated edits. Stop for revised authority if scope, material risk or required
validation changes. A failed P1 gate blocks P4. Preserve a qualified P1
checkpoint if P4 fails and retain all failed evidence for disposition.

Support-agent docs impact: Not required under this scope; commands, settings,
diagnostics, troubleshooting instructions and operator contracts do not change.
No new durable architecture decision or troubleshooting record is expected.

## Validation contract

Each slice requires targeted tests, complete native Windows ordinary and race
lanes, vet, Staticcheck, golangci-lint, affected-package Linux ordinary/race,
guarded benchmarks/profiles, direct diff review and fresh final verification.
Use the native stable-path runner, including PrepareOnly before full runs.
Require critical selected cases to RUN and PASS. Record failures and skips.
Review fixes rerun affected checks and invalidate broader evidence when needed.
Code-map generation is the final source/documentation generation step, followed
by freshness and whitespace checks. No live service changes are implied.

P1 compares original baseline against P1 only. P4 compares the qualified P1
state against P1+P4. Common baseline benchmarks must not name candidate-only
helpers. Candidate-only ownership and allocation tests run separately.

Freeze sources, binary hashes, build/runtime settings, workloads, schedule and
analysis before scored execution. Per case: seven alternating AB/BA pairs,
seven AA pairs and seven BB pairs, with at least one second measured per sample.
Record unrounded elapsed nanoseconds and operation counts outside timing.
Analysis uses geometric mean paired ratios and a 95% Student-t interval on
their logarithms (seven pairs, six degrees of freedom). Unchanged-binary
control intervals must lie within [1/1.10, 1.10]; timing guard upper bounds
must not exceed 1.10. These are approved local engineering acceptance limits,
not a repository-wide rule or a production-latency guarantee.

P1 primary acceptance: supported lower bytes/op and allocs/op for normalized
64/3000-row complete Pebble flushes; no warmed buffer allocation with sufficient
capacity; passing timing guards including one-row controls; paired allocation
profile attribution. Measure normalized/mixed 1/64/3000 rows. Fixture setup is
outside timing; fixed keys and every final stored record are checked.

P4 primary acceptance: the predefined 64-key, two-candidate, eight-reporter
whole-sweep timing interval must be below 1.0; allocation neutrality, secondary
timing guards and CPU attribution must pass. Also measure 1x1x1, 64x16x64,
1000x2x8, standalone controls and replenished expiry-heavy fixtures. Every timed
sweep must perform its intended evaluation with current timestamps and intact
expected population. Calibration must not outlive fixture freshness.

No selective dropping, resampling, method adjustment or silent retry after
invalid/inconclusive/failing evidence. Such evidence does not pass a gate.
Timing and profile runs are separate. Use temporary synthetic databases only.
Passing local evidence is not runtime confirmation: matched production CPU,
allocation/heap, queue/drop and delivery-latency evidence remains separate.

## P1 post-approval test-strategy review

A separate read-only reviewer inspected the approved design and current tests.
This is design-informed adversarial review, not non-steered scientific evidence.
Lead disposition: all P1 findings are covered below; implementation may proceed.
The detailed P4 review must run before that implementation slice.

| Contract | Failure/boundary and stimulus | Observable oracle / evidence | False-green risk | Checker / owner |
| --- | --- | --- | --- | --- |
| Complete overwrite and capacity reuse | Poisoned zero/undersized/exact/excess storage; rich/empty/rich; flags 15 to 0; wrapper/flush also exercise nil | Literal v6 bytes, exact length, same backing pointer when sufficient / unit | Decoder normalization hides writer drift | TestEncodingScratchOverwriteAndCapacity / archive |
| Wrapper owns results | Two results and independent scratch, then mutation | Other results remain literal expected bytes / unit | Equal initial bytes conceal aliasing | TestEncodingScratchOwnedResults / archive |
| Zero warmed buffer allocation | Varied fitting records; one sparse escaping allocation witness | Raw MemStats.Mallocs delta zero; witness at least one / allocation | AllocsPerRun integer averaging hides rare allocations | TestEncodingScratchNoAllocations / archive |
| All stored rows survive buffer reuse | Mixed long/short inputs across two flushes | Every stored row matches literal bytes; public input unchanged / integration | Checking only final row misses aliasing | TestEncodingScratchMixedFlushes / archive |
| Shared fanout remains immutable | Concurrent reader and real TCP/peer/archive consumers | Original fields and delivered identity unchanged / race + integration | Race execution without concurrent stimuli | TestFlushNormalizedOwnedInputConcurrentReaders; TestArchiveImmutableFanoutDelivered / archive + cluster |
| Queue/drain/reopen preserved | Full queue, shutdown across batches, reopen | Accepted rows present, dropped row absent / integration | Successful enqueue mistaken for persisted receipt | TestOwnedQueueDropsFullAndStopDrainsMultipleBatches / archive |
| Whole-flush resource benefit | Real guarded Pebble flush and separate profiles | Lower allocation traffic with all correct rows / benchmark + profile | Helper-only gain or empty-work benchmark | BenchmarkEncodingScratchFlush / archive |

Checker refinements: the old literal helper hardcodes POTA/flags7/TTL3/numerics,
so new flag/empty cases have explicit literal expectations. A zero Spot retains
UNKNOWN toxicity: 74 header bytes plus seven payload bytes, total 81. Exact
zero allocation uses raw totals, not integer-truncated AllocsPerRun averages;
environmental allocation contamination fails the check. No production seam or
instrumentation is introduced.

## P1 observed checks and blocking failure

Production implementation is confined to the approved archive file. A direct
mechanical comparison confirms every preexisting codec statement is unchanged
after substituting buffer selection; `p1/codec-body-proof.json` records it.
Read-only Go quality review found no material finding. A nonblocking nil-table
description was clarified: poisoned capacity-zero storage is nonnil, while the
wrapper and actual flush separately exercise nil. No production fix resulted.

Observed successful checks:

- Initial native targeted ordinary: 16 passing cases, no skips.
- Targeted native archive/actual-fanout race: 17 passing cases, no skips.
- Full vet and Staticcheck: exit zero; full lint: exit zero, zero issues.
- Linux Go 1.27.1 ordinary/race: archive package and exact actual fanout test
  passed. The initial non-login WSL launcher could not find Go; corrected login
  invocations executed the tests. Both launcher and successful logs remain.
- Complete native race lane: exit zero, 70 packages (54 executed, 16 build-only),
  10,782 passing cases and 93 skips. The full log preserves the skip identities.
  This successful race lane does not replace the failed ordinary lane.

The prepared full native ordinary run stopped with exit one in
`dxcluster/internal/testsupport`: `TestFixedBenchmarkClockSharedDomain` reported
`fixed-benchmark-refused: callback-failed` from its child. The wrapper output
does not identify the underlying inner clock assertion; its root cause is
unresolved. This is a failed selected lane, not a full ordinary pass.

One isolated diagnostic passed. A subsequently predefined 50-run diagnostic
batch on the unchanged package produced 47 passes and three failures (exit one).
All package sources plus go.mod/go.sum were checked byte-for-byte, after line
ending normalization, against the pinned baseline. Its nonstandard dependency
closure contains only itself and golang.org/x/sys/windows; it cannot import
the changed archive implementation. `p1/clock-baseline-diagnostic.json` binds
those hashes, the saved executable and all diagnostic outcomes. No assertion,
timeout, benchmark-clock helper or other source was changed to obtain a pass.

P1's full qualification gate is blocked by this recurring baseline failure.
Remaining authorized P1 evidence is being collected, but P4 must not begin.
Repairing the clock subsystem would require separately agreed scope; these
diagnostic observations are not a waiver or an explanation of its root cause.

## Measurement preparation

The frozen controller hashes 3,719 selected compiled-source/module/controller
inputs, including the shared byte oracle. It verifies source/binary identity,
baseline original bytes, exact schedule, six unique results, raw elapsed/N
receipts, minimum duration, inherited GODEBUG, and raw-data reanalysis. Pure
fixtures accepted a valid matrix and rejected duplicate/missing cases and a
short duration. Both unscored one-operation smoke matrices passed.

The reference environment is native windows/amd64 Go 1.27.1, CGO enabled,
GOMAXPROCS=2, GOGC=50, GOMEMLIMIT=1536MiB, GOFLAGS=-mod=readonly and empty
GODEBUG. Scored samples request two seconds to provide margin above the
approved one-second measured-duration floor. Candidate-only helper tests are
excluded from baseline compilation through an empty test-file overlay.
Both source-input sets are checked before and after builds and around runs.

Controller inspection found no remaining material finding after these
checker-only refinements. The initial controller enumeration failed decoding
UTF-8 JSON with the Windows default code page; explicit UTF-8 repaired that
unscored preparation step. Both logs remain. No scored observation was retried.

## Fixed-work allocation attribution and deferred timing

Both frozen binaries completed the exact guarded normalized64 profile case
with N=128 and memory sampling rate one. Raw duration/N receipts, binary/input
hashes, profiles and focused byte summaries are under `p1/profiles/`.

| Allocation site | Original baseline | P1 candidate |
| --- | ---: | ---: |
| Flat encoder allocation | 1,475,584 bytes | 23,056 bytes |
| Spot.CloneWithComment | 7,512,064 bytes | 7,512,064 bytes |

These process profiles also include benchmark calibration, setup and warm
flushes. The buffer-site reduction supports the intended ownership mechanism;
unchanged clone allocation is consistent with retaining private normalization.
The instrumented benchmark lines report 321 versus 258 allocations per 64-row
operation. They are one fixed-work diagnostic pair, not the approved repeated
statistical campaign, a retained-heap result or production resource savings.
Do not use their instrumented ns/op values as a speedup claim.

The seven-pair AB/AA/BB timing campaign was deliberately not started. The full
ordinary prerequisite failed, and its recurring clock check shares Go's Windows
QPC timing substrate with B.Elapsed. The failure does not automatically
invalidate long-duration measurements, but that substrate is not independently
cleared. Stable same-binary controls would not exclude a shared systematic
error. Whole-operation P1 readings also differ from the failing test's live
readings and two-millisecond wait; no cause is inferred from that distinction.

Read-only source inspection explains the missing assertion detail: programmatic
testing.Benchmark uses a discard writer in Go 1.27.1, so inner b.Fatalf output
is lost before the helper emits generic callback-failed. A bounded diagnostic
overlay that prints existing branch values without changing assertions is a
possible next investigation, not work authorized or performed in this slice.

No statistical timing verdict or complete P1 performance-gate pass is claimed.
The prepared controller and successful one-operation smoke receipts remain
available for a later valid campaign; no scored samples were omitted or rerun.

## Approved item mapping and disposition

| Approved item | Implementation | Observed validation / limit |
| --- | --- | --- |
| Flush-local encoding reuse | archive/archive.go, private helper and owned wrapper | Literal bytes/capacity/ownership tests, raw allocation counter, guarded real flush and fixed-work allocation profiles |
| Preserve immutable fanout and persistence | Clone/normalization, Set and commit unchanged | Existing input, queue/drain/reopen, native/Linux actual fanout and race checks |
| P1 complete local gate | Ledger above | Blocked: recurring baseline ordinary-lane failure; scored timing campaign deferred |
| P4 prepared evaluation | No implementation | Gated by P1; original resolver source/README hashes still match |
| Documentation/review | This record, direct codec-body proof, read-only Go quality review | No material source finding; final map/diff checks recorded in artifacts |

Decision disposition: existing ADR-0281/0117/0280/0292 are preserved. No new
durable architecture or operator policy is introduced. The clock failure is
retained here as unresolved validation evidence, without asserting a root cause
or changing the separate clock subsystem's decision/troubleshooting records.
Support-agent docs impact remains Not required for the implemented P1 scope.

Fresh lead verification confirms the source is confined to P1, the original
codec body is preserved, no new long-lived owner exists, all claimed checks
have logs, the failed/skipped/deferred evidence is explicit, and P4's gate has
not been bypassed. A read-only reviewer supplied design-informed evidence;
neither it nor this lead pass is an independent scientific review.

## V1 execution status (historical)

P1 source implementation is complete, but qualification is blocked. P4 has not
started. No commit, push, merge, deployment or service restart was performed.
Artifacts: `.tmp/p1-p4-v1/`; original source and authority hashes were captured
before production edits. No production performance claim is made.

## V2 local execution results

Current evidence is under `.tmp/p1-p4-v2/`. Production diff: one sweep call-site
substitution and one private helper boundary/comment in `spot/signal_resolver.go`
(nine added lines, one replaced call). `production-proof.json` mechanically
reconstructs the complete original file by reversing only those substitutions;
the scoring/publication body is identical. The archive implementation and both
P1 test files still match the preserved v2-entry hashes exactly.

Observed local checks:

- New targeted native ordinary: 16 passing cases, no skips. After final oracle
  refinements, targeted native race: 16 passing cases, no skips; all four exact
  required top-level test identities RUN and PASS.
- The same final tests pass on a baseline binary built with the original resolver
  source through a Go overlay. Both one-operation benchmark smoke matrices pass.
- Native full PrepareOnly: 70 packages, 16 build-only. Full ordinary: exit zero,
  54 executed plus 16 build-only, 10,798 passing cases and 93 skips. The formerly
  failing clock test passed in this run; the earlier intermittent failure remains
  unresolved. A pass here is not a clock repair or a root-cause finding.
- Native full race: exit zero, the same 54 executed plus 16 build-only packages,
  10,798 passing cases and 93 skips. Both full native lanes cover the complete
  70-package inventory; no remainder invocation was needed. The clock test also
  passed under race, so the named progression exception was not applied in either
  v2 lane. The ordinary skip multiset matches the earlier P1 full-race run exactly.
- Full vet, Staticcheck and golangci-lint: exit zero. After test-oracle refinements,
  all three affected `spot` checks also pass. Toolchain: native Go 1.27.1,
  Staticcheck 2026.2.1 and golangci-lint 2.14.0.
- Linux/WSL Go 1.27.1: full `spot`/`archive` ordinary and race, selected actual
  cluster fanout/resolver-loss ordinary and race, and final refined new sweep
  tests ordinary and race passed. Commands and actual package outputs are in
  `linux.log` and `linux-final-targeted.log`.
- Changed support-critical comment check: one production file scanned, pass.
- A production candidate executable built successfully at
  `.tmp/p1-p4-v2/gocluster.exe`; its SHA256/build metadata are saved. Embedded VCS
  metadata identifies the original revision plus modified worktree; it is not a
  committed release. The existing executable and service were not replaced.

### Diagnostic allocation comparison and retained alert

`benchmark-protocol.json` predetermines three 100-operation samples per binary,
six cases, order AB/BA/AB, GOMAXPROCS=2, GOGC=50, GOMEMLIMIT=1536MiB. Source,
binary and baseline-overlay hashes are checked around every process. Each sweep
is guarded for complete population, a fresh evaluation marker and a new snapshot
publication. Expiry input is replenished outside timing. Work includes identical
guard/setup costs where documented in the benchmark. The native race lane ran
concurrently. Timing is diagnostic only, with the unresolved historical clock
failure as an additional limitation. No timing pass/fail or speedup is claimed.

| Case | Baseline B/op, three samples | Candidate B/op, three samples | Printed allocs/op, both binaries |
| --- | --- | --- | --- |
| 1 key x 1 candidate x 1 receiver | 376, 376, 376 | 376, 376, 376 | 2 |
| 64 x 2 x 8 | 39,424, 39,424, 39,424 | 39,424, 39,424, 39,424 | 256 |
| 64 x 16 x 64 | 39,424, 39,424, 39,424 | 39,424, 39,424, 39,424 | 256 |
| 1000 x 2 x 8 | 616,001, 616,000, 616,000 | 616,000, 616,000, 616,000 | 4000 |
| Standalone 2 x 8 | 616, 616, 616 | 616, 616, 616 | 4 |
| Replenished expiry 64 x 2 x 8 | 138,241, 138,240, 138,240 | 138,240, 138,240, 138,242 | 1088 |

All guarded workload processes pass. The separate controller's deliberately
conservative `max(candidate bytes) <= min(baseline bytes)` assertion reports an
alert (exit one) on expiry's two-byte excursion. Retain that failed assertion,
all six raw logs and all values in `benchmark-results.json`; nothing was dropped
or rerun until neutral. This diagnostic alert is not a v2 statistical gate or an
established P4 regression. Printed Go benchmark averages divide integer counters
by N and can conceal sparse allocations, so identical printed counts do not
prove exact allocation neutrality. The tiny byte variation remains unattributed.

One separate fixed-work expiry profile pair investigates allocation sites:
N=100, memory sampling rate one, matching inherited settings (GOMAXPROCS=20 in
both outputs). This is attribution evidence distinct from the GOMAXPROCS=2
diagnostic matrix, including calibration/setup/warm-up and process initialization.
Flat allocated bytes and objects at the relevant production sites match exactly:

| Logical site | Flat bytes, each binary | Flat objects, each binary |
| --- | ---: | ---: |
| Scoring body: original evaluateKey / candidate evaluatePrunedKey | 252,416 | 7,232 |
| publishedResolverEvidence | 8,068,608 | 46,144 |
| publishSnapshot | 2,320,384 | 6,592 |

Sweep itself has zero flat allocation in both profiles. Whole-process totals
differ, including initialization and runtime/fixture bookkeeping; these profiles
do not reconcile every timed benchmark byte. Source inspection and the pair find
no added P4 allocation site. They do not prove that the original variation was
noise or that bytes are exactly neutral. No production instrumentation, clock
change or additional numerical acceptance gate was introduced. Production
resource/latency benefit remains unmeasured.

### V2 disposition and approved-item mapping

| Approved item | Implementation / checkpoint | Validation and remaining limit |
| --- | --- | --- |
| Preserve P1 separately | Archive implementation and both P1 test-file hashes unchanged | Original P1 checkpoint retained; current combined candidate passes complete ordinary/race lanes; old statistical campaign remains unrun and is not required by v2 |
| Remove duplicate sweep prune | spot/signal_resolver.go, private evaluatePrunedKey and sweep preparation | Complete inverse-change proof, literal new preparation/scheduling/lifecycle/transition cases, full native and affected Linux tests |
| Retain standalone preparation | evaluateKey nil/attach/prune wrapper | Direct nil/stale-state tests, baseline-overlay positive checks and standalone benchmark control |
| Bound resource impact | No new production owner, collection, lock, goroutine or allocation expression | Six guarded workload shapes and one attribution profile pair; no added P4 allocation site found, tiny counter variation unresolved |
| Update affected docs | Resolver README and this execution record; regenerate affected maps | Support-agent impact remains Not required; no settings, commands, diagnostic contract or durable architecture change |
| Prepare production comparison | Candidate executable plus comparison plan above | Build/hash metadata saved; deployment and whole-cluster acceptance pending |

Fresh lead verification rechecks authority, the actual diff, original-source
reconstruction, P1 hashes, all native package coverage, failed/skipped/deferred
evidence and claim limits. Design-informed read-only review found no material
remaining source or oracle issue after the documented refinements. It also
verified the allocation-alert disposition: retain the failed diagnostic assertion,
report matching principal allocation sites, and do not claim exact neutrality or
classify the residual variation as proven noise. These reviews are not an
independent scientific evaluation; no scientific/model contract changed.

Final handoff receipts are `closeout-evidence.json`, `production-proof.json`,
`codemap-generate.log`, `codemap-check.log`, `diff-check.log`, `gofmt-check.log`,
`checkpoint.json` and `final-verification.json` under `.tmp/p1-p4-v2/`. The separate
reverse-checked `p4.patch` and checkpoint preserve the P1/P4 rollback boundary.
At the v2 implementation handoff, source changes were uncommitted on `voacap`;
no commit, push, merge, deployment or service restart had been performed. Local correctness/race/static
checks pass. The diagnostic allocation alert and original intermittent clock
failure remain explicitly recorded; production performance and actual delivery
latency have not been measured.

## Publication authorization and deployment handoff

On 2026-10-16 the owner requested merging into `main` and pushing for production
user testing and longer-term performance measurements. The owner clarified:
"Push main; I will deploy it." This authorizes publication of the tested branch;
production replacement/restart remains with the owner. No additional runtime
code or configuration change is part of publication.

The merge includes the eight existing `voacap` commits after `5d40e7f` plus the
separate P1/P4 commits. The full combined source state passed the native lanes
above. `path_reliability.yaml` changes relative to `main` are comments; shipped
values are unchanged, while executable behavior includes the earlier path and
VOACAP work. P1's guarded 64-row profile pair reported 321 versus 258 allocations
per batch; this demonstrates local allocation reduction, not a measured user
latency improvement. P4 removes a redundant traversal; a reliable speedup and
whole-cluster benefit remain unproven. Preserve warmed baseline captures and
binary/configuration identity for the production comparison above.

Commit/merge/push, committed-source identity and CI receipts are recorded under
`.tmp/p1-p4-publication/`. These operations do not deploy or restart production.

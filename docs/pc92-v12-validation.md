# V12 contract-to-test and execution evidence

Authority: [approved v12](pc18-pc92-scope-ledger-v12.md), baseline
`2c0607986f3d3d9dc1921eb5b7c5ae00595143d2`. This record distinguishes planned
checkers from executed evidence. Detailed review uses inherited context and is
design-aware, not independent normative validation.

**Result: v12 stopped at its approved service-cost boundary.** The sustained
63-blocked/one-live test exhausted the live input mailbox and closed its source;
the identical zero-blocker control passed. Corrective closeout and overall
acceptance remain incomplete. No long qualification or broader recovery
architecture was substituted after this failure.

## Post-approval detailed review

The lead accepted all three specialists' checker-only refinements before
implementation. No material scope gap was found; the refinements remain inside
approved v12. No proposed test name in the following tables denotes an executed
test. All three reviews used inherited context, not independent normative review.

| Contract / failure | Stimulus | Observable result | Level / false-green control | Planned checker and owner |
| --- | --- | --- | --- | --- |
| Raw validation precedes repair | Malformed slash/SSID/case/leading-space at each role | Entire decode rejects before canonicalization | Unit; literal vectors independent of helper | `TestRawPeeringCallV12Grammar`, `TestPC92V12RawIdentityRoles`; config/peer wire worker |
| Role-specific padding and stable identity | Entry right spaces versus padded origins/control bytes; valid portable and raw-valid unrepresentable aliases | Only supported padding accepted; stable canonical identity remains required | Unit; no generic TrimSpace oracle | `TestPC92V12RolePadding`, `TestPC92V12StableIdentityBoundary`; wire |
| No partial authority | Seed populated self/external state; malformed later member; retry corrected same timestamp | Exact graph/metadata/ingress/watermark/cache/relay unchanged then valid retry succeeds | Consumer; counts alone insufficient | `TestPC92V12WholeRecordAtomicity`, `TestPC92V12MalformedNoAuthority`; wire |
| Startup and mailbox eligibility | Tracked candidate with prior metadata; full count/byte mailbox; valid current and stale-owner controls | No malformed metadata/staging/gate effects; valid capacity refusal remains | Consumer/race; avoid nil-manager fixture and unreachable fallback | `TestPC92V12StartupMalformedNoEffect`, `TestPC92V12MailboxRawEligibility`, existing mailbox controls; wire |
| Local encoding versus transit | Local aliases in explicit/implicit C and K; valid portable transit and padding | Canonical local wire without input mutation; exact original transit except hop | Consumer; production canonical publisher alone hides direct encoder regression | `TestPC92V12EncoderLocalOrigin`, `TestPC92TransitPreservesPayloadIdentity`; wire |
| Local/auth/private compatibility | Existing alias collisions, literal auth and stale/private ownership cases | Existing selected behavior retained, no private broadcast fallback | Consumer/race; unchanged control suite | Existing publication, inbound identity, PC93 and telnet current-owner tests; wire |
| Raw receiver oracle | Literal raw frame/decoder vectors plus startup | Raw acceptance distinct from login normalization and partial-C behavior | Reference; named cases must run, skips fail evidence | `TestDXSpiderReferenceRawPC92Identity`, canonical/startup/large-C controls; wire |
| Parser bounds and independent mutation oracle | Guaranteed-invalid identity mutations; bounded arbitrary input; large C and colon flood | Atomic rejection, no panic/envelope breach; measured decode allocations/time | Fuzz/benchmark/allocation; roundtrip alone insufficient | `FuzzPC92V12RawIdentity`, `FuzzDecodePC92Atomic`, `BenchmarkPC92V12Decode`, `TestPC92V12DecodeAllocationBound`; wire |
| K numeric replacement | Independently seed5457/633 before omitted build/both/zero/version, then restore | Literal5457/0,0/0,0/0,0/634 and correct restoration | Consumer/receiver; earlier clearing cannot mask a case | `TestPC92GraphKSubjectNumericReplacement`, `TestDXSpiderReferenceKSubjectNumericReplacement`; graph worker |
| New subjects and isolated effective metadata | Empty graph and external K; inspect record, origin and relationship | Subject node gets zeros; distinct origin/edge/raw wire unchanged | Consumer; existing-node-only tests insufficient | `TestPC92GraphKSubjectNewNodeCharge`, `TestPC92GraphKEffectiveSubjectIsolation`; graph |
| Other action/liveness behavior | Seeded A/C/D omissions, implicit subject, populated K | Preserve non-K numeric/IP/member rules, membership and completeness | Consumer/reference; global empty-to-zero must fail | `TestPC92GraphSubjectNumericActionIsolation`, reference controls and existing liveness tests; graph |
| Production SQLite projection | Receive/apply/project/write K sequence | Exact TEXT zero and preserved IP/other fields | Integration; no handcrafted snapshot or numeric SQL cast | `TestTopologyKSubjectNumericProjection`; graph |
| Peak versus final metadata charge | Large old strings to one/two zeros; exact and one-byte-short budget; already-zero repeat | Old backing plus new rounded clones charged at peak; atomic refusal; final shrink correct | Allocation; expected arithmetic does not call effective/projected helper | `TestPC92GraphKSubjectReplacementCharge`; graph |
| Old snapshot ownership | Hold active old numeric generation, clear live state, queue new snapshot, release separately | Old contents/reservation retained; exact once-only releases | Projection/race; live shrink is not snapshot retirement | `TestPC92ProjectionRetainsNumericGenerationThroughKClear`; graph |

The wire review requires replacing two raw-invalid positive fixtures with valid
portable aliases, testing actual tracked startup metadata, and preserving the
distinction between receiver login normalization and raw wire acceptance. The
graph review requires exact TEXT values, independent per-case seeding, node-only
K defaults and old/new string overlap (two newly cloned one-byte zero strings
round to16 bytes). These refine checkers inside approved scope.

## Recovery review disposition

The lead accepted the runtime matrix before implementation. `peer` owns the
production regression families below; qualification-tagged tests own the
external timing oracle. Names remain planned until execution is recorded.

| Contract / failure | Required distinguishing stimulus and observable | Planned checker |
| --- | --- | --- |
| Continuous interval and intersecting resources | Brief loss/restoration between ticks resets; healthy unrelated traffic preserves; non-overlapping component windows cannot add together | `TestPC92V12RecoveryContinuity` |
| Exact graph dependencies and transaction retirement | Equal counts with changed user reference credit; receive/replay/PC93/alternate ingress/loss/expiry/direct-peer paths; only completed transactions affect interval | `TestPC92V12RecoveryGraphDependencies`, `TestPC92V12RecoveryTransactionBoundary` |
| Mailbox classes and delayed handoff | Explicit allocation boundaries, exact64KiB witness, count191/192, byte exact/one-over; interruption before actor receives failure | `TestPC92V12RecoveryMailboxClasses`, `TestPC92V12RecoveryMailboxHandoff` |
| Coherent final clearing and ownership | Refill then healthy again invalidates old interval; undrained newer failure cannot be cleared; concurrent owner retirement obeys lock order | `TestPC92V12RecoveryFinalClear`, `TestPC92V12RecoveryOwnershipAndLockOrder` |
| Cause-specific capacity and witness expiry | Mailbox ignores unrelated exhaustion; alternate ingress reserves only0/1/2 observations; expired witness grants no authority; new-authority resources tested separately | `TestPC92V12RecoveryCause` |
| Independent gate reasons | Ordinary recovery progresses during global gates; effective admission waits for all reasons | `TestPC92V12RecoveryIndependentGates` |
| Cache chronology | Original stored age, strict600s boundary, delayed physical cleanup, expiry/refill interruption with delayed work | `TestPC92V12RecoveryCacheChronology` |
| Real Q5/Q6 timing | External facts define first availability, matching gate change and observation; one-second minimum and two-second maximum; real TTL and normal reconnect authority | Revised `q5IsolationClass`, `q6AdmissionFault` |
| False-green oracle rejection | Four-second hold, premature/interrupted recovery, missing/stale/wrong-generation/overflow evidence must fail actual helper | `TestPC92V12RecoveryOracle` |
| Complete absolute deadline fences | Already expired, late observation, late true predicate, cancellation at each stage and blocked context-aware observation all fail actual Q5/Q6 waiter | `TestPC92V12DeadlineWaiters` |
| Bounded ownership | Concrete pending/active overlap and fixed layouts <=32KiB; ordinary plan retired before sequential recovery plans; churn/Stop joins | `TestPC92V12RecoveryFixedStateEnvelope`, `TestPC92V12RecoveryScratchOwnership`, `TestPC92V12RecoveryRetirement` |
| Early complete service-cost gate | Reachable63 blocked+one live and64 blocked+backlog; near-capacity graph, large C, expiry, replay, lifecycle and 1000-local publication; all actual obligations timed | `TestPC92V12RecoveryCostGate` |

The observer records capacity facts and actual gate changes, never production
healthy-since as its expected answer. Q5 reads cache admission age without
pruning. Missing/overflowed evidence fails closed. A late actor snapshot cannot
restart an obligation. Unprofiled timing evidence and profiled diagnostics are
distinct; expensive qualification waits for the early cost gate. If the gate
requires certificates, reverse indexes, retained plans, new owners or relaxed
limits, execution stops under v12's explicit boundary.

## Focused execution evidence

All commands use Go 1.26.4 windows/amd64 and the process-local environment in
`D:\codex-gocluster-v12-20261001\env.ps1`; caches, temporary work and artifacts
remain on D:. Normal/race and receiver evidence below is focused development
evidence, not the integrated final lane.

- [Raw identity slice](pc92-v12-wire-validation.md): normal/race, two 30-second
  fuzz runs and actual pinned receiver cases passed. Pure raw grammar predicate
  is 0 B/op and 0 allocs/op. Comparable valid decoder allocation counts stayed unchanged;
  median complete 62,171-byte C cost rose 1.058 to 1.376 ms and maximum-entry C rose
 3.855 to 5.942 ms. This CPU increase belongs in service-cost assessment.
- [K metadata slice](pc92-v12-k-validation.md): Go consumer, production SQLite,
  accounting/projection generations, 14 receiver cases and targeted race passed.
  The old graph implementation failed the new assertions under an external
  baseline overlay. The effective-node helper measured 0 B/op and 0 allocs/op.
- Initial `go test [-race] -tags qualification ./peer -run
  'TestPC92V12(DeadlineWaiters|RecoveryOracle|RecoveryObserverOverflow|Q5ObservationDoesNotPrune)$'
  -count=1 -timeout=60s`: PASS, normal 0.198 s and race 1.227 s. The actual shared
  Q5/Q6 helper rejects expired/canceled observations and late true predicates.
  Recovery negatives include premature, interrupted, wrong-generation, missing,
  stale, future-dated, overflow and four-second-late evidence. The cache observer
  leaves an expired retained key untouched. These initial results preceded the
  mailbox-continuity and duplicate-age oracle review fixes described below.
- With `GOCLUSTER_PC92_Q6_PROFILE=preflight`, isolated user data and
  `GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1536MiB`,
  `go test -tags qualification ./peer -run
  '^TestPC92QualificationQ6Faults$/^zero=(false|true)$/^repeat=1$/^admission$'
  -count=1 -v -timeout=90s`: affected rerun PASS 5.967 s. Actual capacity-releasing C commit to
  external reopening was 1.0105066 s and 1.0092489 s. Both socket C/A captures passed
  actual pinned receiver replay with the new IP. This is a focused diagnostic,
  not full Q6; `logs/q6-admission-preflight-r2.log` retains the evidence.
- [CTY evidence](cty-refresh-2c06079.md): current raw/Git hashes, local download
  status and semantic dictionary counts reconciled; no asset mutation.

## First necessary service-cost check

The initial `TestPC92V12RecoveryCostGate` passed on a retained binary built at
2026-10-01T23:00:15-04:00 with SHA256
`45A7FBC8CC1A3C03AA139F60AAAE353E8984A8083F20D7AF732DB6AA3B181D60`.
Its before/build/after manifests were identical. Full source/binary/result
artifacts are under `D:\codex-gocluster-v12-20261001\early-gate`.

This constructs a reachable full graph, then uses ordinary refusals and
withdrawals: 64 configured peers, 63 blocked, one live, 4096 nodes, 65535 users
after release, 131070 edges, 16384 freshness, 8000-member/64039-byte C witnesses
and 1000 local users. A stable provider IP change occurs before 63 sequential
exact evaluations. Unprofiled evaluation took 696.4436 ms and membership control
queue admission 700.8824 ms, within one second. **This necessary condition is
not the complete V12-05 workload.** It omits sustained incoming traffic and
the 64-blocked backlog case; those remain required before expensive qualification.

A separate cold diagnostic using the same binary started at
2026-10-01T23:01:07-04:00. It includes setup, refused-owner retirement, recovery
and teardown. CPU, mutex, alloc_space, inuse_space and block profiles plus
`go tool pprof -top` reports are retained. Recovery-call stacks account for
313.28MB cumulative sampled allocation (including 211.40 MB graph preparation
and 88.88 MB decoding). This is allocation traffic, not simultaneous owned or
live memory, and does not violate or prove the 5 MiB scratch reservation. The
post-teardown inuse profile is not a live-population memory proof. There is no
comparable pre-v12 runtime bundle and no net performance improvement claim.

## Sustained necessary condition: failure and stop

`TestPC92V12RecoverySustainedCostGate` and
`TestPC92V12RecoverySustainedBaseline` used a second retained binary built at
2026-10-01T23:17:48-04:00, SHA256
`31D1DC54D83757D16703230F31CB69F060179DBEFE3D19C57F2FC94E30EED5BA`.
Source manifests matched before build, after build, after the unprofiled run and
after profiling; the binary hash also matched. Artifacts, all 1,315 manifest
workspace source inputs, manifests, profiles and raw logs are retained under
`D:\codex-gocluster-v12-20261001\early-gate-sustained`.

Reproduction: dot-source the environment above; set
`GOCLUSTER_PC92_V12_COST_GATE=1`, `GOMAXPROCS=2`, `GOGC=50`,
`GOMEMLIMIT=1536MiB`; build `go test -c -tags qualification -o <binary> ./peer`;
run from `peer` with
`-test.run=^TestPC92V12RecoverySustained(Baseline|CostGate)$ -test.count=1
-test.v -test.timeout=120s`. The external driver
`early-gate-sustained.ps1` records the manifests and final failing result.

Both cases construct the same reachable graph and 64 registered owners. The
pressure case refuses 63 genuinely infeasible 8,000-member C records at the
65,536-user cap; the control normally closes those same owners without failure
episodes. Both retain 4,096 nodes, 65,536 users, 16,384 watermarks and one live
source. Closing those owners removes their ingress observations in both cases;
this is not a claim that all 262,144 ingress observations remain resident.

The producer uses ordinary `HandleFrame` admission on a 10 ms cadence: per 100
records, 45 A, 45 D, two large C and eight K, plus PC93 every 60 records. D/A
changes a real reference count while the affected user remains reachable, so
all retained witnesses remain infeasible. A stable IP change among 1,000 local
users exercises publication concurrently. Synthetic initial message/detached
watermark values match their recorded admission times to avoid UTC-dependent
fixture rejection. Cancellation of the test actor does not cancel the live
session, so teardown cannot manufacture the reported source closure.

| Unprofiled observation | 63 blocked + one live | Zero-blocker control |
| --- | ---: | ---: |
| Offered PC92 | 205 | 399 |
| Committed PC92 | 12 | 398 |
| Delivered PC93 | 1 | 7 |
| Peak PC92 input count | 192 | 3 |
| Remaining PC92 input count | 192 | 1 |
| Membership queue admission | 104.1313 ms | 11.8975 ms |
| Timed producer/service phase | 2.5580011 s | 4.0005243 s |
| Healthy source closed | Yes | No |
| Result | FAIL | PASS for this necessary control only |

The hard failure is the existing mailbox refusal and source closure, not a new
backlog threshold: 205 offered minus 12 committed minus one refused equals 192
queued. `serviceAdmissionRecovery` performs 63 Parse/Decode/prepare/charge
evaluations after a meaningful transaction, before the next input can dequeue.
The ordinary source reaches the existing 192-record limit and closes through
`HandleFrame`/`recordAdmissionFailure`. The control distinguishes this mechanism
from an invalid timestamp, pressure producer or unreachable capacity witness.

The same binary reproduced failure with CPU/memory/block/mutex profiling
(205 offered, 12 committed, 192 queued, source closed). The control again
passed (399 offered, 398 committed). This cold diagnostic includes setup,
retirement and teardown. Recovery-call stacks account for about 801.66 MB
cumulative sampled allocation in the combined profile; that is allocation
traffic, not live ownership or evidence against the 5 MiB scratch bound.
Profiles support repeated decode/prepare cost; they do not establish a complete
runtime comparison, aggregate memory proof or a mutex bottleneck.

This failed necessary condition is sufficient to stop. The 64-blocked backlog
case and complete mixed workload were not run. New certificates, reverse
indexes, retained plans, owners, reservations, limits or relaxed deadlines would
require a revised design and exact matching scope approval. No such change was
made under v12.

## Review, correction mapping and incomplete closeout

The lead inspected both production slice diffs, material tests and raw receiver,
negative-baseline and benchmark logs. Their changes match V12-01/02. Review of
recovery found an older pending episode could be reinstalled after a newer
episode cleared. The fix retires superseded handoffs and has a distinguishing
regression. Fixed-state accounting now includes actual owner allocation-class
growth and explicit method-value backing. Its bound is 16,064 bytes, below the
approved 32 KiB incremental ceiling; the aggregate allocation proof remains open.

A design-aware read-only review of V12-04 found two false-green paths: mailbox
health had to exist from the claimed availability time, and Q5 had to compare
the original stored admission age immediately after duplicate/refusal. Both
were corrected. `LateMailboxRecovery`, `MissingInitialMailbox` and
`TestPC92V12Q5RejectsDuplicateAgeRenewal` reject the invalid evidence. The reviewer
reread the fixes; this was inherited-context review, not independent review.

The affected qualification-tagged recovery/deadline/cause/dependency race family
passed (1.295 s; `logs/qualification-recovery-race-r2.log`). It covers 22
cause/dependency subcases, sub-tick continuity, delayed mailbox handoff, locked
clear, pending generation, cache chronology and actual checker negatives. A
second focused race passed (1.318 s; `logs/review-fix-targeted-race.log`) for the
duplicate-age negative, layout bound, raw identity review edits and diagnostic
reason enumeration. Synthetic occupancy tests isolate exact predicates; they
do not establish maximum-population allocation or scheduler service cost.

| Approved item | Changed implementation / evidence | Disposition |
| --- | --- | --- |
| V12-01 / RA01 | `config/peering_contract.go`, `peer/pc92_codec*.go`, raw identity/startup/mailbox/reference/fuzz tests; wire report | Implemented; focused normal/race/fuzz/reference evidence passed. |
| V12-02 / RA03 | `peer/pc92_graph*.go`, K metadata/SQLite/projection/accounting/reference tests; K report | Implemented; focused evidence passed. |
| V12-03 | `pc92_recovery*.go`, controller/receive/resources/scheduler/publication, manager and dedupe; recovery/cause/dependency tests | Implemented prototype; focused correctness passed, required service cost failed. Not accepted. |
| V12-04 | `qualification_admission*.go`, deadline/oracle tests, Q5/Q6 fixtures | Implemented; distinguishing negatives/race and both Q6 admission diagnostics passed. Full Q5/Q6 not run. |
| V12-05 | Fixed-state layout test; retained first and sustained cost-gate binaries/profiles | Added fixed bound passed; sustained necessary condition failed. Complete service, scratch/retirement and workload proof remain open. |
| V12-06 | `cty-refresh-2c06079.md`, exact parse/hash/status reconciliation | Retained unchanged and documented; no geographic or downloader-actor claim. |
| V12-07 | Protocol/operator/support docs, allocation record, ADR-0232/linked ADR-0231, TSR-0035, execution ledger | Failure and remaining evidence recorded; corrective closeout fails. |

The ordinary full test run initially failed only its diagnostic-literal
enumerator: the new cause-specific wrapper placed its reason before its cause,
so the scanner missed one existing reason. The test now recognizes that exact
argument and still requires all 19 reasons and unchanged fixed backing. Static
review also found two test-only tagged-switch suggestions and unused names in a
layout-only historical struct; those checker/style issues were corrected.
No production algorithm changed after the retained failing cost-gate binary.

## Final verification at the stop boundary

The lead reviewed the current production diff, new recovery/qualification
helpers, changed tests, scope mapping, generated map and operator/support
claims. This fresh verification is lead-owned, not independent. There is no
new owner, graph clone, reverse index or retained-plan collection in the diff.
The exact-check service failure remains material and prevents closeout.

The larger qualification functions keep one linear fixture lifecycle and
assertion sequence together (setup, pressure, original-age observation,
recovery, drain); splitting those phases would obscure the original deadline
and cleanup owner. No new production function exceeds the 120-line review
threshold. Comment review covered raw/local identity, effective K metadata,
mailbox/cache intervals, failure generations, final lock ordering and observer
restrictions. The crawler-comment script scanned zero applicable paths, so its
PASS is not evidence for these comments; the source-aware review is.

| Check | Observed result |
| --- | --- |
| `go test ./...` | PASS on final rerun, using cached unchanged-package results; `logs/go-test-all-r2.log`. The changed peer package separately passed in 84.958 s after the test-only corrections. |
| `go vet ./...` | PASS; `logs/go-vet-all.log`. |
| `staticcheck ./...` | PASS after test-only fixes; `logs/staticcheck-all-r2.log`. |
| `golangci-lint run ./... --config=.golangci.yaml` | PASS, zero issues; `logs/golangci-lint-all-r2.log`. |
| `go test -race ./...` | PASS on final rerun, with cached unchanged-package results; `logs/go-race-all-r2.log`. The peer race package passed in 100.848 s in the preceding full run. |
| `go vet -tags qualification ./peer` | PASS; `logs/go-vet-qualification.log`. |
| Qualification-tagged targeted race and real Q6 admission | PASS as recorded above; opt-in full qualification remains unrun. |
| `golangci-lint run ./peer --build-tags qualification --config=.golangci.yaml` | FAIL: ten findings on expressions present in baseline 2c06079 (three error comparisons/assertions, four missing-context network calls, two static simplifications, one invariant parameter). See `logs/golangci-qualification.log` and `logs/tagged-lint-baseline-expressions.json`. No cleanup outside the stopped recovery scope was substituted. This is an open tagged-lint result, not a waiver. |
| Workflow contract / troubleshooting records / local support-agent checker | PASS. Support check used local Worker smoke; no deployed-worker or hosted-CI claim. |
| Generated code maps | Regenerated with the repository tool; freshness check PASS. The runtime map also repairs previously stale PC92 inventories. |
| Final worktree/format/document checks | `git diff --check` PASS; all changed/new Go files are gofmt-clean; local links in all 17 changed/new Markdown files resolve (generated-map link syntax is owned by its freshness checker). Branch remains `p92`, HEAD remains baseline 2c06079. |

The first full race run failed only
`TestDailyFileSinkArchiveCollisionMergesWithoutOverwrite` in unchanged
`internal/cluster/logging_test.go`: Windows refused a file read because another
process held the file. Five isolated race repetitions passed (1.187 s), then
the complete race command passed (`internal/cluster` 3.782 s). No data-race
report occurred. The locking process/root cause is not established and no
logging implementation was changed. The first failure remains in
`logs/go-race-all.log`; the rerun does not erase it.

Remaining required evidence includes explicit overlapping-global-gate recovery,
scratch transaction nonoverlap, retirement/shutdown/churn, the 64-blocked and
complete service workloads, fresh full Q5/Q6/cache profiles and runtime/Q4
diagnostics. No timeout, workload, limit or policy was waived. Enabled-SQLite,
context backing, outer retirement and overall workload acceptance remain
separately open. The working branch is uncommitted and is not a qualified release.

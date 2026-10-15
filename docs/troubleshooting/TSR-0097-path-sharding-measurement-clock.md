# TSR-0097 - Path Sharding Measurement Clock

Status: Monitoring
Date Opened: 2026-10-15
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: S4 path sharding test measurement and lifecycle
Trigger Source: Chat request
Led To ADR(s): ADR-0306
Tags: profiling, windows, qpc, benchmark, cancellation, qualification

## RCA Summary

- What happened: the valid original S4 Serial/off comparison failed its
  normal-writer p99.9 guard. After the approved clock repair, Serial/enforce
  failed the same guard. Same-binary controls exposed substantial variability.
- Why: the original test clock calls QueryPerformanceCounter through SyscallN,
  which enters and leaves Go's syscall state on each read. This is an observed
  measurement disturbance, not an established cause of the original failure.
- What fixed it: Approved v3's supported live testing.B.Elapsed measurement
  path removes the identified syscall-state disturbance and passes targeted
  lifecycle tests. It does not fix the later qualification failure. Approved v4
  accepts that performance uncertainty; retained S4 safety validation passed.
- How we know: source inspection, matched local profiles, a fixed same-binary
  control and a diagnostic supported-timer prototype described below.
- Operator/support answer: this concerns local performance evidence. It does
  not establish production savings or a production regression. The earlier
  failed gate remains failed. Approved v4 explicitly accepts the unresolved
  S4 performance risk for development continuation; retained safety checks
  still apply, with full-cluster performance assessment after S5-S7.

## Triggering Request

- Request date: 2026-10-15.
- Request summary: investigate and finish S4 before proceeding through S5-S7.
- Request reference: owner Approved v3; first-wave execution record.

## Symptoms and Impact

The valid twelve-pair original Serial/off run measured normal-writer p99.9
candidate/baseline ratio 1.1666666667, above the 1.10 limit. Elapsed ratio was
0.9970709699 and other role-tail guards passed. Wide individual ratios do not
invalidate this failed acceptance result. S4 was neither committed nor deployed.

A later twelve-pair control used the exact same S3 binary for both labels. Its
normal-writer p99.9 ratio was 0.8839869281: an apparent 11.60% improvement with
unchanged code. All original one-sided equivalent guards passed. The control
does not prove that the earlier candidate failure was noise.

## Timeline

1. 2026-10-15 - original valid comparison stops at the Serial/off writer guard.
2. 2026-10-15 - fixed same-binary, contention and matched-profile diagnostics
   run without changing the original production code or qualification harness.
3. 2026-10-15 - supported-timer diagnostic passes focused ordinary/race checks;
   first allocation-oracle failure is retained separately.
4. 2026-10-15 - owner approves v3's bounded instrumentation amendment. Required
   lifecycle and measurement tests must pass before new qualifying observations.
5. 2026-10-15 - repaired targeted checks and common preflights pass; both fixed
   Serial/off same-binary controls pass all ten symmetric guards. Newly frozen
   qualification passes Serial/off and Serial/shadow, then stops at enforce
   normal-writer p99.9 ratio1.2361111111 versus1.10. Total elapsed1.003743236
   passes. All24 enforce manifests are valid and reproduce the failed result.
6. 2026-10-15 - separate fixed Serial/enforce diagnostics compare each frozen
   binary with itself: baseline writer1.1984, candidate writer3.7682 and
   path-only writer2.8171 exceed reference repeatability bounds. Both elapsed
   results remain within5%. This does not waive or attribute the failed gate.
7. 2026-10-15 - four separate unscored runs retain all16M original-position
   samples after the epoch. Quantiles/maxima/zeros reproduce valid receipts;
   slow operations also occur outside growth/aging. Exact warmed-allocation
   checker repairs pass Windows ordinary/race and Linux/cgo targeted tests.

## Hypotheses and Tests

1. A real serial cost may explain the failed tail guard.
   - Evidence: candidate shardForKey appears in the serial CPU profile; sampled
     updateBucket cumulative CPU was 260ms versus 190ms in the baseline capture.
   - Outcome: Inconclusive. One perturbed profile cannot attribute tail cost.
2. The observer changes scheduling and consumes material CPU.
   - Evidence: installed Go 1.27.1 SyscallN calls cgocall and entersyscall/
     exitsyscall. Two timed reads per operation add roughly eight million
     enter/exit pairs across four million operations. Clock.NowNS inclusive
     sampled CPU was 620/3720ms (16.67%) baseline and 520/3930ms (13.23%) candidate.
     These inclusive totals overlap runtime callees and cannot be added to them.
   - Outcome: Supported as a mechanism, not as sole cause of the failed gate.
3. A supported benchmark timer can remove those syscall-state transitions.
   - Evidence: installed testing.B.Elapsed uses testing's Windows QPC/stdcall
     path. A genuine fixed-1x callback prototype passed shared concurrent
     ordering, complete samples, zero per-read allocations, wrong-iteration
     refusal and post-return detection of a deliberate cleanup failure.
   - Outcome: Supported feasibility. It did not qualify S4 performance.
4. Process allocation totals isolate timer allocations.
   - Evidence: the first prototype saw 16B/one object during the standard-timer
     interval. The retained revision uses AllocsPerRun for steady clock reads;
     both clocks measured zero per-read allocations in ordinary and race runs.
   - Outcome: Rejected as a sufficient timer-only allocation oracle. Process
     totals remain diagnostics, including unrelated runtime activity.
5. A zero AllocsPerRun result proves zero allocation events.
   - Evidence: installed Go1.27.1 testing/allocs.go divides allocation count by
     run count using integer arithmetic before returning float64. Thus a zero
     average can hide fractional per-operation allocations. All24 enforce
     Serial qualification receipts record372 events/37200 bytes in each
     variant. Lazy coarse receiver-sidecar allocation matches the source shape;
     direct attribution is not established by those process totals.
   - Outcome: Rejected as an exact-zero oracle. After archiving the complete
     frozen sources, warmed unit fixtures changed to one measured whole batch,
     following one whole warm batch. The measured batch alone funds its
     admission witness; all calls retain lifetime accounting. A deliberately
     escaping one-per-batch allocation must be detected. This changes no
     qualifying workload or failed receipt. Do not warm or discard qualifying
     reports to hide the fixed workload's growth.
6. Receiver storage growth or bucket aging alone accounts for the slow tail.
   - Evidence: separate post-epoch raw exports retain original positions. All
     four runs reproduce every stored quantile/max/zero count. At least10us
     normal samples also occur in ordinary same-reference updates:1031/1063
     for pair1 baseline and766/779 for pair2 candidate. There are only186
     first coarse-storage uses perrun. Classifications are source-derived,
     not observed internal counters.
   - Outcome: Insufficient as a complete explanation. The p99.9 rank jumps
     when slow-operation counts cross about1000 of1M. Precise runtime/host
     attribution needs correlated event evidence and operation start times;
     neither was collected. No production-regression verdict follows.

## Findings

- Root cause: the original tail regression's cause is undetermined. The
  measurement clock's syscall-state changes are verified source behavior and
  a contributing measurement disturbance; they are not a proved sole cause.

The clock's syscall-state changes are established; the original regression's
cause remains unknown. New qualification must use a newly frozen common harness,
preserve all original observations and limits, and first pass symmetric
same-binary repeatability controls. Profiles cannot substitute for those gates.

The live benchmark clock requires immutable timer state and primed frequency
in the pinned Go version. All readers must join before callback return. A
programmatic benchmark's context starts at Background; its cleanup cancellation
cannot release readers that callback defers are already waiting to join. Use an
outer-derived finite work context and cancel before release/join on every exit.
Check failures after Benchmark returns, including later cleanup failures.

Connected profile maintenance has an owner outside the eight queue/consumer
owners. Actual Compact overlap does not prove complete profile coverage. The
maintenance owner must retire before capture and the clock domain close.

## Decision Linkage

- ADR created/updated: ADR-0306 records the private full-key selector and the
  owner-accepted S4 performance exception under exact Approved v4.
- Decision delta summary: S4's remaining timing campaign and final matched
  profiles are deferred to assessment after S5-S7. Failed/unrun evidence retains
  its status; correctness/race/resource/lifecycle and real soak remain required.
- Contract/behavior changes: no production contract changes. Preserve the
  stable native executable paths required by ADR-0265 and required test-leaf
  evidence required by ADR-0299.

## Verification and Monitoring

- Validation already run: original comparison, fixed same-binary control,
  original contention diagnostic, eight matched diagnostic profiles, and
  supported-timer ordinary/race prototype. V3 targeted ordinary/race lifecycle
  checks, Linux/cgo, analyzers, controller fixtures and rebuilt common preflights
  passed, as did both prescribed Serial/off repeatability controls. V3
  qualification stopped at the valid Serial/enforce writer-tail failure.
  Remaining comparisons, final profiles, real soak and full lanes did not run.
  Separate diagnostic-only same-binary Serial/enforce series demonstrated failed
  writer-tail repeatability with full unchanged populations in both binaries.
  Four sample-retention diagnostics preserve16M samples, with exact successful
  receipt reproduction. After the checker repair, helper ordinary/race each
  passed31cases and core each74. Fresh Linux/cgo allocation cases, full-repository
  vet/Staticcheck/lint (0issues) and TSR/support checks passed.
- Under Approved v4, full native ordinary/race lanes and actual loopback glyph
  ordinary/race checks passed. A subsequent checker-only refinement isolates
  each warmed allocation mode in a bounded same-saved-binary child, preserving
  exact batches and the zero limit. An injected escaping64-byte allocation in
  the measured beacon batch must fail the real allocation assertion. Final
  affected-package ordinary/race and Linux/cgo checks passed. An earlier
  single process-wide allocation stays failed and unattributed; isolation is
  not causal attribution. The real 66-minute maintenance soak passed: two
  64-shard compactions an hour apart, heap release at both cycles, nearly 299s
  of successful post-cycle work in all four roles, 13 available reports and
  actual logger/maintenance retirement.
- The completed Go soak passed, but the original postprocessor expected AtUTC/
  AtNS/EmittedUTC/AsOfUnix instead of the saved snake_case JSON tags. Its failed
  log and frozen controller remain intact. A separate reviewed verifier fixes
  only property spelling, tests 22 positive/negative cases against actual Go JSON,
  and validates required RUN/PASS, final package PASS and all frozen identities.
  It accepted the same completed proof without rerunning the workload. Synthetic
  checker fixtures must follow the producer's serialized contract; copying the
  consumer's mistaken property names into fixtures can hide a schema mismatch.
- Command evidence: ignored `.tmp/path-reliability-first-wave/s4/` contains
  `run-diagnostics.ps1`, `run-clock-probe.ps1`, diagnostic logs and the exact
  sources/binaries. `investigation-20261015/results.md` records results and
  limitations. Both original build-input sets were archived and hash-checked
  before v3 source changes; old baseline and evidence remain intact.
- Signals: per-role p99.9, complete sample counts/zeros, individual guard
  results, callback and reader retirement, source/config/tool/binary identity.
- Stop conditions: any repeatability or required acceptance failure; changed
  frozen identity; mixed clock domains; incomplete work or reader retirement.
  Do not repeat unchanged controls until they pass or pool separate protocols.
  Approved v4 excepts S4 performance failures from blocking later development;
  all retained safety checks still precede the S4 checkpoint. This task-specific
  exception does not waive S5-S7 gates or establish production performance.

## References

- Issue(s): owner first-wave request and Approved v3.
- PR(s): none.
- Commit(s): this S4 checkpoint follows S3 base 9145209, with the explicit
  Approved v4 performance exception; no deployment or push.
- Related ADR(s): ADR-0265, ADR-0299, ADR-0306.
- Related docs: `docs/path-reliability-first-wave-execution.md`.
- Related source: `internal/testsupport/clock_windows.go`,
  `pathreliability/sharding_runtime_performance_test.go`,
  `internal/cluster/path_sharding_connected_test.go`,
  `internal/cluster/path_sharding_profile_test.go`.

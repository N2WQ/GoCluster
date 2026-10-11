# Call correction delivery execution

Authority: exact `Approved v1` on 2026-10-11 for
[the scope ledger](call-correction-delivery-scope-ledger-v1.md).
Baseline: `8890e79`; branch: `fix_temporal_family_delivery`.
Unrelated profiling changes were present at branch creation and remain unowned.

## T0 - toxicity summary synchronization

Commit: `42f0e0f`.

Implemented the test-only worker join before exact terminal counters. The
targeted cluster/toxicity ownership controls passed; the summary assertion also
passed 1,000 ordinary and 1,000 race repetitions. No production classifier
change.

Checkpoint passed on 2026-10-11:

- Stable Windows runner: all 70 packages ordinary (561.1s) and race (943.2s).
- `go vet ./...`, `staticcheck ./...`, and the configured golangci-lint lane.
- Connected CustomSCP producer/consumer regression, ordinary and race; matching
  run identities and independently reopened storage reached actual delivery.
- Troubleshooting-record checker and direct final test/TSR diff review.

Logs: `.tmp/temporal-family-delivery/T0/`; connected ordinary run
`1a4def4f29224005a327c26afcbe2a9b`, race run
`91de346f0eab46ec970a832889dbc636`. The terminal assertion checks exact zero
pending/backlog after the join; receiving/applying still precedes shutdown.
Support documentation impact: existing TSR-0068 follow-up; no runtime or
operator contract change. Codemap generation/check is the final write/check.

## Postapproval test-strategy review

The read-only review found no material scope gap. Its checker refinements are
accepted within v1: baseline counterexamples must compile against the defective
APIs; new admission-API checks follow reproduction; destination absence uses
FIFO barriers and admission/drop observations rather than socket quiet time
alone; concurrent arbitration is checked against actual admission order and
direct inspection of the mutex span. These are validation requirements, not
additional production hooks or behavior.

## Remaining ordered checkpoints

T0, T1, F1 and F2 are complete. Each checkpoint passed its selected full lane
before the next production patch began. The four local commits preserve the
unrelated profiling work; merge, push and deployment remain outside authority.

## T1 - temporal final-request retirement

Commit: `0665ede`.

Baseline `42f0e0f`: `TestTemporalRetirementReclaimsFinalEvictedRequest` compiled
and failed for fallback_resolver, abstain and bypass when the newer request
completed first. Each asserted `terminal ownership leaked: pending=0 keys=1`.
The reverse completion order passed. Evidence:
`.tmp/temporal-family-delivery/T1-decoder-baseline.log`.

The unchanged runtime/replay baseline also failed. Actual TCP emitted
K1ABD/P/25%, sealed ring and raw persisted bytes retained that identity/glyph,
and reopened archive recovered K1ABD/P. No fresh request was held; the configured
abstain counter incremented. Original-input PC61 K1ABD was preserved. Replay
likewise emitted K1ABD/P without admission. Logs:
`T1-delivery-baseline.log`, `T1-replay-baseline.log` in the same directory.

Implementation: every known terminal request now calls `retireRequestLocked`
under `d.mu`; its existing bounded pending scan reclaims event-key state only
when the final same-key owner leaves. Defer preserves ownership. Separately
bounded accepted identity history is untouched.

Targeted ordinary/race checks passed across correctionflow, cluster and replay:
both retirement orders, fallback actions, force/retry, missing state, key churn,
concurrent requests, confirmation controls and real delivered/replayed output.
The repaired runtime positively held the new key, received an earlier FIFO
barrier, retained ownership before its exact deadline, then emitted K1ABC/C/75%
once. Raw bytes were compared against literal version-six fields before decoding;
reopen recovery and original PC61 were separate checks.

Three guarded terminal-scan benchmark samples: one pending request 244-249ns,
25,000 pending requests 305-312us; both 0 B/op and 0 allocs/op. The cap sample
forces a full other-key scan, not decoder scoring or production throughput.
Evidence: `T1-bench.log`.

Complete T1 checkpoint passed: all 70 packages ordinary (564.3s) and race
(945.8s), vet, Staticcheck, configured lint, and connected CustomSCP ordinary
and race. Logs: `.tmp/temporal-family-delivery/T1/`; connected run identities
`8884fa734f4740749c63f3641d85f523` and
`7b866612e88e47e0bbe3deb1c5e7ac2a`. The post-review concurrent retirement test
also passed ordinary/race (`T1-post-review.log`, `T1-post-review-race.log`).

Support impact: TSR-0085 and source-map routing. Decision disposition: restore
existing request/event ownership; no new ADR or accepted-history policy.

Executed targeted commands (then the same runner command with `-Race`):

```powershell
& .\scripts\test-windows.ps1 -Packages @('./internal/correctionflow','./internal/cluster','./cmd/rbn_replay') -Run 'Temporal.*(Retirement|Reclaim|Confirm|Proposal)' -Timeout 180s
```

Baseline commands used the same runner, ordinary build, 180s timeout, selecting
the exact tests `^TestTemporalRetirementReclaimsFinalEvictedRequest$`,
`^TestRuntimeTemporalReclaimDelivered$` and
`^TestReplayTemporalReclaimFreshAdmission$` in their respective packages.

The benchmark compiled with `go test -c -o` into the canonical correctionflow
`package.test.exe`, acquired the shared runner lock, and executed directly from
`internal/correctionflow` with these arguments:

```text
-test.run=^$ -test.bench=^BenchmarkTemporalRetirementPendingScan$ -test.benchmem -test.benchtime=300ms -test.count=3 -test.paniconexit0
```

Read-only Go quality review found no material defect. Its two refinements are
accepted: exact command selection is recorded above, and concurrent workers now
report errors and return without calling the test goroutine's `FailNow` helper.
The affected targeted check reran and passed after the full checkpoint.

## F1 - telnet-only destination isolation

Commit: `350f447`.

Baseline `0665ede`: loaded-config production-constructor regressions compiled
and failed on missing original PC61, sealed ring, raw archive records and
independently reopened archive for the hidden base call. Immediate and overflow
history assertions also failed; CustomSCP byte checks showed the older membership
timestamp and absent new receiver observation. Actual delayed ownership and
timeout-suppression controls executed. Evidence:
`.tmp/temporal-family-delivery/F1-baseline.log`.

The production change preserves shared secondary flags when family display
hiding rejects telnet output. After actual immediate or overflow disposition,
normal fanout eligibility governs historical admission. Real pending holds,
risky timeout suppression and all-secondary-denied behavior are retained.
Legacy and CustomSCP recording are tested separately, with raw bytes checked
before independent reopening; original peer fields and SELF policy have separate
actual recipients. The prior peer regression's family expectation is corrected;
its shared MED-refusal control remains unchanged.

Decision disposition: restore ADR-0014's telnet-only destination contract;
TSR-0086 records the two source causes and destination/history counterexamples.
F2 arbitration began only after this checkpoint passed and committed.

F1 targeted ordinary/race passed with:

```powershell
& .\scripts\test-windows.ps1 -Packages './internal/cluster' -Run 'FamilyDelivery|PeerOriginal(FinalSuppression|Refusal)|OutputPipeline(Stabilizer|FinalMed|.*Secondary)|TelnetFamilySuppressor|FTConfidence' -Timeout 180s
```

Then the same command with `-Race`. Existing peer source-publication controls
also passed both modes, selecting `^TestShouldPublishLocalSpot` in `./peer`.
Evidence: `F1-targeted.log`, `F1-targeted-race.log`, `F1-source-controls.log`,
`F1-source-controls-race.log` under `.tmp/temporal-family-delivery/`.
The baseline used the same ordinary runner and timeout, selecting
`^TestFamilyDelivery(ImmediateDestinationsAndHistory|SchedulerOwnershipAndHistory)$`.

The loaded no-telnet constructor control independently passed with both
historical owners. The connected fixture and literal persistence cohort stay
in one test file to keep their ownership and expected source fields reviewable.
Read-only Go quality review found no material defect; its closeout refinements
are accepted (record this cohesion and stage only owned shared-document hunks).

Complete F1 package validation passed: all 70 packages ordinary (576.6s) and
race (943.1s), vet and Staticcheck. Configured lint initially reported a
tagged-switch suggestion and an unnecessary initial test-counter assignment.
Both test-only style issues were repaired; all `^TestFamilyDelivery` cases
reran ordinary/race and passed, and lint then reported zero issues. The style
changes do not invalidate the broader production/package results. Evidence:
`F1-post-lint.log`, `F1-post-lint-race.log`; the original lint failure remains
in `F1/lint.log`.

Connected CustomSCP ordinary and race passed with run identities
`1eeba2573e8c4decab9761b96b3fa79b` and
`68b041c51b774902a22ea7e92bb49cbd`. Final troubleshooting-record checks passed;
the changed-only crawler checker inspected one support-critical production
file and passed. Fresh final diff review confirmed the small shared-gate repair
and preserved the unrelated profiling hunks. Codemap generation/check is the
final write/check before the F1 local commit.

## F2 - fresh percentages and admitted per-lane history

Baseline production `350f447`: `^TestTelnetFamilyQueueRefusalDelivered$`
compiled against the existing void broadcast API. A real unstarted four-slot
queue refused the strong slash input exactly once. Draining verified all four
original filler pointers and no seed admission. After starting the SAME server,
both normal and DIAG clients received only K1BAR instead of N2WQ then K1BAR.
The fresh literal 3/1 population independently established 75/25 shares and
usable authority. Sealed ring, three original PC61 tuples, raw persisted bytes
before decoding and separately reopened archive passed. This is a confirmed
display counterexample, not a compilation/setup failure or restart-loss claim.
Evidence: `.tmp/temporal-family-delivery/F2-baseline.log`.

Postapproval test-strategy review disposition: all missing checks fit approved
v1. Replace the ten old specificity/cached-contested tests with fresh authority
and actual admitted-history checks. Explicitly cover independent lane clocks,
frequencies and literal aliases; repeated-call rechecks; retaining all admitted
rivals; comparison before mutation; admission refusal; map/list/count churn;
configured relations/modes/tolerances; global safety vetoes and score basis.
Arithmetic vectors and usable behavioral authority are separate assertions.
The actual queue/TCP fixture remains independent of stub unit publishers.
Qualification-only synchronization uses the existing BroadcastReady observation
to inspect the family mutex span, with producer joins before observer retirement.
Dense history uses 20,000 entries and a realistic bounded candidate population,
not 20,000 resolver candidates or an expanded normalization cache.

The first targeted run compiled and exposed two inconsistent test fixtures:
removing a ranked rival left its authoritative winner fallback intact, and a
direct release fixture skipped reporter-field preparation that production
performs before scheduler ownership. Normalization alone did not fill the
stripped reporter field, which the literal-byte check caught again. The fixtures
now remove both support paths when asserting absence and run the existing
preparation/finalization stages before envelope construction. The literal byte
oracle is unchanged. Neither failure required a production behavior change.

The first cost check measured 2.34-2.85ms for hidden dense history and
4.33-5.94ms for admitted repeats, with zero allocations. Its bounded full
history scan was unnecessary: only identities represented in the fresh
publication can authorize hiding. A read-only design/quality challenge verified
the candidate-driven refinement against the indexing invariant and all gates.
Arbitration now looks up represented literals in the three existing bucket maps;
per-lane literal equality prevents canonical alias pooling. Winner/runner-only
fallback and zero-ranked overrides retain the unchanged helper contract. No new
cache, index or retained population was added. Expiry still visits actual
retiring entries. Baseline timing evidence: `F2-bench.log`; final timings are
reported after remeasurement.

Production implementation: the existing family mutex owns one fresh selection,
lane comparison, nonblocking queue admission and compact-witness commitment.
Repeated identities reevaluate rivals; dominance does not delete them. Each
lane retains its own normalized literal, frequency and inclusive timestamp.
New retained strings are privately copied after admission, preventing source
substring retention; callbacks and resolver rows are never retained.
`TryBroadcastSpotOwned` adds an admission result while the void APIs, stage
marker, queue drops/logging and immutable pointer ownership remain intact.
Refused broadcasts retain attempted temporal/lastOutput semantics and acquire
no direct SELF fallback. Shared destination/history gates are independent.

Decision disposition: ADR-0295 partially supersedes the affected display
clauses of ADR-0014/0033/0148; TSR-0087 preserves the admission counterexample.
YAML/config edits are comments only; existing keys, defaults and values stay
active. Source-map routing and the domain contract describe the selected policy.
Final targeted ordinary/race and complete validation passed.

Read-only final Go quality review found no material production/oracle defect.
It verified candidate-driven lookup, fallback/zero-rank precedence, per-lane
literal ownership, coupled bounds and the live SELF refusal barrier. Its two
closeout refinements are accepted: comments now distinguish admitted offers
from client visibility, and fixture cohesion is recorded here. The 735-line
unit matrix exercises one private owner with shared exact mask/state invariants;
the 553-line delivery cohort shares loaded construction, TCP ownership and
independent persistence fields. Linear fixtures keep setup, synchronization,
ordered readback and cleanup together, rather than scattering their ownership.
Their size is a navigation cost, not an unreviewed monolithic production change.

Fresh standalone `gopls references -d` confirmed the four production plan
callers and its benchmark; the admission API is used by the existing void
wrapper and the pipeline's sole final handoff, with repaired-state tests.
The MCP session initially returned stale positions/old benchmark arguments;
standalone current-disk references, compilation and direct source inspection
replace that stale evidence. Config comments passed the comment-only checker;
no schema, defaults, values or wire/persistence formats changed.

Final targeted commands, each using the canonical Windows executable paths:

```powershell
& .\scripts\test-windows.ps1 -Packages './internal/cluster' -Run 'TelnetFamily|FamilyDelivery|PeerOriginal(FinalSuppression|Refusal)|OutputPipeline(Stabilizer|FinalMed|.*Secondary)|FTConfidence' -Timeout 180s
```

The same command with `-Race` also passed. Additional telnet admission, snapshot
and SELF controls passed ordinary/race, selecting
`^(TestTryBroadcastSpotOwned|TestBroadcastSpot|TestDeliverSelfSpot)` in `./telnet`;
published-eligibility controls passed both modes with `^TestP3LookupEligible`
in `./spot`. Evidence: `F2-targeted-final*.log`, `F2-telnet-targeted*.log` and
`F2-eligible-controls*.log` under `.tmp/temporal-family-delivery/`.

Qualification builds acquired the SAME runner lock, compiled with
`go test -c -tags qualification -o` into the canonical cluster executable,
and directly executed from `internal/cluster` with:

```text
-test.run=^TestTelnetFamily(ConcurrentAdmission|QueueRefusalNoSELFFallback)Qualification$ -test.count=3 -test.timeout=180s -test.paniconexit0
```

Ordinary and race both passed. Thirty controlled subcase runs per build cover
both identity/consumer orders, success versus exact two-drop full refusal,
and a live PASS SELF client after ordinary/release refusal. The synchronous
BroadcastReady gate establishes mutex ownership across selection/admission;
producer joins precede observer retirement. The BroadcastReceived gate prevents
queue drain, then a positive FIFO TCP barrier exposes any direct SELF retry.
Evidence: `F2-qualified-final.log`, `F2-qualified-final-race.log`.
The qualification-only sources also passed `go vet -tags qualification
./internal/cluster ./telnet` and the configured lint lane with
`--build-tags qualification` for those packages (zero issues); this covers
files omitted by the default build. Evidence: `F2-qualified-vet.log` and
`F2-qualified-lint.log`.

Benchmarks likewise compile into canonical paths under the shared lock and
execute directly with `-test.run=^$`, `-test.benchmem`, `-test.benchtime=300ms`,
`-test.count=3` and `-test.paniconexit0`. Selectors:
`^BenchmarkTelnetFamilySuppressor` in cluster and
`^BenchmarkP3LookupEligibleProjection/all_fresh` in spot. On this native Windows
i9-10900, final minimum-maximum measurements were:

| Cohort | Time/op | Bytes/op | Allocs/op |
| --- | --- | --- | --- |
| Dense 20,000 history / 16 ranks, hidden | 3.755-4.326us | 0 | 0 |
| Dense admitted repeat | 3.687-3.780us | 0 | 0 |
| Dense refused repeat | 3.533-3.605us | 0 | 0 |
| Cap 2,048, new identity eviction | 691-793ns | 488 | 5 |
| All-fresh exact selection / 16 ranks | 864-903ns | 0 | 0 |
| Partial-expiry exact selection / 16 ranks | 11.27-14.59us | 13,659 | 30 |
| Five-key neighborhood selection / 72 ranks | 164.17-166.57us | 106,041-106,045 | 413 |
| Concurrent 64-entry repeated admission | 5.013-5.411us | 0 | 0 |
| Existing all-fresh LookupEligible typical | 150-160ns | 0 | 0 |
| Existing all-fresh LookupEligible cap 16x64 | 6.909-7.067us | 0 | 0 |

Actual owner publications contain 94 rows for each exact key; the five-key
cohort unions repeated receivers into 150 distinct receivers and 72 candidates.
Outcome, selection-count and retained-cardinality checks keep cohorts honest.
The new arbitration adds no per-entry projection/allocation; newly owned cache
strings and inherited partial/neighborhood projection costs are reported
separately. The first extra-runner attempt to list a sub-benchmark was correctly
refused because Go lists top-level names; selection validation was corrected
before executing the successful spot benchmark. Evidence: `F2-bench-final.log`
and `F2-eligible-bench.log`. These local averages establish no production p99,
RF accuracy or live-load result; operational profiles remain a follow-up.

Complete F2 checkpoint passed: all 70 packages ordinary (584.5s) and race
(961.0s), `go vet ./...`, `staticcheck ./...`, configured golangci-lint (zero
issues), and connected CustomSCP ordinary/race. Connected run identities:
`e359e20d20824bbebe8923bbcff6efd1` and
`17c97177ec0640488101504308a394b8`. Each connected run observed exact producer
bytes, independently reopened qualified support and actual consumer delivery
from the SAME database, including the missing-history control. Evidence:
`.tmp/temporal-family-delivery/F2/`, with all seven results in `checks.json`.
During the full lane only import grouping was formatted; behavior was unchanged
and final-state analyzers passed. Source implementation and test oracles stayed
settled throughout the checkpoint.

Final documentation checks passed for indexed/RCA-linked troubleshooting
records, comment-only YAML and three changed support-critical crawler entries.
Codemap generation/check is the final write/check before the F2 local commit.
Fresh lead verification confirms approved policy, immutable ownership, coupled
bounds, successful-only per-lane history, unchanged output formats and the
protected profiling hunks. The separate mathematical oracle described in the
ledger occurred before approval; engineering review and this lead verification
are not substituted for independent scientific evidence. No live feed, field
labels or production profile was used. Fresh key/window eligibility still does
not establish a single transmission episode, and support shares remain
uncalibrated measures of receiver evidence.

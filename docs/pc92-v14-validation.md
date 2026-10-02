# PC18/PC92 v14 validation plan and evidence

Status: **implemented; v14 correction-specific validation complete**. Overall
PC18/PC92 acceptance remains incomplete; see the final closeout below. The matrix below is
the pre-implementation plan; observed results are recorded later in this file.
The user authorized `Approved v14` on 2026-10-02; see the
[approved execution ledger](pc18-pc92-scope-ledger-v14.md). The lead accepted
and dispositioned all seven findings below as covered checker-only refinements
within that approval before production implementation began. They add no
product-policy or scope change.

This post-approval detailed test-strategy-adversary review was read-only and
design-aware. It inspected the repository skill, current configuration loader
and contract validator, handshake/replay ownership, outbound backoff, admission
failure paths, writer queues, recovery publication, allocation proof/tests,
Q4/Q5/Q6 fixtures, absolute-deadline helper, and sustained recovery producer.
The reviewer did not edit source or execute tests. This is engineering evidence,
not independent certification or evidence that the implementation is correct.

The implementation design reviewed uses fixed manager-owned retry slots under
`Manager.mu`, existing controller service points, session/attempt correlation,
inbound retry ownership after password authentication, and outbound ownership
before dialing. Scalar enqueue/flush/target receipt progress is captured under
`queueMu`; successful Flush completion is reported outside that lock.

## Contract-to-test matrix

All `V14` test names below are **proposed checker names**, not claims that those
tests already exist or have passed. At matrix creation every row was planned/not run. Implementation
may refine a checker name while preserving its stated falsification obligation;
the final evidence must map the actual executed checker back to the row.

| Contract/invariant | Failure/boundary | Stimulus/fault | Required observable | Evidence level | False-green risk | Exact planned checker | Owner |
|---|---|---|---|---|---|---|---|
| One terminal outcome per actual retry attempt | Mailbox, writer, controller and Run report the same failure | Concurrent notifications for one identity/session/attempt | Exactly one failure event and one backoff advance; all reporters retire | Unit + race | Sequential calls miss real duplicate races | `TestPC92V14RetryOutcomeOnce` | `peer` retry tests |
| Old callbacks cannot affect replacement | Old session reports after replacement wins | Hold old writer/Run callback; establish replacement; release old callback | Replacement ownership, delay and healthy interval unchanged | Integration + race | Comparing only session pointer misses reused identity state | `TestPC92V14RetryStaleOutcome` | `peer` lifecycle tests |
| Controller invalidates old ingress before replacement startup | Backoff expires while invalidation is queued | Stall controller before failure drain, then authenticate ready replacement | No startup grant before factual invalidation acknowledgment; grant possible after acknowledgment | Integration | Only checking cooldown allows stale topology authority | `TestPC92V14RetryInvalidationBarrier` | `peer` controller/session tests |
| Refusal is atomic | Graph/cache/freshness partially advance | Refuse valid large record at each admission boundary; later send identical timestamp/key through eligible owner | Before/after authority unchanged except intended ingress incompleteness; later valid admission succeeds | Unit + integration | Only checking cardinality misses replacement or watermark mutation | `TestPC92V14RetryRefusalAuthority` plus existing raw-authority tests | `peer` authority tests |
| Retry depends on new actual admission, not old witness fit | Old large record remains impossible | Keep 8,000-member refused C infeasible; retry with small valid record | Small record commits while large fixture remains demonstrably infeasible | Qualification | Releasing enough capacity accidentally proves old algorithm too | `TestPC92V14RetryFreshAdmission` | qualification fixture |
| Authentication precedes retry ownership | Invalid password/callsign/IP takes slot or delays identity | Invalid and valid concurrent inbound attempts | Invalid arrivals never grant, own identity retry candidate, advance delay or change fairness position | Wire integration + race | Calling coordinator directly bypasses authentication | `TestPC92V14RetryAuthenticationBoundary` | session integration |
| One overload candidate per identity across directions | Simultaneous inbound and outbound attempts | Synchronize inbound authenticated arrival with outbound reservation/dial | At most one retry candidate; loser releases all reservations without terminal-attempt double count | Integration + race | Testing only registered sessions misses duplicate handshakes | `TestPC92V14RetryDirectionRace` | manager/session tests |
| Ordinary admission remains unchanged | Retry machinery applies to all candidates | Ordinary, non-overload 128-candidate Q4B staging/winner race | Existing pending/staging/deadline contract remains reachable | Qualification | Testing only overloaded candidates hides a reduced ordinary cap | Existing `TestQ4StagingPlanIndependentBoundaries` and full Q4B run | capacity harness |
| Failed outbound dial advances identity history but consumes no startup grant | Grant taken before successful dial | Refuse TCP connection while another identity is ready | Failed dial advances configured identity delay once; other identity gets next legal grant without phantom delay | Integration | Synthetic outcome events omit actual dial path | `TestPC92V14RetryDialFailure` | outbound integration |
| Shared overload backoff preserves configuration semantics | Double wait, wrong sentinel normalization, success resets early | Explicit 2s/300s config; base>max; zero/negative direct constructor; loader zeros | Literal expected delay sequences and one waiting interval per failure | Unit + integration | Using production `newBackoff` to compute expected values | `TestPC92V14RetryBackoffContract` | retry/config tests |
| Startup pacing is global and has no accumulated credit | Idle period permits burst | Several ready identities after long idle, plus advancing service time | Consecutive startup-grant elapsed timestamps differ by at least one second | Unit + real-time integration | Comparing production `nextDue` merely restates algorithm | `TestPC92V14RetryGlobalPacing` | retry tests/oracle |
| Fairness survives candidate replacement | New candidate resets identity rank | N continuously ready identities; repeatedly replace selected candidates | A continuously eligible identity is overtaken by no more than N-1 grants | Unit + concurrent integration | Treating every new session as new fairness identity hides starvation | `TestPC92V14RetryFairness` at N=1,2,8,63,64 | retry oracle |
| Missing/ineligible identities reserve no grant | Ring holes stall ready peers | Configured absent identities, expired candidates and global-gated candidates around a ready one | Ready eligible identity receives next legal grant; no empty grants | Unit + integration | Dense all-ready fixture cannot expose holes | `TestPC92V14RetryFairnessHoles` | retry tests |
| Original handshake deadline remains absolute | Waiting restarts init/login timeout | Hold startup grant through original deadline, then release | Candidate closes by original deadline; no protocol startup after expiry | Wire integration | Observer returns successful result after blocking beyond deadline | `TestPC92V14RetryHandshakeDeadline` plus existing `TestPC92V12DeadlineWaiters` | handshake tests |
| Default 64-way wave tolerates candidate expiry without extending deadlines | Assumes 64 starts fit within inbound 60s | 64 eligible inbound attempts under default init deadline | Expired attempts are counted/retired; continuing retries eventually progress fairly; no 64th-start-before-63s assumption | Qualification | Extending fixture timeout masks production deadline extension | `TestPC92V14RetryFullWave` | qualification retry harness |
| Global clock/publication closure interrupts healthy interval without ordinary failure increment | Gate closure restarts cooldown or loses real failure | Gate-only closure and both orderings of real failure versus gate/retirement | Gate-only preserves history/due; genuine prior failure counted once; reset interval interrupted | Unit + race + Q6 | A single ordering cannot prove precedence | `TestPC92V14RetryGlobalGatePrecedence`; existing global-gate Q6 variants | retry/Q6 tests |
| Retry reset requires matching post-establishment C/A local Flush | Initial A/K or enqueue mistaken for success | Allow initial output; block post-establishment A Flush | No healthy interval/reset while A pending; C and A physically ordered | Writer integration | `sendRecord` can return nil after closing failed recipient | `TestPC92V14RetryRecoveryFlush` | writer/publication tests |
| Flush target registration cannot lose fast completion | Writer flushes before target is installed | Immediate-success writer synchronized against enqueue registration | Exactly one matching completion, no missing reset eligibility, no stale target | Concurrent unit + race | Slow socket fixture never hits fast-writer ordering | `TestPC92V14RetryFastFlushRegistration` | transport tests |
| Failed/canceled/stale Flush does not satisfy recovery | Old generation completes or A errors | C succeeds; fail/cancel A; deliver old-generation completion after replacement | No eligibility/reset for failed or replacement attempt | Writer integration + race | Success-only net.Pipe test proves little | `TestPC92V14RetryFlushFailureAndStale` | transport/retry tests |
| Healthy interval begins after both replay/establishment and recovery Flush | One condition completes much earlier | Test both orderings; stagger replay completion and Flush | Reset not before later endpoint+60s; under qualified service observed within next second | Unit + real-time qualification | Oracle reads production `healthySince` | `TestPC92V14RetryHealthyResetBoundary` | retry oracle |
| Quiet PC9x can reset; legacy cannot | Reset depends on received traffic or accepts fallback | Quiet valid PC9x; legacy fallback; no remote authoritative C | PC9x history resets; legacy history does not; remote completeness remains false until remote C | Integration | Sending periodic test traffic accidentally supplies hidden reset condition | `TestPC92V14RetryQuietAndLegacy` | session/authority tests |
| Immutable C/A precedes later membership changes without extending one-second queue admission | Recovery A stalls while withdrawal arrives | Change local membership after C begins; load timestamps and maintenance | Matching baseline A precedes D; D queued <=1s from change; complete C/A delivered <=5s from handshake | Integration + load qualification | End-state topology hides wrong order or lateness | `TestPC92V14RetryMembershipDuringRecovery` with periodic C/K both enabled and zero | publication oracle |
| Stop/cancel releases retry candidates, waiters and callbacks | Waiter survives shutdown or calls retired controller | Stop during dial, grant wait, replay and blocked writer; hold terminal callback | All bounded owners release after expected callback completion; no post-stop grant/reset | Lifecycle + race | Goroutine count sampled before deferred retirement | `TestPC92V14RetryCancellationRetirement` | lifecycle tests |
| Incremental fixed bookkeeping <=32KiB | Small field addition crosses allocator class for 641 objects | Actual source layouts; all retained session identities and coordinator backing | Conservatively rounded positive layout/backing deltas <=32KiB; transport channel item/storage unchanged | Allocation-layout unit + source inventory | Summing raw field widths or only 192 live sessions understates memory | `TestPC92V14RetryFixedStateEnvelope`; existing metadata/transport allocation checks | allocation proof/tests |
| Cap key is a required raw integer in 1-64 | yaml.v3 silently truncates float; disabled bypass | Missing/null/float/string/bool/negative/zero/65/huge plus valid integers, with peering enabled and disabled | Loader rejects malformed key before typed decode; accepts valid key | Loader consumer | Testing only typed struct cannot expose float truncation | `TestPeeringMaxPeersRawContract` | `config` |
| Active row count obeys cap; dormant behavior preserved | Counts rows incorrectly or silently subsets | N enabled identities; N+1; disabled rows; `direction:both`; globally disabled rows>N | Active N+1 fails; N distinct succeeds; both counts once; dormant overcount remains loadable | Loader + constructor | Enabled boolean applied differently in two entry points | `TestPeeringMaxPeersActiveContract` | `config` + `peer` |
| Direct constructor rejects before allocating side effects | Validation follows DB/open/workers | Missing/invalid cap and enabled-row overflow with persistence enabled | Error before DB path creation, listener or worker ownership | Consumer integration | Error alone passes even after leaked resources | `TestNewManagerMaxPeersValidationBeforeResources` | `peer` |
| N reaches all relevant resource consumers | Fixed64 remains hidden logical cap | N=1,2,8,63,64; exercise registration/replay/publication and owner turnover | Established/retry/replay<=N; transport<=N+128; pending<=128; actual publication excludes phantom nodes | Unit + integration + capacity qualification | Merely checking channel capacities skips runtime rejection and counts | `TestPC92V14ConfiguredCapacityConsumers` | manager/publication/capacity tests |
| Cache original admission age and strict TTL are preserved | Duplicate renewal, observer pruning or early expiry | Repeated duplicate reads near 600s; observe without prune; exact boundary and later cleanup | Age unchanged; retained at equality; expires only >600s; observers never alter state | Unit + Q5 | Recovery success used as proxy for cache expiry | Existing `TestPC92V12Q5ObservationDoesNotPrune`, `TestPC92V12Q5RejectsDuplicateAgeRenewal`, revised Q5 isolation | dedupe/Q5 oracle |
| Retry oracle rejects malformed evidence | Missing/duplicate/out-of-order/wrong-generation event accepted | Feed independent malformed event traces and observer overflow | Each negative control fails; valid factual trace passes | Checker unit | Deriving expected due/reset from production state | `TestPC92V14RetryOracleNegativeControls` | qualification oracle |
| Sustained workload actually offers approved traffic | Dropped ticker ticks lower load | Artificially delay producer and observer; real scheduled 100 PC92/s and approved spot/message mix | Scheduled versus actual counts and lateness fail underproduction; all mandatory deliveries reconciled | Harness negative control + qualification | Old producer reports only received ticks and can pass at reduced rate | `TestPC92V14WorkloadUnderproductionRejected` and `TestPC92V14RetrySustainedQualification` | workload harness |
| Retry service remains bounded under maximum pressure | Repeated graph scans still occur or grants starve live service | 0-blocker control;63-blocked+1live;63recovering+1healthy;64recovering+maintenance; near-full graph/large C/1,000 locals | Input workload met, healthy source remains live, membership/deadlines met, bounded cleanup; comparative profile identifies retry cost | Profile + short gates then >=30min qualification | Idle blocked entries prove little about actual retry work | `TestPC92V14RetryCostGate`, `TestPC92V14RetrySustainedQualification` | qualification/profile evidence |
| Final verdict corresponds to tested final source | Stale binary, partial run, changed CTY or source | Build/run wrapper with manifests and retained executable; failure/interrupt negative control | Final authoritative verdict only after required phases; binary/hash/manifests/CTY pinned | Workflow fixture + final execution | Earlier passing phase mistaken for overall acceptance | Final-source qualification wrapper and retained artifacts | lead/scripts |

## Accepted checker refinements

The lead accepted each of the following seven findings as a covered
checker-only refinement within approved v14. No remaining material scope gap or
normative conflict was identified by this review. Recording a planned checker
does not constitute implementation or validation evidence.

1. **Backoff oracle.** Use explicit literal expectations. At the inspected
   baseline, direct-constructor `base<=0,max<=0` becomes a **1-second constant
   delay**, through `runOutbound` clamping and `newBackoff`; loader zeros become
   **2 seconds/300 seconds**. Preserve both entry-point semantics under v14's
   existing-normalization clause. Do not use the production backoff helper as
   the expected-results oracle.
2. **Allocation oracle.** Measure allocator-class growth, including
   Manager/controller backing and any callback or timer additions. A
   641-session charge must use the actual rounded session allocation delta;
   neither raw new-field bytes nor only active-session count is adequate.
   This does not close the existing context/retirement/SQLite aggregate gaps.
3. **Retry timing oracle.** Derive due times from factual failures plus
   independently specified configuration, grant times from observed startup
   events, and reset eligibility from separate establishment/replay and
   successful matching Flush events. Never read production `nextDue` or
   `healthySince` for expected results.
4. **Offered-load checker.** The old sustained producer uses a 10ms ticker and
   counts only delivered ticks. Replace its acceptance oracle with an
   independent wall-time schedule and explicit scheduled/offered/late counts.
   Include an intentionally stalled-producer negative control.
5. **Fast-writer test.** The enqueue/target registration race needs forced
   synchronization, not repetition alone. Verify both immediate Flush before
   enqueue returns and A failure/cancellation after C success.
6. **Cap consumer tests.** Test logical limits with smaller N, then retain the
   full N=64 qualification. Merely changing fixed-index capacities or searching
   for literal64 cannot prove behavior. Keep ordinary Q4B's128-candidate
   workload.
7. **Evidence closeout.** Report audit/retry corrections independently from
   full480MiB/overall acceptance. None of these planned checks authorizes
   claiming the unresolved aggregate proofs complete.

## Evidence to append after execution

For each executed checker, record its command or retained-binary invocation,
relevant source manifest, observed result and artifact path. Record failed,
skipped, interrupted and superseded results explicitly. The final closeout must
map the actual checks to the matrix, identify remaining gaps and retain separate
results for correction completion and overall acceptance. At creation of this
record, every implementation, allocation, lifecycle, load, interoperability and
final-source qualification result was **not run for v14**. Subsequent observations
are recorded below; the matrix's proposed checker names are not execution claims.

## Implementation and distinguishing checks

| Approved items | Implementation | Actual evidence surface |
| --- | --- | --- |
|V14-01/02|config raw/typed validation, YAML; Manager/ownership/controller/publication limits|`TestPeeringMaxPeers*`, `TestNewManagerMaxPeersValidationBeforeResources`, `TestConfiguredCapacityConsumers`|
|V14-03/04/05/06|`pc92_retry.go`, scalar failure handoff, manager/session startup and retirement|`pc92_retry_contract_test.go`, `pc92_retry_auth_test.go`, `pc92_retry_fairness_test.go`, `pc92_retry_startup_test.go`; preserved authority boundary tests|
|V14-07/08|FIFO writer receipt, post-establishment A marking, later endpoint timing and global-gate serialization|`pc92_retry_writer_test.go`, healthy/reset/gate/legacy tests, actual retry wave and Q6|
|V14-09|fixed lifetime-owned ring/timers, unchanged channel elements, phase counts|`TestPC92V14RetryFixedAllocationEnvelope`, `TestPC92V14RetryTimerReusedAcrossAttemptsAndReset`, allocation/bookkeeping/ownership tests|
|V14-10/11|factual event oracle, absolute schedule and bounded mixed delivery evidence, TCP retry wave|retry oracle/negative controls, mixed missing/duplicate/late/generation controls, cost gate, full retry/Q5/Q6 wrappers|
|V14-12|narrow tagged error/context/switch/time/constant checker repairs|tagged vet/lint and affected qualification tests|
|V14-13|ADR-0233, TSR-0035, runtime/config/support/allocation docs and maps; retained-binary retry wrapper|workflow/TSR/map/diff checks and86 final wrapper fixtures plus33 observation fixtures|

Development checks observed on2026-10-02 include full normal tests, full race,
vet and staticcheck; targeted normal/race lifecycle, config and actual pinned
receiver regressions; tagged peer tests/vet/lint; and30-second raw-identity and
atomic-decoder fuzz campaigns (152777 and131663 executions respectively).
These were development-state checks, with affected checks repeated after review
fixes; final-state lane results and long profile verdicts must be recorded before
correction closeout. Logs live in `D:\codex-gocluster-v14-20261002\logs`.

The changed fixed-owner proof is28032 bytes:9472 Manager/coordinator growth,
zero session allocation-class growth across641 identities,18432 for64 reusable
timer backings, and128 for two overlapping six-counter samples. The timer object
is reused across attempts to bound runtime-heap zombie generations; Stop alone
would not establish that bound. This is not the complete480MiB proof.

The corrected short service comparison offered and committed all399 scheduled
PC92 records plus7 PC93 in each4-second case.63blocked+one live had peak input1
and24.87ms membership queue admission; zero blockers had peak1 and23.60ms.
The first actual63-way75-second PC92/PC93 diagnostic completed7499/7499/7499
scheduled/offered/committed PC92,125 PC93, all retry/reset oracles and recovery
in208.15seconds overall. It predated the combined mixed fixture and cannot
substitute for its final source qualification.

The first combined mixed preflight failed mandatory spot delivery. Its direct
constructor fixture omitted `WriteQueueSize`, producing the documented zero-size
normal queue, which refuses all nonblocking normal admission. The fixture now
explicitly selects the shipped128; no runtime fallback or queue policy changed.
The failed run is retained as negative evidence that missing delivery fails the
oracle, not relabeled as a successful load result.

Fresh design-aware review findings were dispositioned before qualification:
use occurrence-time max for establishment/Flush callbacks; fence startup queue
admission against global closure while closing sockets only after unlocking;
reuse one timer object per identity; preserve established legacy attempts across
PC9x-only gates; report eligible retired histories accurately; and account every
scheduled input and mandatory output. These engineering reviews are not an
independent scientific certification.

Support-agent impact: **Required**, reflected in the peer support card/index.
ADR-0233 supersedes only admission-recovery clauses; TSR-0035 preserves the
failed exact-check experiment and records the new ownership findings.

Correction closeout: **complete for v14**, with final evidence below. Overall acceptance:
**incomplete**, with enabled-SQLite/context/retirement allocation proofs and
required final-source overall workload profiles still open. No deployment,
restart, commit or push is authorized or claimed.

## Final verification and qualification attempts

The final relevant production state passed `go test ./...`, `go test -race
./...`, `go vet ./...`, `staticcheck ./...` and `golangci-lint run ./...`.
Qualification-tagged peer race tests passed in125.740 seconds, with tagged vet
and lint also passing. Generated maps, workflow contract and diff checks passed.
The final lane logs use the `*-final.txt` names in the artifact directory above.

The combined75-second63-peer diagnostic passed in210.46 seconds, including
mandatory delivery, reset and a subsequent failure returning to the2-second
base delay. Its exact intermediate counts were not retained in a complete log;
do not treat inferred counts as observed evidence. The64-peer diagnostic passed
in213.68 seconds, with all original60-second initialization deadlines retained;
its log is `D:\codex-gocluster-v14-20261002\provisional\retry-wave-64.log`.

The CPU-profiled short comparison passed with399 scheduled/offered/committed
PC92 records and7 PC93 in each case. Observed membership queue admission was
30.5021ms with63 blocked identities and25.8206ms in the zero-blocker control.
The profile includes fixture construction and teardown, so its substantial
`loseIngress` sample share is not a steady-state retry cost attribution.
Artifacts: `logs/retry-cost-enabled2.txt`, `logs/retry-cost-enabled.pprof` and
`logs/retry-cost-top.txt`. An earlier invocation skipped the opt-in tests, and
one retained-binary invocation failed argument parsing; neither is evidence.

The retained-binary cache-memory profile passed measurement and provenance at
`D:\codex-gocluster-v14-20261002\final-cache-memory\verdict.json`; its aggregate
qualification flags remain false. The first retained-binary retry/Q5/Q6 runs
were explicitly interrupted on2026-10-02 after a fresh review found a retry
fixture error-classification gap: a post-handshake recovery order/deadline error
could be swallowed as an expected candidate expiry. Their `final-retry`,
`final-q5` and `final-q6` verdicts correctly remain failed (`test_failed: exit
-1`). No positive sustained result is claimed from those runs. The checker is
being tightened within V14-10/11 before fresh final-source execution.

The same fresh review required two further checker refinements: trigger a local
withdrawal, join and IP change immediately after a retry recovery C admission,
then require its immutable matching A before those deltas and every delta
within the original one-second admission deadline; and verify the actual local
recipient of private PC93, not just its private-versus-announcement class. The
mixed retry profile covers the overlap with periodic C/K disabled in the full
30-minute case and enabled in an additional75-second case. The64-peer
maintenance case remains75 seconds. These close existing matrix obligations;
they do not change production policy or replace Q1-Q3 client latency evidence.

Corrected75-second mixed preflights then passed in210.06 seconds (periodic zero)
and210.28 seconds (enabled600/1800). Each offered/committed7500 PC92 records,
12500 distinct spot keys plus125000 duplicates,137500 ingests,63 announcements,
62 private messages and25 bulletins. Mandatory spot delivery reconciled
12398/12398 and12416/12416, with maximum producer lateness17.542ms and20.9181ms.
Both enforced the new membership-during-recovery check, all63 resets and the
post-reset base delay. Their complete logs are the `provisional/*-corrected.log`
files; executable SHA256 is
`EBA55D25EC463B5FA16FAE446C8FBB0C153300B5DCD7576B6358564EF43A71B6`.

The82 updated wrapper fixtures passed. Final checker race tests passed in6.205
seconds; tagged vet/lint, workflow/TSR/map and diff checks also passed. The first
new checker lint invocation found two test-only issues, both corrected before
these final checks. `final-cache-memory-corrected` passed measurement/provenance.

The second sustained attempt (`final-*-corrected`) was deliberately stopped
after read-only review found that the event oracle enforced every observed
delay but did not require a real300-second capped interval. Duration alone
cannot prove that boundary. Those verdicts remain failed/interrupted, even
though all Q6 fault subcases and the first Q5 cycle passed provisionally. The
full retry case must now exhibit and log an independently derived ninth-or-later
failure followed by a startup grant at least300 seconds later; absent or early
cap evidence must fail. This is an existing V14-11 evidence obligation, with no
production behavior change.

The final matrix reconciliation also distinguished quiet recovery-tail results
from reset timing under the qualified workload. Full qualification now keeps a
single absolute-schedule producer and queued maintenance alive for a declared
600 seconds after the original overload cutoff. The three fixed load durations
are2400/675/675 seconds (3750 total), inside the unchanged65-minute process
timeout. Per-peer handshake, five-second recovery, one-second membership and
60-to-61-second healthy reset bounds are unchanged. Stable recovered peers have
mandatory deliveries throughout the tail; the intentional post-reset fault has
an explicit lead-in that cannot waive already admitted obligations. Short
preflights remain diagnostic and do not prove this loaded tail.

The mixed checker now counts actually parsed PC92 actions against the literal
45/45/2/8 profile and checks every C has8000 members with64035 bytes plus its
1-to-8-byte timestamp (excluding CRLF). Wrong-mixture and undersized-frame
negative controls passed. The86 final wrapper fixtures reject both shortened
cases and overload-only runs missing the loaded tail. Final-source execution
was still required at this stage; its subsequent results follow.

## Final v14 correction closeout

On 2026-10-02, all four final retained-binary wrappers exited zero with
`measurement_passed`, `provenance_passed` and `profile_accepted` true. Each
reported `qualified=false` and `overall_accepted=false`; those aggregate flags
remain correct. Earlier interrupted attempts remain failed evidence.

Artifact root: `D:\codex-gocluster-v14-20261002`. Each directory below retains
the executable, test log, before/build/after source manifests and authoritative
`verdict.json`. All three manifests matched within each run. The frozen source
was clean branch `p92` at `d9211f85eb356d51ec7f7269a486377d42ca9722`.

| Artifact directory | Run ID | Measured execution | Result |
| --- | --- | --- | --- |
| `qualified-v14-retry` | `eb8c4be65b0344bfad0bb76f983fbaef` | 3780.491 s | All three full load windows, recovery/reset and subsequent-failure checks passed |
| `qualified-v14-q5` | `b99dc8c080d9470584dafbfd52ab0514` | 2606.963 s | Three real expiry cycles in each of four isolated cache classes, followed by terminal zero-key cleanup |
| `qualified-v14-q6` | `c5a027d214fa4e5992db6a891fba74a3` | 1580.531 s | Fault cases with periodic publication enabled/disabled and the 1200-second receive-only case passed |
| `qualified-v14-cache-memory` | `2035daae70d64bcabc52902a409fed00` | 0.863 s | Retained cache allocation profile passed; not an aggregate protocol-memory proof |

The three qualification executables have SHA256
`58394BF1F395C3DDB2E79FB4045B2049EBA3F1349FB414405549AF79DF5A2E3C`;
the cache-memory executable has SHA256
`CB3E03EFFEDB9F4F1C14ADD96942AE006E671E6395D150192E9D744B2CB34780`.
The exact CTY plist hash in the manifests is
`BA9FFE6B669E144A383F70DBB01F8A48ACB534F90DDF712E2EC0CD4FF59D1D8D`.
Q6 used the pinned DXSpider receiver
`3e9b3621d94dd45c68702e4a0f896aac33f2a91d`.
Execution used Go 1.26.4, Windows/amd64, `GOMAXPROCS=2`, `GOGC=50`, and
`GOMEMLIMIT=1536MiB` on an Intel i9-10900 (10 cores/20 logical processors).
GOMEMLIMIT is a runtime setting, not the 480 MiB owned-protocol ceiling.

The 40-minute 63-recovering-plus-healthy case offered and committed 240,000
PC92 records: 108,000 A, 108,000 D, 4,800 C and 19,200 K. It admitted 400,000
distinct spot-forwarding keys plus 4,000,000 duplicates, verified 4,400,000
local ingests, 2,000 announcements, 2,000 private messages and 800 bulletins.
All 4,869,418 mandatory peer deliveries reconciled. Maximum producer lateness
was 23.0462 ms. C records contained 8,000 members and measured 64,040–64,043
bytes excluding CRLF. Healthy membership queue admission was 100.4883 ms;
the separate recovery-overlap oracle also passed. A factual ninth-failure
retry interval measured 300.0090257 seconds. After all 63 healthy resets,
another failure returned to the base delay in 2.0027664 seconds.

The 675-second periodic-enabled mixed case offered and committed 67,500 PC92,
112,500 distinct spot keys and 1,125,000 duplicates. All 5,983,658 mandatory
peer deliveries reconciled; maximum producer lateness was 15.9035 ms and
healthy membership queue admission was 97.3061 ms. Its post-reset retry took
2.0006834 seconds. The 675-second 64-peer case retained queued maintenance,
completed all resets and measured the next retry at 2.0008931 seconds. All
three cases continued their declared workload for the full 600-second tail;
original handshake and recovery deadlines were not extended. The enabled
periodic case spans the K interval, not the 1800-second periodic C interval.

The final qualification-tagged race suite passed in 116.559 seconds after the
last checker refinements; tagged vet/lint and all 86 wrapper fixtures passed.
These supplement the production normal/race/static checks recorded above.
The cache-memory profile measured simultaneous four-class retained heap growth
of 82,187,904 bytes; this is scoped cache evidence, not SQLite/context/retirement
coverage. The changed fixed retry ownership proof remains 28,032 bytes.

A fresh lead-owned verification reconciled the approved items, actual final
source, checker obligations, retained verdicts and claim limits. Engineering
reviews were design-aware; no independent scientific certification occurred.
Only this evidence record and the ledger status were updated after the frozen
runs; production code and test harnesses were unchanged. Those documentation
updates are outside the retained run manifests and receive documentation checks.

V14 correction-specific implementation and validation are complete. Overall
acceptance remains incomplete pending the enabled-SQLite, context-child backing
and outer-retirement ownership proofs and remaining required final-source
Q1–Q4/full sustained-cache qualification. No deployment or restart was performed.
This closeout does not authorize a commit or push.

## Continued overall qualification on 2026-10-02

At the user's instruction to continue under the approved ledger, execution
resumed on clean source `c07483f34f43f31852b68ee46236119464e591b9` (the prior
closeout documentation commit; production and harness code unchanged).
Evidence root: `D:\codex-gocluster-v14-remaining-20261002`.

| Retained directory | Run ID | Execution | Observed result |
| --- | --- | --- | --- |
| `runtime-preflight` | `169c2c58b26a481b96abce4121792a96` | 56.476 s | Failed enqueue latency; all 385,827 required deliveries arrived |
| `runtime-profile` | `75589f5dc1884e1e94f0f155f3f7ff14` | 55.994 s | Profiled preflight also failed latency; profiling is diagnostic |
| `q4-preflight-a` | `ae6fb79d0b044f62be0b2c0327861ee8` | 485.203 s including setup | Passed short capacity/reachability checks, not full Q4 acceptance |
| `q4-preflight-b` | `57ebfdc827214bfdb500e904f560d296` | 481.610 s including setup | Passed short staging/winner-race checks, not full Q4 acceptance |
| `cache-sustained` | `9b3e04a6e57d4386a1145052096c4102` | 3360.215 s | Full 45-minute load and 11-minute drain passed |
| `shipped-q1` | `463f9122aa8947f6972c8be1e7273040` | 3392.295 s | Failed peer continuity and required delivery; diagnosis below |

Every wrapper above passed source/binary provenance, including failed runs.
The tagged runtime executable SHA256 was
`B76672212404C0B05F6A0CFBB46FF1A803A9BA2FEFD900BF6EB4DC4D606AA5CB`;
cache-sustained used the previously retained cache executable hash
`CB3E03EFFEDB9F4F1C14ADD96942AE006E671E6395D150192E9D744B2CB34780`.
The strict latency preflight and its separate CPU-profile repeat ran without
other qualification load tests. Capacity diagnostics and the informational
shipped-settings profile overlapped portions of cache-sustained; that host
concurrency is not concealed or used to qualify strict latency.

The unprofiled preflight offered 3,334 new spot keys, 33,340 duplicates, 2,000
PC92 records, 34 PC93 and seven bulletins. Ninety of 100 clients exceeded the
5 ms enqueue p99 threshold, producing 180 failed overall/minute cohort checks.
Client enqueue p99 histogram upper bounds ranged from 4.4 to 6.5 ms; first-byte
bounds ranged from 11.1 to 17.6 ms. No required delivery was missing, no cache
refused input, and no final global gate was active. Producer lateness peaked at
2.2448 ms. The profiled repeat failed all 100 clients' enqueue threshold, with
5.3–7.2 ms enqueue and 12.2–18.4 ms first-byte upper bounds. Its load-only
20-second CPU capture had 17.25 seconds of samples: client writerLoop accounted
for 49.91% cumulative samples, Windows WSASend for 51.25%, and the PC92
controller loop for 6.20%. These overlapping stacks must not be summed; they
identify investigation targets, not a proven causal explanation of p99 tails.
The profile and before/after allocation captures are retained in runtime-profile.

Capacity A reached 1,000 local users, 64 peers, 128 pending candidates, 4,096
nodes, 65,536 users, 131,072 edges, 262,144 ingress observations and 16,384
freshness entries. Capacity B retained 1,000 users/63 peers, exercised 128
candidate winner races, and reached 7,888 staged records charging the full
16,777,216-byte staging budget. The pressure-cycle portions were about ten
seconds; setup time does not substitute for either required 30-minute phase.
Both reports retain the open aggregate allocation dependency.

The full sustained cache profile admitted exactly 450,000 new spot keys,
4,500,000 duplicates, 270,000 PC92 keys, 4,500 PC93 keys and 900 bulletin keys,
without refusal. Spot occupancy stabilized at approximately 100,017 entries.
All four classes and expiry indexes drained to zero. This closes that component
workload obligation; it does not prove delivery, aggregate memory or latency.

The shipped-settings run offered all declared 45-minute traffic and completed
the 11-minute drain, but its final verdict failed. Fifteen receivers timed out
at 16:13:15 UTC, leaving 2,293,898 required PC92 relays missing. Each of 100
local clients also lacked eight required enqueue/read observations. Those
local misses are not explained by the peer timeout and remain under diagnosis.
Neither symptom is waived. The fixture only responds to received PC51 pings;
the pinned DXSpider source also initiates PC51 every 300 seconds
(`perl/DXProt.pm`, pingint and periodic ping branch). The fixture omitted that
healthy-peer behavior. Shipped GoCluster keepalive and idle settings are both
600 seconds; relying only on responses leaves a deadline/timer race. Production
settings and deadlines are not being changed to repair this fixture.

Two checker-only refinements were reviewed and implemented under inherited v7/S14 and
v14's qualification/closeout authority: bounded fixture-owned 300-second PC51
initiation throughout load/drain, and bounded missing-token examples from the
actual reader/enqueue owners. Their tests must reject early/wrong-address ping,
bad cancellation/stall behavior, missing outputs hidden by example caps, and
invented parent-side enqueue observations. They must preserve required
recipients, exact failure counts, rates, durations and all production semantics.
The lead dispositioned the detailed falsifiability matrix before implementation.
The heartbeat boundary/wire/busy-read/cancellation/write-failure controls and
missing-example count/recipient/remote-ownership/malformed-reply controls pass.
Missing examples are limited to 16 per recipient and observation type; exact
full missing counts remain decisive. The maximal child reply measured
1,509,338 bytes against the unchanged 2,097,152-byte packet limit. The shared
input layout is unchanged. The final integrated tagged package normal/race
tests passed in 2.806/5.167 seconds. Staticcheck passed again after the final
mechanical lint corrections. The final tagged golangci-lint 2.11.4 run still
reports 29 preexisting findings (two errorlint, 23 gosec, four quick-fix
staticcheck); the seven findings introduced by these refinements were fixed.
Tagged vet retains the existing Windows MapViewOfFile uintptr conversion
warning at pc92_runtime_mapping_windows_test.go:60. These findings are reported,
not treated as clean checks or silently repaired outside this scope.

The refined diagnostic preflight `runtime-corrected-preflight`, run ID
`c08dbb940e0544778202d3c8f2d8480d`, passed measurement and provenance in
55.789 seconds, with all required deliveries and 3.3–4.6 ms enqueue / 9.7–13.9 ms
first-byte per-client p99 bounds. Producer lateness peaked at 3.935 ms. Its
retained binary was
`9FD8409EBAAA7DE0E4019C7C9A18BFC8EA870C33E579430822B35B7BE5127030`.
This short repeat neither erases the earlier failures nor demonstrates a
performance repair: the five-minute fixture ping was not reached, and the
missing-example scan runs after measurement. No production optimization was
made. The subsequent lint-only fixture edits require the next retained binary.
Full runtime profiles remain necessary; no successful shipped-settings or
strict Q1–Q3 acceptance is claimed at this point.

Support-agent impact of these refinements: no new operator/runtime behavior;
existing support-card contracts remain unchanged. This evidence record owns
the qualification diagnostics. No ADR is needed for a test-fixture correction.

The separate `ownership-probe` diagnostic used an external Go overlay, leaving
the repository unchanged. It passed in 1.93 seconds. An injected blocked logger
held 160 post-Run inbound goroutines with zero owner permits, but weak references
proved all measured session objects and large dynamic rejection-error backing
were collected. Strong-root and ungated controls passed; releasing the sink
joined all outer goroutines. This disproves whole-session retention from lexical
capture for that pinned rejected-inbound path, not every error/build path. The
context-child-map backing and enabled SQLite proofs remain open. SQLite is also
used by FCC paths, so a process-wide heap cap is not a peer-only repair. The
read-only specialist and lead reviews were design-aware, not independent
scientific certification. Production source and settings remain unchanged.

### Full Q1 after fixture correction

`q1-corrected` completed the full 45-minute load and 11-minute drain on the
frozen refined fixture. Run ID `c1cf35ca7b5a41e6925886802c900508`, execution
3393.463 seconds, binary SHA256
`E0F0619D4E0E3B686677A1D611093606111965ACD3E769E71F23D2D7C8F4D65B`.
The wrapper passed source/binary provenance and correctly returned failure.

- All 16 peers remained established through the drain. All 4,050,000 required
  PC92 relays and all 6,750,000 required peer spot deliveries arrived, with no
  PC92 duplicate/unknown relays. Final caches drained to zero, without refusal
  or global gating. This verifies the healthy fixture's long-run liveness for
  Q1; it does not waive the remaining Q2/Q3 fault/pressure profiles.
- Offered counts were exactly 450,000 new spot keys, 4,500,000 duplicates,
  270,000 PC92, 4,500 PC93 and 900 bulletins. Maximum producer lateness was
  4.6596 ms. Of 52,067,250 required token deliveries, 52,066,350 arrived.
- Every one of 100 local clients missed the same nine spot inputs at both
  enqueue and read: 90004, 163460, 238635, 269385, 284511, 313619, 358299,
  358514 and 450793. The bounded diagnostics captured the complete set because
  nine is below their 16-example limit. Peer spot forwarding delivered all of
  these inputs. The prior shipped-profile omissions remain recorded separately.
- Every client's overall enqueue p99 exceeded 5 ms (12.1–13.8 ms histogram
  upper bounds). Overall first-byte p99 was 19.5–24.5 ms, but individual input
  minutes reached 36 ms. Across 4,600 client cohorts (overall plus 45 minutes),
  4,500 exceeded the enqueue threshold and 1,360 exceeded first-byte 25 ms.
  Minute zero passed latency; later sustained behavior did not. The report's
  4,600 failures comprise 100 missing-delivery checks and 4,500 combined
  delivery/latency cohort checks, not 4,600 distinct lost spots.

These observations establish a sustained runtime gate failure despite complete
peer forwarding. The earlier 20-second preflight is insufficient to establish
steady-load latency. Production dedupe and runtime optimization are unchanged;
the missing-token follow-up is an external read-only diagnostic.

### Missing-spot attribution and current scope boundary

The external `hash-collision-probe` matched the entire nine-ID missing set at
both observation owners for all 100 clients. Eight pairs collide in SLOW
secondary dedupe; one pair collides in primary dedupe. These are distinct
11-character callsigns, within both existing fixed-width key encodings. The
shared caches retain only a truncated 32-bit hash and do not compare full keys
(`dedup/secondary.go`, `dedup/deduplicator.go`, `spot/spot.go`).

| Missing input | Earlier distinct input | Cache | Equal 32-bit hash |
| --- | --- | --- | --- |
| 90004 | 36137 | SLOW | `380bb22d` |
| 163460 | 96942 | SLOW | `0ba7fa8f` |
| 238635 | 170190 | SLOW | `1b844355` |
| 269385 | 250302 | SLOW | `b54f730a` |
| 284511 | 274707 | Primary | `ad34e59e` |
| 313619 | 273870 | SLOW | `0722b540` |
| 358299 | 316096 | SLOW | `b4cce594` |
| 358514 | 305397 | SLOW | `29256a70` |
| 450793 | 389384 | SLOW | `ad984985` |

The initial ideal-schedule simulation was diagnostic only. Attribution then
used actual mapped 10 MHz input timestamps and the report's before-load/final
authority times. Q1's non-full population does not apply an authority offset.
The inferred UTC epoch interval was 17:06:19.563660–17:06:19.622059 UTC, widened
for conversion rounding; the analysis allowed another 10.6597 ms for the
publication-to-wire interval using the measured producer lateness and next
six-ms schedule boundary. Every pair endpoint stayed within one wire minute;
the smallest remaining minute-boundary margin was 3.226994 seconds. This is a
bounded reconstruction from source ordering and clock evidence, assuming no
unobserved wall-clock discontinuity; it is not a directly captured wall-clock
load-epoch field or a packet capture.

The retained pair-control binary invokes actual `processSpot` and
`ShouldForward`: the first input forwards, its distinct collided input is
suppressed, a different-hash control forwards, and exact expiry forwards.
All nine cases passed in 0.69 seconds. Binary SHA256:
`4C2AA45138D4E99E7E83008D6E9E0AF41EB20DD362229922083832DED4E07C51`.
`reconciliation.json`, `pair-controls.log` and `provenance.json` retain the
input/partner/callsign/minute evidence and source matches to the Q1 manifest.
The primary expiry control deliberately seeds only its isolated cache to test
that boundary; it does not inject authority into qualification. FAST/MED
simulation assumed blank grid and is not used as final attribution. The earlier
shipped run has no retained missing IDs, so its eight omissions are not given
the same token-level attribution.

The live run did not capture individual deduper drop traces. The reproduced
defect and exact nine-ID correspondence are stronger evidence than aggregate
counters alone, but do not constitute an instrumented trace of every live drop.

The fixture corrections are complete, and full Q1 exposes two
remaining acceptance failures: shared local spot dedupe loses distinct keys,
and sustained latency exceeds the existing limits. A 20-second CPU profile
does not establish the cause of steady-state latency after the first minute.
No production dedupe, correction, queue, runtime tuning or persistence change
has been made, and no required recipient or threshold has been relaxed.

Further Q2/Q3 and shipped-profile repeats are deferred until these production
failures are addressed; full Q4 remains gated on the allocation proof. They are
not waived or marked passed. V14 explicitly requires stopping when allocation
or service gates need changes outside its boundary. Shared dedupe/runtime
repairs, a bounded context/terminal-diagnostic ownership design and enabled
SQLite allocation work require a concrete follow-up scope. The approved v9
negative feasibility experiment did not authorize a production driver change.

Documentation now records the narrowed ownership evidence, qualification
outcomes and support guidance. TSR-0035 retains these troubleshooting lessons;
no new durable architecture choice or ADR was made. The final lead-owned review
checked the actual fixture diff, bounded diagnostics, retained verdicts and
claim limits. The reviews were design-aware, not independent certification.
All production code and runtime settings remain unchanged. Documentation
updates after Q1 are outside its frozen manifests and receive separate checks.

Final documentation checks passed: workflow contract against baseline
`2c0607986f3d3d9dc1921eb5b7c5ae00595143d2`, troubleshooting record/index
consistency, all generated code maps and diff whitespace. The runtime fanout
map initially reported stale test inventory; regenerating that one map added
the two qualification test files and refreshed its fingerprint, after which
the all-map check passed. Tagged lint restricted to the new diff against HEAD
reported zero issues; this does not erase the 29 full-package findings or the
existing vet warning documented above. No Go suite was rerun solely for these
documentation/generated-inventory updates. No commit, push, deployment or
restart was performed.

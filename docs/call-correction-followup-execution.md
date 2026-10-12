# Call correction follow-up execution

Authority: owner exact `Approved v2` on 2026-10-11 for the self-contained
[scope ledger](call-correction-followup-scope-ledger-v2.md). Baseline
`96c0a84a19110fa248eddab182e622b174b73082`; branch `fix_call_correction_followup`.
S1 through S4 are sequential; no patch is complete until its checkpoint passes.

## Logging prerequisite

Before production edits, ordinary and race runs passed for exact selection
`^(TestDailyFileSinkArchivesStaleActiveOnStartup|TestDailyFileSinkArchiveCollisionMergesWithoutOverwrite)$`
in `./internal/cluster`, timeout 180s, using scripts/test-windows.ps1 and its
canonical executable. Both exited zero with PASS. Evidence:
`.tmp/call-correction-followup/prerequisite/logging-ordinary.log` and
`logging-race.log`, with corresponding exit-result JSON files.

Historical missing-archive/file-sharing failures remain in prior logs. These
two passing prerequisite runs do not erase those failures or establish logging
repair. Logging changes remain separately authorized; any checkpoint recurrence
blocks progression without exclusion or waiver.

## S1: selected loss propagation

Postapproval test-strategy review completed before production mutation. The lead
accepted the checker matrix: qualify healthy/absent exact inputs and positive
history; actual cap admissions and scheduler retries; real expiry recovery;
base-one/ambiguity-three budget equality, both timeout actions and overflow;
actual temporal controller terminal/overflow abstention; unchanged healthy
exact low-P/state controls; delivered TCP/ring/reopened archive/original peer
evidence. No material scope gap was found. Distinct-receiver support readback
alone is not a once-only oracle; eligible occurrence controls are planned in S3.

Implemented initial request-local selected-loss capture, including temporal
terminal/overflow abstention, and fresh retry loss overlay on unchanged exact
delay inputs. No loss/receiver state was added to scheduler envelopes. The
ordinary correction selection stays local; an initial review's potential
context-pointer escape was removed before final targeted verification.

Observed checks before the full checkpoint:

- Exact selected inventory: 31 top-level cluster tests, including six new S1
  tests/13 subcases. Ordinary/race final runs passed after the local-selection
  refinement (`targeted-final-ordinary.log`, `targeted-final-race.log`). They
  cover actual cap loss, qualified healthy/absent exact evidence and S history,
  actual scheduler retries, budget equality, release/suppress, overflow,
  controller abstention, fresh recovery and healthy exact-input controls.
- Broken-state overlay restores baseline stage/delivery source without changing
  tracked files. New initial tests failed for `selected loss escaped a qualified
  hold`; this expected failure is recorded in `baseline-red.log/.json`.
- Matching healthy ordinary-stage benchmark: baseline 2028ns/op, final
  2138ns/op; both 1B/op and 0allocs/op. These are single local runs, not a latency
  improvement claim. Compiler escape evidence reports leaking override contents,
  with no ordinary selected-struct heap move (`escape.log`).
- Existing final selection controls: all-fresh capped16 0allocs/op, partial
  expiry 30allocs/op, five-key union 413allocs/op/106042B. Source inspection
  confirms no added initial neighborhood union (`bench-baseline.log`,
  `bench-final.log`).
- Tagged family qualification ordinary/race count3, plus tagged vet/lint,
  passed (`qualification-*.log`); these exercise existing delivery ownership.
- Troubleshooting-index/RCA, changed Go crawler comments and whitespace checks
  passed. Source-aware comment review preserves specific loss authority and
  exact policy meaning. Support-agent impact: confidence support card updated.

Read-only Go quality review found the potential initial selection escape above;
the lead fixed it and reran affected tests. Fresh review found no further
material production issue. Replay already supplies selected evidence to the
shared delay evaluator; this establishes loss-policy agreement, not identical
ordinary state/share input or actual scheduling.

Complete S1 checkpoint passed: 70 packages ordinary (596.1s) and race (981.9s),
53 test packages and 17 build-only in each; vet (2.9s), Staticcheck (4.8s),
configured lint (9.2s), and connected CustomSCP ordinary (10.2s)/race (16.7s).
All seven results and commands are in `.tmp/call-correction-followup/S1/checks.json`.
The formerly failing logging cases also passed inside both complete runs.

Changed-document relative file links passed (eight links); TSR/RCA/index,
support-critical comments and whitespace passed. Existing ADR-0030/0046/0292
govern S1; new TSR-0088 records the wiring failure and recovery lesson. No new
durable decision is selected by S1. Support card impact is the current selected
loss/stabilizer explanation, without a new percentage meaning.

Fresh final lead review confirms narrow production changes, exact delay-input
preservation, request-local scalar ownership, no saved loss or new retained
state, qualified regressions and correctly scoped cost/coverage claims. Final
codemap generation/check and direct diff review passed. S1 checkpoint commit:
`b0d780cdde27940377fa23d40d1c83a001194419`; tree clean before S2 began.

## S2: legacy history clocks and capacity

Postapproval detailed test-strategy review completed before production edits.
The accepted matrix covers untouched physical state/reference coupling, future
refusal under receiver/shard pressure, exact zero normalization, inclusive
nanosecond boundaries, before/after-wall caller clocks, actual owner startup
fences, actual maintenance dispatch/cadence and delivered floor consequences.
Consumers were confirmed in current source: live general floor/stabilizer/
resolver, replay stabilizer/resolver, reveng resolver only, live FT floor even
with global correction off. Reveng remains legacy with CustomSCP selected.

Initial targeted config tests exposed an incorrect fixture assumption: explicit
zero YAML values are already rejected before normalization, and these keys are
required rather than defaulted when omitted. The fixture now supplies positive
required YAML and preserves explicit-zero rejection; raw direct-constructor
sentinels are tested separately. No loader/default change was made. Failed runs
remain in `S2-config-targeted.log` and `S2-config-targeted-final.log`; subsequent
results are recorded separately.

Two externally changed `pipeline.yaml` values (stabilizer delay/check count)
were discovered after S1. They are preserved and excluded from this patch's
checkpoint commit; S2 owns only required setting explanations.

Implemented `RecentBandStore.RecordAt`/`CleanupAt`, inclusive upper query bounds
and preserved Boolean minima. Live wrappers use wall UTC; offline callers supply
evaluation time. Config's pure consumer validator runs before actual relevant
owners; reveng's extracted construction retains its previous resolver settings
and legacy backend. Replay removes legacy wall maintenance and uses one scalar
deadline from rows, samples and terminal drain, without workers/catch-up bursts.

Final targeted ordinary/race passes cover 10 new store tests (plus existing
store controls), four config tests, seven replay tests, two reveng tests and
three runtime tests plus existing FT/Custom capacity controls. Logs use
`S2-{spot,config,replay,reveng,cluster}-*.log`. The first delivery run's incorrect
DIAG assertion and FT client mode exclusion were repaired in the new fixture;
`S2-cluster-targeted.log` retains that failure and `*-final.log` the passing run.
Normal output has no numerical share; DIAG legitimately shows unavailable `--%`.

Tagged family qualification ordinary/race count3 passed. Fresh inherited-context
Go quality review found no material source/scope defect; it did not run checks
and was not independent. Preexisting replay terminal-clock advancement followed
by remaining earlier samples is preserved; no reconstruction of pruned history
is claimed. Final review confirms actual-owner predicates and deletion coupling.

Local cost: raw/normalized full queries16 took 1110/936ns and64 took2764/2767ns,
all8B/1allocation. A matching query-only overlay that restores the old retained-
length return took683/656ns and1829/1754ns with the same allocation. This isolates
the added bounded upper-filter pass; it is not a full old/new runtime comparison.
Future rejection16.5ns/0allocations; existing/overflow admission1allocation;
key churn439B/3allocations. Replay not-due dispatch5-6ns and due32-key sweeps
15.8/54.1us (16/64 receivers), all0allocations. No production latency claim.

Tool/package inventory refreshed: Go1.27.1, Staticcheck2026.2.1/v0.8.1,
golangci-lint2.14.0; 70 packages,53 test/17 build-only. YAML changes are comments
only for approved work, with the five-line header intact and positive required
keys/loader contracts unchanged. Support impact: confidence card and historical
freshness/replay docs updated. ADR-0296/TSR-0089 record the policy and diagnosis,
with relevant prior-record links. Relative file links34 and TSR/RCA/index passed.

Initial complete S2 run stopped at config after128.6s: the preexisting shipped-
YAML test requires delay15/checks5, while separate workspace edits supply5/6.
The failed ordinary log/checks.json remain in `S2/`. No test was excluded or
changed to accept unrelated values, and those user edits remain intact.

The approved staged patch is reproduced in the detached ignored worktree
`.tmp/call-correction-followup/S2-scoped-worktree`, with original15/5 values and
only approved YAML comments. Ignored copies of the Windows/connected runners
change only source/output-root routing: build/run from that source worktree,
use original canonical `.tmp/windows-tests/.../package.test.exe` paths and shared
lock, retain path safety/native-target/count/paniconexit0/stale-build controls.
Tracked workflow scripts are unchanged. The formerly blocked exact shipped-YAML
test passes there. This proves the approved patch state, not the unrelated dirty
YAML combination. The full seven checks are rerun from that worktree; results go
to `S2-scoped/checks.json`. S3/S4 production edits have not begun.

Complete scoped S2 checkpoint passed: 70 packages ordinary638.9s/race1040.7s
(53 test/17 build-only), vet5.6s, Staticcheck9.2s, configured lint13.7s,
connected CustomSCP ordinary10.4s/race17.0s. All seven commands/results are in
`S2-scoped/checks.json`; both logging cases passed within complete ordinary/race.
Tagged qualification vet/lint also passed. No logger repair, exclusion or waiver
was applied. The unrelated dirty-YAML shipped-value failure remains recorded.

Approved-item mapping: store admission/query/capacity/physical deletion coupling
-> `spot/recent_band.go` and clock/churn/concurrent tests; actual owner minima
-> config validator and runtime/replay/reveng startup fences/tests; live CW/RTTY/
FT clocks/delivered S/? consequences -> runtime recorder/tests; offline admission
and replay synchronous cadence/forced evaluation -> replay/reveng clock tests and
maintenance benchmarks. There is no new worker/registry or CustomSCP clock change.

Fresh final lead review confirms future refusal precedes store normalization/
mutation, the inclusive upper/lower bounds, non-destructive backward exclusion,
unchanged valid-admission lower pruning, coupled references and actual startup
consumers/capacities. The reveng extraction preserves every resolver setting;
only owner-specific validation precedes construction. Support-critical comments,
config header/key explanations, ADR/TSR/index links and claimed evidence are
consistent with the approved slice. Final map generation/check and direct staged
diff review precede commit. Subsequent slices remain pending.

S2 final maps/check, staged diff and whitespace passed. Checkpoint commit:
`7e43cbec378f71f58c3024c8f486e2bb44f87ec8`. Only the two unrelated YAML values
remained dirty; all approved S2 changes were committed before S3 began.

## S3: accepted-observation learning

Postapproval detailed test-strategy review completed before Go edits. Accepted
refinements: positive WhoSpotsMe controls require291/NA metadata and an isolated
exactly-one record through the same actual pipeline; natural V/C cannot prove
overflow because they bypass holding; compare store-eligible deltas and separately
prove actual delayed promotion to V. Disk expectations include accepted hidden/
refused eligible observations, independently from delivered/archive rows.
Shutdown tests distinguish discard from actual buffered transfer and forced
temporal/FT acceptance, without adding a stop guard or changing ownership.

Implemented one existing-recorder group on one acceptance clock, called before
secondary decisions by immediate, overflow, delayed and non-stabilized paths.
Pending/rehold/suppression returns precede it. Store-specific filters remain;
family lane witnesses/output metrics remain separately owned. The obsolete
main-loop pending predicate is now test-fixture code rather than an unused
production gate. No new retained state, worker, config or protocol is added.

The first targeted run exposed fixture assumptions: no eligible configured SLOW
client lanes, and a disabled legacy S-floor bonus in the later-support probe.
These are repaired in the new fixtures through actual lane configuration/
readback and enabled bonus before control/learning. The failed log remains
`S3-first-targeted.log`. Shutdown and existing shared delivery cases passed in
that run; final targeted and complete S3 checkpoints remain pending.

The subsequent identity fixture also used an empty CTY database that erased
receiver metadata during the production license refresh. The corrected private
database rejects the proposed replacement while retaining the N0 receiver and
K1CTL positive control. Both failed runs remain recorded. Final acceptance
ordinary and combined delivery/family/output race checks passed, including
actual final C/B identity, hidden families, queue refusal, pending/rehold,
discarded shutdown work, buffered transfers and forced temporal/FT releases.

An ignored build overlay restores only the old stabilized output-dependent
learning gate. The exact immediate/all-denied regression passes its positive
control, then fails on the target's missing WhoSpotsMe occurrence as expected.
This falsifier is retained in `S3-old-output-gate-expected-red.log`.

Recording benchmarks passed qualified occurrence/receiver controls: legacy CW
1044ns/16B/2allocs, FT1052ns/16B/2allocs, CustomSCP1204ns/8B/1alloc. These
repeat one receiver with a fixed source timestamp; CustomSCP measures duplicate
recording, not fresh persistent writes. No production capacity claim follows.
Fresh review found no acceptance wiring defect and clarified pre-acceptance
discard wording; the newly unused FT wrapper was removed. The review inherited
context and is not independent. TSR/index and changed-source crawler checks pass.

The complete S3 checkpoint uses the verified approved source in
`S3-scoped-worktree`, preserving the separate dirty5/6 YAML settings outside
the source under test. Canonical executable/lock paths and runner safeguards
remain unchanged. Tagged checks and the full seven-check lane are in progress;
S4 remains read-only until this checkpoint passes and is committed.

Initial complete S3 ordinary passed70 packages in691.8s. Race stopped at the
cluster package after729.4s on the preexisting overflow-admission fixture, not
a race or logging failure. It captured its query clock before NewSpot supplied
the observation timestamp; S2 correctly excludes that later observation at the
earlier query clock. Four related old fixtures now explicitly stamp observations
at their declared query clock. This test-only clock correction retains their
admission assertions. Initial results remain in `S3-scoped-initial/`; repeated
targeted clock checks and the complete seven-check lane rerun on the final state.

Final-state complete S3 checkpoint passed: ordinary599.3s/race1050.3s,
70 packages (53 test/17 build-only), vet4.9s, Staticcheck10.7s, configured
lint16.9s, connected CustomSCP ordinary10.3s/race16.7s. Commands/results are
in `S3-scoped/checks.json`. The four clock fixtures passed200 repetitions each
ordinary/race. Tagged concurrent admission/queue refusal passed count3
ordinary/race and tagged vet/lint. Both logging cases passed in the complete
final ordinary/race runs; no logger repair, exclusion or waiver was applied.

Approved-item mapping: the four mutually exclusive acceptance sites and shared
clock -> `output_pipeline_delivery.go`; store safeguards -> unchanged recorder
admission in `bootstrap.go`/`ft_confidence_runtime.go`; all paths and positive
occurrence controls -> `accepted_learning_paths_test.go`/hold fixtures; final
identity -> identity fixtures; family/lane/queue independence -> output-gate
fixtures and actual TCP/disk readbacks; discard/transfer/forced acceptance ->
shutdown fixtures; duplicate recording cost -> benchmark. ADR-0297/TSR-0090,
prior-decision links, spot README and confidence support card carry the contract.

Fresh final lead verification confirms all pending/rehold/core-suppression
returns precede recording and no accepted path records twice. Source clocks,
CustomSCP V-only, output counters, family lane witnesses, bounded store ownership
and shutdown sequencing remain intact. Reviewed documentation distinguishes
pre-acceptance discard from output refusal. Relative links41, TSR/index and
changed-source crawler checks pass. Source in the isolated checkpoint matches
approved Go files; separate YAML5/6 values remain excluded and preserved.
Final map generation/check, whitespace and direct staged review precede commit.

S3 final maps/check, staged diff and whitespace passed. Checkpoint commit:
`aeb59623395eeb624faf0b36e63e94547157f54f`. Only the unrelated YAML values
remained dirty when S4 began.

## S4: percentage basis and all-zero decoder support

Detailed postapproval test-strategy review completed before S4 Go mutation.
The inherited-context reviewer is not an independent numerical review. The
matrix covers actual zero/default-weight publication, legacy positive versus
explicit unavailable, known/rounded zero and missing ranked evidence,
Compact/projection/loss recovery, neighborhood state/share agreement, mixed
bases and order, decoder present/absent/subject N1/2/10, positive smoothing and
separate transition/prior controls, fresh release authority, family eligibility
and delivered P/0 with store safeguards. No policy or scope gap remains.

Checker refinements accepted: a distinct explicit-unavailable scalar prevents
legacy positive-total inference from reviving mixed evidence; a private lookup
seam exercises complete production neighborhood arbitration using immutable
published rows. Raw fixtures derive genuine zero-weight publications before
explicit declaration; positive and unavailable controls retain actual tallies.
Reverse bucket order and an unavailable/weighted/weighted sequence prove invalid
basis cannot reset. Final aggregate ranks precede classification. The N10 owner
fixture must have adequate capacity and prove no protected evidence loss.

The compact basis scalar reuses snapshot padding, adds no retained registry,
and is excluded from JSON. Real owners publish weighted basis even at total0;
missing reliability still supplies existing unit weights. Basis/helper/classifier
and compaction benchmarks target zero allocation with correctness controls.

Implemented scalar basis publication/projection and consistent percentage,
classification, neighborhood and decoder consumers. Header fallback remains for
rankless legacy snapshots; complete ranks cannot grant absent/zero call evidence,
and empty normalized requests remain unavailable. Diagnostics label known zero
weighted; family comparison still requires positive usable support, including
positive numerators rounded to zero. Existing conflict ratios, positive-weight
loss emissions, thresholds, smoothing, transitions and priors remain unchanged.

Owner ordinary/race checks passed count3, including actual CW/RTTY zero N1/N2/N10,
no loss/default substitution, unit defaults, projection and immutable churn.
The initial partial-expiry fixture swept1ns beyond its claimed positive boundary;
the repaired real batch proves inclusive retention and1ns-later expiry. Its
failed initial log remains `S4-spot-ordinary.log`. Basis/Compact cost checks
reported0 allocations; Compact size312B, with scalar stored in existing padding.
Helper cost checks also reported0 allocations, about110-124ns for guarded
percentage/classifier/Compact paths. These are local costs, not capacity claims.

All shared flow tests passed count3 ordinary/race before final checker refinements;
all replay/reveng tests passed ordinary/race, including real CW/RTTY A2-zero/B1-unit
minimum-report rejection and all-zero uncertain outcomes. Replay retains its
resolver-only glyph/outcome role without a new per-row percentage artifact.
Its deliberately receiver-only metric fixture now declares raw basis explicitly.

Initial cluster checks exposed only new fixture expectations: DIAG retains
input SNR10 (`CW 10 dB 0%`); a terminal abstention requires the existing configured
abstain overflow/fallback action. Corrected fixtures preserve production policy.
The failed `S4-cluster-ordinary.log` is retained. Fresh helper review restored its
preexisting empty-call rejection and added malformed empty-row/header vectors.

The actual-row aggregation checker crosses51 and25 boundaries: A580/B500 plus
A420 produces A1000/1500=66% confident; four A420 neighbors produce81%.
Inherited best-bucket ranks would incorrectly classify38% probable or21% uncertain.
Four ignored mechanism overlays restore old owner denominator, per-call raw
fallback, all-zero decoder fallback, and pre-rank classification respectively.
Each exact qualified checker failed on its intended assertion; expected-red logs
are retained. Final flow/cluster ordinary/race checks are running without overlays.

Final shared-package checks passed all tests count3 ordinary/race, including the
empty-request and actual aggregate-rank cases. Final cluster ordinary/race
selections passed runtime P/0, exact DIAG SNR+percentage, real low-P budgets,
temporal apply/abstain, eligible learning controls, family guards and preexisting
delivery/shutdown regressions. Family cost checks reported0 allocations for
positive, known-zero, rounded-zero and deliberately raw paths. The exact current
selection path also measured0 allocations (313.9ns in its actual A2/B1 control).
The private seam retains the prior linear bounded arbitration in one function;
splitting policy steps further would obscure their ordering and ownership.

Tagged family concurrent admission/queue refusal passed count3 ordinary/race;
whole-module tagged vet/lint passed. The complete seven-check lane is running in
`S4-scoped-worktree`, with approved Go source verified against the main workspace
and only the unrelated YAML5/6 edits excluded. Canonical executable/lock paths,
native-target guards, direct count1/paniconexit0 and stale-build refusal remain.
There is no tracked workflow change or firewall action. TSR/index/crawler checks
pass; ADR-0298/TSR-0091, prior decisions, operator domain/spot docs and the
confidence support card record the basis contract. Domain ASCII insertion preserves
its existing byte encoding. Fresh final scope/quality verification remains in progress.

Complete S4 production-state ordinary706.7s/race1098.2s passed70 packages
(53 test/17 build-only), followed by vet5.1s and Staticcheck8.7s. Configured
lint found QF1003 in the new delivery fixture's destination if/else chain.
The equivalent tagged switch changes only fixture syntax; affected actual
delivery cases passed ordinary/race again. Broader results remain valid under
the review-checklist invalidation rule because production, inputs and oracles
are unchanged. Initial lint and pre-fix checks remain in `S4-scoped/`; final
lint7.5s and connected CustomSCP ordinary10.2s/race21.0s passed. The aggregate
seven-command evidence and explicit fixture correction are in `checks.json`
and this record. No failure was excluded or waived. Both logging cases passed
within complete ordinary/race runs, with no logging repair.

Approved-item mapping: basis and genuine totals -> `spot/signal_resolver.go`,
`resolver_evidence.go`/`resolver_loss.go`; freshness/copies -> `resolver_fresh.go`
and Compact; eligible percentages, final-rank classification, mixed union and
neutral support -> shared correctionflow helpers/decoder. Spot basis/churn,
flow golden/union/temporal tests prove the contracts; runtime delivery/family
and real replay/reveng tests prove consumer consequences. Existing B32 zero
and deliberately raw fixtures retain distinct meanings. Diagnostics/family
guards and benchmark fixtures cover label, hiding and allocation boundaries.
ADR-0298/TSR-0091 and operator/support docs carry the selected method contract.

Fresh final lead verification and inherited-context cross-package quality review
found no material defect or scope expansion. The latter authored spot tests and
is not independent. Actual source confirms scalar ownership without new retained
state or lifecycle changes, JSON exclusion, immutable publication/copies,
absorbing unavailable basis, rank ordering/eligibility, and preserved positive
smoothing, loss emission, transitions, priors and ambiguity rails. Final source
matches the isolated source under test; YAML5/6 edits remain preserved outside
the checkpoint. Relative document links57, TSR/index and changed-source crawler
checks pass. Final map generation/check, whitespace and staged review precede
the `fix(confidence): preserve resolver support basis` checkpoint commit; its
exact hash is recorded locally in `S4-scoped/checkpoint.json` and final handoff.

All four approved slices are implemented and verified sequentially. Evidence
establishes these local contracts; field accuracy, live-feed behavior, production
load, Linux parity and broader firewall permission remain outside these checks.

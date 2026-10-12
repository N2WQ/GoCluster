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

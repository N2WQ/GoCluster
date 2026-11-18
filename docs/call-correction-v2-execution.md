# Call Correction v2 Execution

Authority: [approved Scope Ledger v3](call-correction-v2-scope-ledger-v3.md).
Owner authorization: exact `Approved v3`, 2026-11-17.
Study-input amendment: [approved Scope Ledger v4](call-correction-v2-scope-ledger-v4.md),
exact `Approved v4`, 2026-11-17; v3 runtime and final validation boundaries remain.
Branch: `call-correctionv2`; baseline `3fd37c6cb48d2028756ca18184d4f18a8dc95bb0`.
Active execution worktree: `C:\src\gocluster-call-correctionv2`.

The owner confirmed another task was using the original checkout after it
switched to `meta-bands` at 11:07. Work was isolated without changing that
task's band/filter edits. The S3 commit initially landed on that shared branch;
the requested branch was advanced to the same commit with a guarded fast-forward.
Subsequent correction work uses only the isolated worktree. Frozen data inputs
were copied and hash-verified; local index flags preserve those hydrated inputs
without including them in source commits. No model installation was performed.

## Evidence status

Latest checkpoint: final selected-state ordinary tests, vet, Staticcheck, lint
and connected ordinary/race CustomSCP regressions pass. The actual Bayes-off
release capture completes all 90,000 inputs and deliveries but **fails** the
frozen typical CPU gate at 437,500 ns/input. Complete race package coverage
passes after a documented test-only clock repair and affected continuation.
This is not a completed or production-qualified delivery. See the
[qualification report](call-correction-v2-qualification.md) for the preserved
failed gate, diagnostic comparison and remaining obligations; earlier entries
below retain their chronological state boundaries.

Before implementation, separate scientific reviews and an independent scope
adversary challenged the original and materially revised designs. The final v3
challenge had no unresolved material scope blocker. The postapproval detailed
contract-to-test review was completed and its checker refinements adopted before
S0. Reviews are evidence plans, not implementation/validation passes.

The postapproval matrix covers legacy report compatibility, row/SNR binding,
reference isolation and embargo, frozen candidate/provenance, full paired outcome
denominators, per-cell activation boundaries, actual study CLI refusals, independent
parser grammar, harmonic presence, admitted-neighbor selection, receiver SNR joins,
top-cohort-only scoring, immutable/fresh publication, canonical union/display/
lifecycle/temporal delivery, decision-local history capture, cell-map/capacity
consumers, actual Bayesian reachability, preserved temporal objective and paced
asynchronous resource qualification. Exact checker substitutions must preserve
these failure mechanisms and be recorded with executed results.

## Unit accounting

| Unit | Implementation | Validation |
| --- | --- | --- |
| S0 evaluation foundation/provenance | `cmd/rbn_replay/study_*` and `cmd/callcorr_reveng_rebuilt/evaluation_contract.go`: legacy numeric compatibility, frozen provenance, paired episodes/unknowns and missed opportunities | Actual 36 tuning arms/six whole candidates, six held-out arms, two analysis engines, exact parity and separate 32-check held-out stream audit; all activation floors retained |
| S0b conditional study geography | `cmd/rbn_replay/study_geography.go`: schema-2 frozen CTY/H3 scenario and complete focal-row coverage | Actual CSV-to-history plus unknown/same-cell controls; study 163 ordinary/163 race cases after analysis repair, detached compatible comparator checks, exact cross-arm covariate bindings |
| S1a legacy report parsing | `spot/comment_parser.go`: removed substring fallback, retained whole-integer grammar and separate human rules | Parser/display/RBN consumers, literal grammar, 422,237 guided grammar and 95,100 guided legacy-parity fuzz executions; earlier unguided runs remain separately labeled |
| S1b harmonic presence | `spot/harmonics.go`: missing reports neither compare nor seed; measured cross-receiver/mode policy retained | 33 presence/boundary/maintenance/native-delivery cases, including present zero, actual TCP and reopened archive |
| S2 neighborhood admission | `internal/correctionflow/shared.go`: only actually admitted adjacent populations set neighborhood use | Exact distance-2/3 immediate/temporal/stabilizer witnesses and negative dissent/loss/nomination controls; four mechanism traces preserve full official decision SHA |
| S3 receiver SNR | `spot/resolver_evidence.go`, `resolver_confusion.go`, `resolver_fresh.go`, `signal_resolver.go`: bounded receiver joins and fresh optional ordering | Permutations/conflicts/expiry/immutable readers, direct-versus-binned scoring, modeled capped benchmarks, exact whole tuning-stream parity and passed matched resource gate |
| S4 canonical slash authority | `spot/resolver_canonical.go`, flow and native family/display consumers: receiver unions, semantic state, observed display and independent slash threshold | Native licensed delivery, lifecycle/race checks, exhaustive binary64 witnesses, 128 map-derived merge cases, large/overlap/ragged population oracles and shape benchmarks; no hidden merged-64 recap |
| S5 per-decision history | `spot/correction_history.go` and `correction.go`: eight first-read alias entries, fresh next gate | Changing-store/alias-max/zero/requirement and real backend consumer checks; independent source review and nine paired benchmark shapes |
| S6 Bayesian retention/study/activation | `spot/correction.go`, `config/call_correction_bayes.go`, capacity validators and live/replay/evaluator mapping: independent capped rails and complete masks | Separate arithmetic oracle and real consumer reachability; omission/empty/null/merge/capacity checks; untouched zero-contradiction/sample/coverage failure selects implemented whole-off fallback, with no retuning |
| S7 final integration/resource/docs/lane | Native paced/integration fixtures, config/spot/replay/support docs, ADR-0358 and TSR-0137/0138 | One matched capture passed all four controls; ordinary/static/connected checks and complete race package coverage pass; actual release capture has zero loss but fails CPU, so resource qualification remains unmet; final supporting receipts are below |

## Baseline and study artifacts

Local ignored evidence belongs under `.tmp/call-correctionv2/`; runtime assets and
production data remain at their original paths. Baseline source/config/assets,
workload, and tool fingerprints must be recorded before dependent changes.
Baseline replay executable was built from the unchanged approved production source
before S0 edits. The paced fixture must remain baseline-compatible and its baseline
capture must precede S1-S6 production changes.

The initial ignored `baseline/manifest.json` records 20 copied runtime YAML files,
10 frozen data inputs (136,185,542 bytes), Go 1.27.1 windows/amd64 and the shipped
replay executable hash. The configured local FCC exception-list file was absent;
isolated arms use an explicitly empty list, identically, with that setup difference
recorded. FCC/ISED enforcement and actual license database snapshots are retained.

Executed S0 compatibility check:

```powershell
& .\scripts\test-windows.ps1 -Packages @('./cmd/callcorr_reveng_rebuilt') -Run '^TestCCV2EvaluationCompatibility$' -RequireCases @('dxcluster/cmd/callcorr_reveng_rebuilt::TestCCV2EvaluationCompatibility') -Timeout 180s
```

Observed: one package, 13 passed cases, zero skipped cases, exit 0. Literal cases
cover correct/wrong/missing/extra predictions, duplicated/conflicting labels,
loaded labels without replay rows, zero denominators and sample-size variation.
All legacy numeric formulas and ordered pairing remain unchanged. New population
fields remain invariant when only reservoir sample size changes; undefined RF and
label-event coverage values are explicitly null. This is reporting integrity,
not an RF or Bayesian improvement result.

Baseline owner/selection/freshness benchmark logs and stable native binaries are
fingerprinted in the same ignored manifest. Three 300 ms samples were captured per
case on Go 1.27.1/windows-amd64. Typical owner evaluation was 21.3-25.2 us with
4 allocations; the existing tied-confusion case was 2.93-3.28 us with 7 allocations.
All-fresh lookup retained zero allocations; dirty capped publication was
286-307 us with 73 allocations. These are local component baselines, not production
latency or full capacity. The first direct-binary invocation misparsed unquoted
PowerShell dotted flags and failed before executing benchmarks; quoted complete
native arguments corrected that invocation. No failed run is counted as evidence.

The baseline paced native fixture passed all three required subtests (330.77 s,
zero skips). Typical and recovery each offered/accepted/processed 15,000 inputs;
dense offered/accepted/processed 60,000. No input, resolver, retained-evidence or
telnet capacity loss was observed; every owner joined after natural drain, and
reopened archive rows equaled ring handoffs. Steady CPU/input was 415,365 ns
typical, 636,719 ns dense and 386,719 ns recovery. Actual steady rates were
199.0/s, 780.4/s and 199.9/s. The dense case includes a long natural hold drain;
configured duration is the offered-slot budget, not an asserted wall duration.
The frozen executable, full transcript, CPU/heap profiles and JSON artifact are
under `baseline/`; this establishes comparison data, not the candidate's <=10%
gate or production capacity. Profiled candidate runs must use the same fixture,
workload, settings, assets and observation path.

The official 20261115 and 20261116 ZIPs have been downloaded and SHA256 recorded
without opening their contents. The reference geometry is fixed separately from
the not-yet-selected candidate: A/B family cohorts, 100 Hz anchor radius, 500 Hz
competing-track guard, three unanimous families, R1 [-30,0] seconds, R2 (80,195]
seconds, a 75-second configured decision cutoff and 300-second local warmup.
Each day starts with empty A-only historical state; this is not full historical
warmth or production steady-state parity. An actual terminal decision beyond the
cutoff makes its later reference unknown. Final holdout contents remain sealed
until one complete candidate and activation map are frozen.

The RBN-only reference study uses a fixed predictor receiver cohort A and withheld
cohort B. Every paired model arm sees A-only input/history/learning. References
are separately constructed from B observations and strictly later reports, with
focal aliases excluded. Qualification is expressly for this reduced-input archive
population. Full-input shadow/stability/capacity evidence does not inherit an
independent-reference accuracy claim.

Native `TestCCV2Study*` passed 33 cases with zero skips, including raw row/SNR
binding, family cohort isolation, exact reference cutoffs, unknown/conflicting
labels, frozen nonchaining episodes, embargo/configuration freeze, whole-cell
qualification, collateral harm and actual-command refusals. A first run caught
a malformed spillover test fixture with different strong references between
paired arms; fixing the fixture preserved the strict pairing check, and the
complete selected group passed on rerun. These are hand-counted tool contract
checks and a tiny real-runner integration, not held-out empirical qualification.

Independent S0 review subsequently found receipt-shift inflation, incomplete
off/on policy/binary provenance, non-executable candidate selection and scalar
normalization drift. All were repaired and independently rechecked. Final
`TestCCV2Study*` passed 112 native cases, zero skips, in a detached worktree at
`4395eb31` with only the reviewed study-tool repairs applied; original production
correction source remains unchanged there. It also rejects exposure on the
holdout UTC date even within warmup. Per-row strong contradictions survive
episode deduplication and can block the complete stateful combination outside
the directly selected cells. A rebuilt hook-enabled shipped binary is frozen
with its exact bytes, source revision and dirty tool-file hashes; its source
revision alone is not claimed to identify those instrumented bytes.

No new model has been qualified; no production accuracy, capacity, deployment,
hosted CI, final-lane pass or task completion is claimed by this record.

S1a's frozen legacy parser retains its original substring regex in test-only
code, with an observation of fallback use. Differential checks exclude only the
intentionally retired report/presence result and still compare all other fields.
Separate literal grammar vectors and integer/malformed-token fuzzing establish
the changed contract. The first RBN consumer fixture omitted mandatory CQ class
and expected zero reports to be delivered; correcting that fixture preserved the
existing source policy (missing/zero dropped). The final selected group passed
139 cases without skips. Prepared-binary fuzzing executed 2,716,179 grammar and
2,376,232 differential inputs without failure; the tool explicitly reported no
coverage instrumentation, so these are unguided fuzz results.

S3 adds 8 logical bytes per retained owner row (40 to 48 bytes) and public row
(64 to 72 bytes), measured with native `unsafe.Sizeof`. At 16x64 incidences this
adds 8192 logical bytes to each full row population; map allocation classes,
slice backing and overlapping immutable generations require profile accounting.
Reusable owner SNR-bin scratch is bounded by admitted tied-candidate count times
the immutable model bin count; fresh readers use local scratch. There is no
second receiver registry or lifetime pair cache. All-fresh reads remain zero
allocation. Partial projection additionally derives canonical evidence; its total
cost must be measured rather than inferred from the model branch alone.
The independent S3 source review found no remaining production correctness issue
and identified the missing modeled capped benchmark; that case was added using
the actual checked-in model. Native race checks include simultaneous modeled
partial projections. Bench artifacts are component evidence; final quiet paced
comparison and full lanes remain required, and intermediate CPU timings are not
the release performance verdict.

S6 preserves the capped log-support/history arithmetic and evaluates the report
and advantage bonuses independently. The latter explicitly inherits the former
history/contested-neighbor/candidate qualifications; distance-2 subject validation,
weighted delta, extra confidence and all final ordinary gates still apply. It
does not stack another report on recent +1. A separate non-steered scientific
review derived literal score ranges, partial-history vectors and conditional
consumer witnesses before root implementation tests. The current old thresholds
remain bootable; their distance-2 report and both advantage cutoffs exceed the
corresponding score maxima (470 and 560/470).

Native tests demonstrate conditional reachability of report and full-report-tie
advantage rails through real admitted receiver rows, V-only Custom SCP history,
two real H3 cells, CTY/licensing and immediate/temporal/stabilizer TCP/peer/archive
paths. Master-on empty masks and bare distance-1 contested ties are negative
controls. Replay has separate immediate/temporal positive/off/empty controls.
These constructed histories establish wiring and arithmetic, not the availability
of those inputs in real archive replay or their RF correctness. Development runs
corrected fixture source-path ownership, canonical historical query spelling and
the existing confidence/percentage oracle; production behavior was not changed to
make those fixtures pass.

The optional cell map preserves nil legacy eligibility for CW/RTTY/USB/LSB,
explicit empty/no-distances semantics, raw-null rejection and normalized
duplicate/domain bounds. Only this complete mask replaces its previous value
across merged runtime files; all other maps retain deep merge. Settings and all
diagnostic/capacity consumers share one config-owned structural predicate.
Unreachable thresholds and zero weights do not erase actual history demand.
Independent code review found no remaining material gate/config issue.

Six complete tuning vectors/maps were predeclared before official-archive
evaluation at `.tmp/call-correctionv2/tuning-predeclaration-v1.json`, SHA256
`e6aa0e9a916390654087c9f9af539c0d2825106ee20f7d7f145731bb61bcdf41`.
The two final holdout ZIP contents remain sealed. A full-day source-readiness
review then found that ordinary CSV replay supplies no DE grid to its empty
Custom SCP history, so positive geographic requirements cannot qualify. A
header-only inspection of the tuning archive confirmed no receiver coordinates.
Frozen CTY supplies representative prefix coordinates, not verified station
locations. At that point, the input assumption remained unresolved and blocked a
study verdict or Bayesian activation. The later S0b amendment below records its
conditional disposition; this earlier readiness finding and the six-vector
predeclaration remain historical evidence. No history threshold/backend was
relaxed.

S4 derives canonical groups from the admitted raw rows without another lifetime
registry. One key retains at most 16 x 64 incidences and a five-key neighborhood
at most 5120; receiver overlap is deduplicated for votes and dissent. Raw caps and
loss protection remain authoritative. Snapshot winners and delivered spellings
are observed displays; transitions, support and correction accounting use full
semantic identity. The separate slash minimum remains two; ordinary gates and
bare dissent remain in force. Native delivery covers immediate, temporal and
stabilized paths, current display after expiry, license checks, TCP/DIAG,
original peer, ring and reopened archive. Replay checks its actual emitted
fields; replay does not populate the live confidence-percentage field.

Independent source review found and closed a borrowed-slice mutation in merged
frequency evidence. Cross-bucket witnesses retain their original bucket scope;
equal-time conflicts within a bucket remain conflicts. Concurrent readers and
literal exhaustive binary64 frequency oracles exercise actual 512-row canonical
groups and five-view 2560-row groups, including strict boundary and noninitial
interior minima. A final scratch refinement copies only frequency values rather
than complete receiver rows. Three local benchmark samples measured 57.5-60.4 us,
about 41.4 KiB and 12 allocations for the largest pair, versus 335-601 us and
about 976 KiB before that refinement. Frequency-value backing across a distinct
candidate pair is bounded by 5120 values (40 KiB), plus normal runtime overhead.
The ordinary 3/64-row cases retain zero allocations. These are component results,
not the final paced resource gate.

The broad five-package S4 run passed spot and flow, but identified legacy cluster
fixtures that expected candidate-wide SNR clearing and literal-alias-only family
percentages. Those expectations contradicted the approved receiver-specific and
canonical contracts. Fixtures now assert the surviving receiver measurements,
absorbing equal-time conflicts, canonical percentage and independently admitted
literal display lanes; 116 affected cases passed. The two replay/evaluator
packages separately passed 541 cases with one intentional subprocess-helper skip.
The failed broad cluster run is not counted as a full-package pass. Final ordinary
and race lanes on the completed relevant state remain mandatory.

S5 uses synchronous scratch for the two immutable gate identities and their four
existing aliases each. It caches zero and partial counts, takes each identity's
maximum, and preserves the original normalized query methods and consumer
requirements. It makes no atomicity claim across aliases, stores or publications.
Changing-store tests prove first-read consistency and fresh next-call evidence;
the existing concrete Custom SCP and legacy consumer tests remain passing.
Independent source review found no remaining material issue. Paired three-sample
native benchmarks retained every no-history allocation count and introduced no
lock: bare no-history remains 0 B/0 allocations, although its local median rose
from 291 to 402 ns. Custom-history bare/slash/shared-base medians fell from
4217/8413/3341 to 1854/5112/2416 ns with unchanged allocation counts. Legacy
repeated-query cases also reduced allocations. Component logs preserve all nine
shapes and do not replace the final paced CPU/input comparison.

## S0b study-input amendment

The owner selected frozen CTY representative receiver locations and authorized
the additive v4 scope with exact `Approved v4`. Separate scientific review and
scope/test challenge preceded that approval. CTY input uncertainty is explicitly
distinct from the earlier accepted RBN reference-label uncertainty. This is a
conditional input scenario, not reconstructed historical station geography,
reported receiver grids, live enrichment timing, site independence or RF truth.

`study_geography.go` implements the selected `cty_representative_v1` hook from the
already frozen CTY image, using live receiver normalization and portable CTY
precedence. Known four-character grids carry derived provenance. Missing matches,
invalid coordinates and empty receivers remain unknown; valid zero coordinates
are accepted. The hook runs only on focal-A inputs before activity/history/
learning and adds no receiver registry. Withheld B input, raw row hashes, episode
IDs, R1 and R2Frozen remain unchanged. Terminal R2 retains its existing decision
cutoff. Ordinary no-study replay/server behavior and history requirements remain.

Version-2 plans/reports/comparisons require an explicit `input_scenario` binding
CTY, H3 res1 and H3 res2 digest roles to every arm's actual configured paths and
manifest. Run-owned copies are verified and loaded before raw archive parsing.
Old/missing schema, scenario/asset/role drift, corrupt/missing setup inputs and
an empty CTY image fail setup rather than becoming row-level unknowns. Failed
runs cannot publish a qualifying completion report.

Each terminal focal-A decision carries receiver-location lookup, matched prefix,
derived grid and known/unknown reason. Complete coverage counts every focal-A
row, including warmup, suppressed and license-rejected outcomes; known plus
unknown reconciles with focal and terminal totals. Comparisons reconcile those
counts against decision streams and reject changed paired input covariates.
Unknown input locations stay in the population rather than being filtered out.

The captured native receipt
`.tmp/call-correctionv2/s0b-study-followup-all.txt` reports one package, 158 passed
cases, zero skips, ordinary/non-race execution. It covers literal CTY points,
portable precedence, known zero/invalid coordinates, real CSV-to-resolver-V-to-
CustomSCP learning without seeded DE grids/history, two-cell versus same-cell/
unknown controls, ordinary no-study behavior, all-terminal-status/warmup coverage,
unchanged frozen references, and actual-command schema/scenario refusals. The
actual mapped four-receiver/two-cell consumer qualifies only the constructed
two-cell CSV control; these are tool contract tests, not official archive results.

The initial `.tmp/call-correctionv2/s0b-study-targeted.txt` run timed out after six
minutes in the new CSV integration fixture. That fixture called row processing
directly with an uninitialized sample clock, causing minute-by-minute catch-up
from zero time. The revised fixture uses ordinary `replayDay`, which initializes
the clock and services scheduled/EOF work. The separate CSV follow-up passed five
cases, then the complete 158-case follow-up passed. The failed receipt remains
preserved and is not counted as a pass or a production performance finding.

Prior manifests, binaries, schema-1 artifacts and the original six predeclared
candidate vectors are retained. Schema-2 study checks also passed 158 native race
cases; the detached original-core comparator passed 156 compatible ordinary
cases, all with zero skips. The original production correction code and pre-mask
instrumentation seam remain in that comparator.

Schema-2 tuning controls were frozen at 2026-11-17T19:26:55Z, before official
evaluation, in `.tmp/call-correctionv2/tuning-plan-v2.json`, SHA256
`8cdba5fde284f8bf7ad34ede758afc9bcd3856bbf99a44c9321376f868864c42`.
The shipped executable SHA256 is
`c2f827138c87d14754d93abe835a1a13a059e6cbebbe063125e06a69ee63c80b`;
the identical repaired OFF/ON executable SHA256 is
`3088e921d31d8dcc51b39633f79e3b23a3e80b7e910b129322701d2a4611c679`.
Their source revisions are respectively `4395eb31` and `00061b22`; the separate
freeze manifest records actual executable bytes, dirty-file hashes and full Go
build information. A revision alone does not identify an instrumented build.
All arms use the same owned 20-file configuration and ten hashed assets.

Official tuning is underway. No complete empirical verdict, final resource
qualification, activation, runtime asset installation, deployment or push is
claimed here. Both final holdout contents remain sealed. A separate non-steered
review confirms that CSV replay retains measured zero reports while full live
RBN input discards them; this is another declared archive/live population limit,
alongside A-only input, empty daily history and representative CTY geography.

## S7 evidence checkpoint at 669dccfd

The durable [qualification draft](call-correction-v2-qualification.md) is
**in progress**. Runtime prediction remains frozen at `00061b22`; later changes
repair descriptive inactive-cell support-net analysis plus comments/tests, not
runtime correction predicates. The separate analysis freeze is
`analysis-freeze-v2.json`, SHA256
`3e5fb816237afee15c404e5346625200d40a0b12d650790e742e488ab69eb814`;
analysis executable SHA256 is
`e1fd712c86c4dd1a3a32ef7aa12aa156a5c17120da2dd156732eaab6c9f4c75e`.
Original comparisons remain preserved. Corrected comparison proofs require
identical active-cell/gate statistics and paired-outcome SHA256; only inactive
support-net fields and output paths may differ.

At this checkpoint three complete CW/2 configurations each show 146 incremental
episode IDs, three jointly strong, 8.219% R1 and 15.753% R2 coverage, and no new
strong contradiction. These observations fail the strict evidence floors. The
six-candidate study is incomplete; no final candidate/policy is selected and
final holdout contents remain unopened. A separate non-steered empirical oracle
recounted the first candidate's six streams and passed 19 numerical checks while
confirming the floor failure.

An independent repair adversary found 2,244 changed final/status receipts across
1,340,516 focal-A IDs. Among 74 changes with both references agreeing, 58 new
agreements span 53 episode IDs and 16 new disagreements span 11. Four full-day
mechanism traces retain exact original decision-stream SHA256 and unchanged
snapshot-read metrics. They expose one supported recovery and three regressions
from removal of the old pre-admission neighborhood flag's accidental restriction;
no new coding defect was identified. Counts, receipt/episode distinctions and
source-specific limits are retained in the draft, without calling artifact tags
RF errors or reclassifying unknown/conflicting references.

Native `s7-race-full.txt` reports 14,101 passes, 99 skips, 76 packages with 61
executed and 15 build-only; the skip-name audit matches the earlier ordinary
14,057-pass/99-skip lane exactly. Latest study checks after the metadata repair
passed 163 ordinary and 163 race cases with zero skips. Connected CustomSCP
ordinary/race handoffs passed. Guided fuzz receipts record 422,237 grammar and
95,100 legacy-parity executions; earlier unguided receipts are separate history.
Repository vet/Staticcheck/lint cleanup passed before the metadata repair, with
focused replay vet/lint afterward. This is not a completed final selected-state
lane or a performance verdict.

Source review additionally confirms that resolver evidence takes its supplied
`now`: live initial output supplies admission/evaluation UTC while replay supplies
the archive row time. Same-second archive conflicts need not reproduce live
clock ties. The frozen rule remains, and claims remain conditional alongside the
CTY-derived geography and measured-zero population differences.

The matched performance worktree is prepared at `669dccfd`; the actual Go config
probe verifies identical legacy settings with only the new omitted/null mask type
field added. Shared final fixture bytes match each other, not necessarily older
raw baseline hashes with their line-ending/style differences. The preregistered
`paced-final-protocol.json` requires both matched-setting and actual final-policy
candidate runs to meet the <=1.10 typical CPU/input gate against each original,
quiet-repeat and forthcoming fresh baseline, without added loss/backlog and with
profiles. No candidate measurements exist at this checkpoint. Final resource,
candidate/policy/holdout and complete final selected-state validation remain
pending.

## S7 resource failure and local merge repair

The final-fixture original-core control passed all three paced cases, with
371,093.75 ns per accepted typical input, no input refusals or archive loss,
natural drains and joined owners. This adds a tighter denominator to the two
earlier controls; the preregistered gate still requires each candidate to pass
against every control.

The first matched-settings candidate at `669dccfd` **failed**. Typical cost was
432,291.67 ns/input, above 1.10 times that fresh control. Dense input refused
13,214 of 60,000 offered receipts, filled the 20,000-entry output queue and did
not naturally drain within 90 seconds. Its final forced shutdown did eventually
join; that does not turn the natural-drain failure into a pass. The captured
executable, transcript, result and profiles remain in the performance worktree's
`paced-final-matched-current-fixture/`. No holdout content was opened.

The whole invocation sampled 279.41 CPU seconds and 74,858 MB of cumulative
allocation churn, not live heap. Canonical rebuilding accounted for 57.83% of
allocation bytes and preliminary neighborhood receiver maps for 36.74%.
Cumulative CPU figures overlap: merging included 89.71 seconds and canonical
construction included 66.52 seconds, so they are not additive costs.

The local repair remains inside S3/S4/S7 and preserves selection semantics:

- Delay aggregate derivation until after the exact-only return. Capture only
  source nominee/best-snapshot headers, including every canonical member of
  an admitted settled source, rather than building and discarding row maps.
- Merge already sorted per-publication canonical rows. Keep raw incidence
  counts/provenance, maximum weights, newest/equal-time SNR joins and all original
  source-bucket frequency witnesses. Recompute display support from distinct raw
  spelling receivers at their global newest timestamps; locally superseded
  spellings cannot become eligible in a larger union.
- Read sorted memberships for overlap and population totals. Small cohorts use
  head scans; large cohorts use one local cursor heap, batching repeated receiver
  keys. This avoids both per-receiver set maps and quadratic disjoint-head scans.
  No registry, retained cache, ingest barrier, new owner or lock is added.

A separate source/shape reviewer independently challenged the candidate and
identified the need to measure 80 distinct identities, not only two large alias
groups. That check found the draft linear reduction's 3.84-4.02 ms disjoint cost;
the first heap draft instead penalized complete overlap. The final batched
cursor design measures 234-270 us for 80 disjoint groups and 104-124 us for
complete overlap, with about 2.7 KB/one allocation in each shape. The corresponding
map reference measures 672-674 us/436 KB/46 allocations and 123-132 us/6.7 KB/nine
allocations. These are local component observations, not a passed paced gate.

The three-source overlapping merge fell from 1.75-1.86 ms/677 KB/498 allocations
to 326-381 us/92.8 KB/74 allocations. The five-source disjoint merge fell from
6.13-6.31 ms/2.70 MB/756 allocations to 1.61-2.01 ms/1.09 MB/119 allocations.
The same merge-only fixture builds against both production revisions; changing
normalization/variant work explains the later allocation-count improvement.

The independent map construction checks complete rows, identities, display and
variant headers, raw counts, source frequency rows, overall weights/conflicts
and all candidate-pair intersections. It includes 128 deterministic mixed cases,
global alias supersession with an older retained frequency witness, 80-group
disjoint/overlap controls and ragged partial overlap with a conflict on a
lower-weight membership. Targeted ordinary/race and full consumer qualification
are recorded separately as they complete. Frozen tuning predictions/selection
remain preserved; untouched confirmation requires a fresh executable freeze and
actual tuning-stream parity before any holdout inspection.

The selected five-package correction-consumer race group subsequently passed
633 cases with zero skips, and settled vet/Staticcheck/lint passed. A broader
five-package race command hit its six-minute package limit in the unchanged
native process-launcher tests; that failed receipt is preserved rather than
counted as a full pass. Final full lanes use the appropriate larger package
timeout. The exact-only selection benchmark, with neighborhood checking enabled,
measures 939-968 ns with 0 B/0 allocations.

The next matched capture accepted/processed all 90,000 raw inputs with zero
recorded loss. Typical CPU was 191,406.25 ns/input; dense CPU was 979,166.67,
52.1% above the fresh original dense control. Dense input-stop temporal backlog
was 5,976 and naturally drained in 15.35 seconds. Every owner and TCP barrier
joined, and all 90,003 archive rows including barriers reopened. Nevertheless,
`matched-sorted-v1/` remains **failed**: its named recovery case had no observable
backlog at input stop, violating that fixture's case-specific coverage assertion.
These measurements are not reclassified as a passed invocation.

A separate non-steered test-strategy adversary inspected the contract, fixed
fixtures and actual captures. It classified the narrower named-case obligation
as a checker-only refinement: S7 requires recovery from actual backlog, which
the dense case exercises, not backlog in one particular case name. The refined
checker requires at least one fixed case with actual input-stop backlog,
successful natural completion, all owner/TCP joins, no residual backlog/errors
and complete archive readback. Sampled high-water values or forced retirement
cannot supply coverage. Every case's existing loss/completion checks remains.
Thirteen ordinary and thirteen race checker vectors passed with zero skips.

`paced-coverage-addendum-v2.json` records this post-capture finding before new
measurements, binding five byte-identical shared fixture files across original,
matched and release worktrees. Workloads, rates, seed, assets, caps, warm/steady
windows and barriers are unchanged. New identical-fixture original/candidate
captures are required; every earlier CPU denominator is retained. Boundary
comparison additionally reconciles each fixed case's raw acceptance with
resolver processing, finalized fanout, ring, both TCP clients and reopened
archive, rather than accepting archive-versus-ring equality alone.

The revised runtime prediction executable remains frozen at `cf217f2f`, SHA256
`78977742e88c6d9398bde91745f15f98670653a29515fda9963844c7f8d1274e`.
Its four actual selected-candidate tuning replays reproduce complete OFF/ON
decision-stream SHA256 on both dates, including all labels and input/reference
coverage. `study-v3-parity/equivalence-proof.json`, SHA256
`6eff5553e992715eb1f866730b270d260aa0a72d86b6f4a1e68971ecf3055efe`,
binds the separate executable/plan freeze. No tuning artifact or selected vector
was overwritten. Final holdout contents remain unopened at this checkpoint.

## S7 untouched result and release policy

The identical-coverage-fixture original control and matched candidate passed.
The lead's separate artifact comparator requires all per-case raw/resolver/
finalized/ring/TCP/archive counts, actual backlog recovery coverage, zero loss
and <=1.10 typical CPU/input against every retained control. The matched result
is 203,125 ns/input, 52/55 of the newest original control's 214,843.75; all four
comparisons pass. Dense cost is 990,234.375 versus 578,776.042 ns/input, a real
71.1% increase, with zero refusal/loss and natural completion in 15.37 seconds.
Every 90,000-input invocation reconciles 90,000 TCP receipts per client and
90,003 reopened archive rows including controls. Whole-invocation allocation
churn is higher than the original, as the qualification record discloses.

The held-out plan was frozen at 2026-11-17T23:17:46Z, SHA256
`ca63715c9e509e5d2f7215bb279f02aafae8ab1ba4005ebe5a4e6c36292a9932`.
The first recorded inspection is 23:17:47.410554Z. Freeze SHA256 is
`c25196aba81cb1eb1ff4f963f107ace1ee76106c7fef4ccb39b159bf500e55bf`.
One complete CW/2 candidate, both actual prediction binaries, all config/assets,
references and CTY input rules remain fixed; no same-holdout adaptation occurred.

All six held-out arms and two comparison engines completed. A separate empirical
reviewer recounted all six terminal streams before reading any generated
aggregate. It derived 79 additional normalized episode IDs, two jointly strong,
R1 coverage 9/79 (11.392405%) and R2 coverage 22/79 (27.848101%). November 15 has
61/1/7/15 incremental/joint/R1-covered/R2-covered and November 16 has 18/1/2/7.
Positive supported nets are +9/+15 and +2/+8. New contradiction flags are R1=1,
R2=3 on November 15 and zero on November 16; the flags are distinct episode,
mode and configured-distance units. Here they also equal the affected rename
receipt counts. No inactive-cell spillover or unpaired raw ID was found. All
1,024,414 focal-A IDs and 6,043 strong-reference disagreements remain accounted.

The fixed verdict is **whole Bayes off**: zero-contradiction, sample/date and
coverage floors fail. Positive support does not waive any failed floor.
The lead directly checked the four actual OFF/ON rows; one target already
appeared elsewhere in OFF's episode and correctly adds no new normalized gain,
while its changed-row contradiction is retained. These are RBN-reference
disagreements, not independently adjudicated RF errors.

`study-v2-holdout/comparison-corrected/comparison.json` SHA256 is
`6d4142ebc98439cdfd7cef305c30ffd6e05ab0d73d2cb08eb99ce8e775503d7c`;
the paired stream SHA256 is
`a3fb7eafd5e41e4b3c3f68647c3997d61e0c26131bf65f225f860b7476d9a3c6`.
The two engines report identical activation statistics/verdict. Independent
aggregate reconciliation follows the recorded raw recount.

The shipped `pipeline.yaml` therefore uses master false and an explicit empty
cell mask. Ordinary gates, confusion ranking and recent +1 stay active; the
retained bonus is implemented but inactive. Operators must apply both off fields
to their active/private config directory. Compatibility scalar values remain
bootable, not qualified calibration. Config tests passed 970 cases without
skips. The first config run found two legacy omitted-mask capacity fixtures
inheriting the new explicit empty sample mask; only that owned fixture now
removes the seeded mask to exercise its stated legacy policy. No production
capacity check was weakened. Final complete lanes and the separate actual
release-policy paced capture remain mandatory.

## S7 final functional coverage and unresolved resource gate

The selected release-off state passes the complete native ordinary lane:
14,253 test pass events, 99 skips, 61 executed/15 build-only packages across 76.
The subsequent full race attempt reaches RBN with every earlier package passing,
then exposes the new clock-dependent noon fixture described in
[TSR-0137](troubleshooting/TSR-0137-study-geography-and-replay-clock-fixtures.md#follow-up-a-clock-dependent-wire-fixture).
Only that test changes, to distinct always-admissible `0000Z`/`0002Z` inputs.
RBN's full ordinary rerun passes 129 cases without skips. The race continuation
passes all 11 unfinished packages, 4,512 native pass events and five skips.
`s7-final-race-coverage-audit.json` verifies the successful unchanged prefix plus
continuation covers all 76 packages, 61 executed/15 build-only, with exactly the
same 99 skip identities as ordinary. The failed original invocation remains
failed; no fabricated single full-run pass or summed authenticated event count
is reported. The localized test fix does not invalidate other package results.

Complete final vet/Staticcheck/lint and connected ordinary/race CustomSCP
producer-to-consumer checks pass. RBN vet/Staticcheck/lint pass again after the
test-only clock repair. Guided parser fuzzing, independent temporal vectors,
bounded shape benchmarks, native original-peer/normal/DIAG delivery, reopened
archive and earlier unit-specific receipts remain valid at their recorded
source boundaries. Exact final receipts include `s7-final-ordinary-full.txt`,
`s7-final-race-full.txt`, `s7-final-race-continuation.txt`,
`s7-final-rbn-clock-ordinary.txt`, `s7-final-rbn-clock-{vet,staticcheck,lint}.txt`,
and `s7-final-customscp-connected{,-race}.txt`.

The actual release-policy three-case capture completes every one of 90,000
inputs and each TCP delivery, 90,003 reopened archive rows including markers,
natural recovery and all owner/barrier joins, without recorded loss. It still
**fails** the frozen typical CPU gate: 437,500 ns/input exceeds two retained
controls. The comparison receipt is `paced-final-release-gate-v1.json`.
Its failed verdict is not superseded by diagnostic cases, cycle counts,
post-hoc averages or favorable repeats.

Separate independent counter reviews verify the same-image profiler crossover,
exact kernel/user/cycle phase arithmetic and the original-core counterpart.
The unchanged original's two unprofiled CPU totals vary 41.57% while cycles vary
2.23%. These observations do not identify a code defect or harmless accounting
cause. WPR refuses the owned kernel-trace probe with policy error `0xc5585011`;
no recording remains. The full [qualification report](call-correction-v2-qualification.md)
retains all figures, provenance, precision limits, pending-work/recovery
differences and the copied baseline-plan prose discrepancy. All temporary
instrumentation is archived and restored byte-for-byte in both owned comparison
checkouts; their standard ordinary images are re-prepared. No instrumentation
or policy change is applied to the release source.

A fresh lead-owned source pass rechecks the approved parser/harmonic behavior,
receiver joins/cohort reduction, canonical identity/display/frequency witnesses,
fresh authority, admission/nomination, gate-local history, configuration presence
and capacity consumers, and actual mutation/accounting callers. No additional
runtime defect is identified. `resolver_canonical.go` remains one cohesive file:
publication construction and read merging use the same private canonical form,
join/display rules and capped rows, with small focused helpers. Separating those
derivations would obscure direct comparison of their shared invariants. The
long selection routine remains a bounded linear admission/priority/synthesis
flow, keeping its five source witnesses and early-return ordering together.
This source pass is lead-owned, distinct from the separate source/shape,
scientific, test-strategy and empirical recount reviews.

Final supporting receipts are `s7-closeout-{tsr-check,yaml-rigor,crawler-check}.txt`,
`s7-closeout-markdown-links.json`, `s7-closeout-codemap-{generate,check}.txt` and
`s7-closeout-diff-check.txt`. Map generation is the final source/document write;
checks and direct final-diff review follow it. ADR/TSR identifiers were adjusted
before commit because the concurrent primary-checkout task occupied ADR-0357
and TSR-0136. This branch uses ADR-0358 and TSR-0137/0138, without editing the
other task's checkout.

S0–S6 and the conditional study-input unit are implemented. S7 remains incomplete
at its CPU qualification obligation. The owner decision on retaining strict
EACH-control comparisons versus planning a new uncertainty-aware average-cost
study is pending. No new interpretation, scope or authorization is inferred.
No push, model installation, merge, production deployment or restart occurs.

## Follow-up: method priority and requested stability estimate

The owner subsequently prioritized the correction method and deferred CPU, heap
and allocation optimization. This does not choose the proposed average-cost
alternative or waive the original failed resource gate. The owner requested
the existing replay stability method as the accuracy estimate for more
corrections with improved or unchanged estimated accuracy.

Read-only postprocessing of the existing three frozen arms on November 7, 14,
15 and 16 reproduced all 12 original 60-minute/two-row/1,000-Hz stability
totals before rescoring the same predictions with the checked-in replay
settings: 15 minutes, three raw rows and 500 Hz. All 2,364,930 focal raw IDs
paired without duplicates or missing rows. Raw ZIP/decision hashes, admission
counts and original applied/stable totals reconciled.

The requested pooled estimate is shipped 25,489/34,527 (73.823%), repaired OFF
28,199/37,846 (74.510%) and frozen ON 28,449/38,155 (74.562%). The ON gain is
entirely 309 additional CW correction receipts, 250 stable and 59 unstable.
All 37,846 existing corrections retain their semantic and literal target,
decision time and stability result. No common correction is lost, retargeted
or retimed. Twenty-two additions change an OFF license rejection into output.
The stable `YO9KWC -> YO9CMC` receipt nevertheless disagrees with its strong
saved R2 reference. Follow-on stability and reference agreement remain distinct
proxy observations, not RF truth.

The [qualification report](call-correction-v2-qualification.md#owner-requested-stability-reanalysis)
records complete daily results, estimator semantics, provenance, pairing and
limitations. This supplementary S0/S6 evidence is recorded under the already
approved S7 documentation unit. No frozen artifact, prediction, configuration,
activation requirement or failed verdict changes. The proposed v5 diagnostic
implementation still requires exact `Approved v5`; no new diagnostic interface,
model, runtime method or production activation is implemented by this update.
November 17 contents remain unopened by the follow-up analysis.

The change is ordinary qualification/execution documentation. Existing support
routing still points to these reports; no operator command, runtime contract,
support-map entry, ADR or TSR changes. Use the documentation-only validation
lane; existing Go evidence remains at its recorded source boundaries.

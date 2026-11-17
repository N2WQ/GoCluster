# Call Correction v2 Execution

Authority: [approved Scope Ledger v3](call-correction-v2-scope-ledger-v3.md).
Owner authorization: exact `Approved v3`, 2026-11-17.
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
| S0 evaluation foundation/provenance | Legacy compatibility, frozen baselines, strict paired-study CLI and native paced fixture complete | 13 compatibility + 112 final study cases passed; native paced baseline passed; archive/candidate qualification pending |
| S1a legacy report parsing | Removed substring SNR fallback; whole integer grammar and human rules retained | 139 native parser/display/RBN cases passed; 20s grammar + 20s legacy differential fuzz passed (unguided prepared binary) |
| S1b harmonic presence | Missing incoming reports neither compare nor seed harmonic evidence; measured cross-receiver/mode policy preserved | 33 native presence/threshold/expiry/maintenance/delivery cases passed, including TCP and reopened archive |
| S2 neighborhood admission | Only admitted comparable adjacent populations set neighborhood use | 13 selection/accounting + 7 native delivery cases passed; immediate/temporal/stabilizer exact distance-2/3 recovery |
| S3 receiver SNR | Receiver timestamp/conflict join, immutable SNR rows, top-cohort bin scoring and fresh reranking complete | 1068-case spot pass before final scoring extraction; targeted final checks + 11 native race cases passed; modeled shape benches executed, final resource qualification pending |
| S4 canonical slash authority | Immutable canonical receiver union, observed display, independent threshold and identity-consistent consumers complete | Full spot (1087) and flow (202) passed; 14 native delivery, 144 replay/selection, 23 race cases passed; legacy fixture corrections passed 116 cases; final frequency scratch checks passed 63 cases |
| S5 per-decision history | Pending | Not run |
| S6 Bayesian retention/study/activation | Pending | Not qualified |
| S7 final integration/resource/docs/lane | Pending | Not qualified |

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
allocation, and the no-model partial branch retains its previous allocation path.
The independent S3 source review found no remaining production correctness issue
and identified the missing modeled capped benchmark; that case was added using
the actual checked-in model. Native race checks include simultaneous modeled
partial projections. Bench artifacts are component evidence; final quiet paced
comparison and full lanes remain required, and intermediate CPU timings are not
the release performance verdict.

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

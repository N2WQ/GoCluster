# Call Correction v2: v7 executed results

Status: **Implemented and executed; no qualifying method selected.**
Authority: the owner's exact `Approved v7`, November 18, 2026, and the later
clarification that the tertiary tie-break counts each affected input once.
Workspace: `C:/src/gocluster-call-correctionv2`, branch `call-correctionv2`.
All predictions and evaluations used clean source
`188c30af9632fae7ab8b7629298dd0f954d0341c`.

The new method actually ran. Across four exposed archive dates, all sixteen
complete variants increased applied corrections and the pooled follow-on
stability fraction. Every variant nevertheless introduced at least one new
strong-reference-disagreeing rename under each frozen reference and failed the
approved tuning eligibility rule. The fixed selector returned
`no_eligible_candidate`, with `selected_complete_method: null`.
No November 17+ archive contents, names or hashes were inspected for this work.
Prospective confirmation was not started because no tuning candidate qualified.
No live method was installed or activated.

## Complete experiment and observed gains

The four dates were November 7, 14, 15 and 16, 2026. Each arm contains all
2,364,930 focal inputs: respectively 458,495, 882,021, 793,630 and 230,784.
Four disabled controls reproduced the original complete E1 decision streams
byte for byte before active outcomes were interpreted. All 64 active captures
and all eight three-role paired evaluations completed without a failed run or
retry. Each arm had independent correction/history/temporal state.

The repaired-OFF control applied **37,846** corrections, of which **28,199**
were stable: **74.509856%**. Stability retains the existing replay rule:
15 minutes, at least three follow-on rows and 500-Hz tolerance, including its
same-second, inclusive-boundary and day-end behavior. It is a reporting
consistency estimate, not the probability that the transmitted identity was
correct. Selection used integer cross products, not the rounded percentages.

Each row below represents **two separately executed complete candidates**:
`f<gap>_t<seconds>_geometry_d<distance>` and
`f<gap>_t<seconds>_ordinal_d<distance>`. Their applied results match. Thus all
sixteen candidates are represented; these are not cell-wise combined winners.
`d2` enables CW distances one and two; `d1` enables distance one only.
The R1/R2 columns count new disagreeing renames separately and can overlap.

| Gap Hz | Witness seconds | CW map | Applied | Stable | Stability % | Added / lost | Net gain | New R1 / R2 disagreements | Eligible |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 150 | 60 | d1 | 38,143 | 28,456 | 74.603466 | 298 / 1 | 297 | 1 / 1 | No |
| 150 | 60 | d2 | 38,668 | 28,864 | 74.645702 | 823 / 1 | 822 | 84 / 29 | No |
| 150 | 120 | d1 | 38,197 | 28,496 | 74.602717 | 352 / 1 | 351 | 4 / 1 | No |
| 150 | 120 | d2 | 38,764 | 28,924 | 74.615623 | 919 / 1 | 918 | 101 / 29 | No |
| 250 | 60 | d1 | 38,148 | 28,456 | 74.593688 | 303 / 1 | 302 | 1 / 2 | No |
| 250 | 60 | d2 | 39,222 | 29,305 | 74.715721 | 1,377 / 1 | 1,376 | 256 / 44 | No |
| 250 | 120 | d1 | 38,176 | 28,473 | 74.583508 | 331 / 1 | 330 | 4 / 2 | No |
| 250 | 120 | d2 | 39,284 | 29,343 | 74.694532 | 1,439 / 1 | 1,438 | 267 / 44 | No |

Every variant retained 37,845 common applied inputs, lost one, and had zero
retargeted or retimed common baseline receipts. The affected-baseline-input
union is therefore one. The complete accounting also preserves noncorrection,
licensing, hold, unknown, outside-map and reference outcomes. The observed
outside-active-cell rename count is two in each variant; it is descriptive
accounting of the complete stateful arm, not evidence that every changed
outcome was a direct extra-path proposal. The observer classifies distance from
the original raw subject to final output; proposal admission measures from the
current correction-stage subject using canonical gate distance. Thus this
counter alone does not establish that a proposal escaped its admission mask.
All-cell reference checks include it.

Pooled improvement does not imply every date/mode improved. Exact stability
declined in these groups, in both rankings:

| Variant pair | Date | Mode | Control stable / applied | Variant stable / applied |
| --- | --- | --- | ---: | ---: |
| f150_t120_d2 | November 14 | CW | 8,932 / 11,544 | 9,247 / 11,957 |
| f250_t60_d2 | November 14 | CW | 8,932 / 11,544 | 9,455 / 12,228 |
| f250_t120_d2 | November 7 | CW | 5,403 / 7,298 | 5,511 / 7,444 |
| f250_t120_d2 | November 7 | All | 5,406 / 7,302 | 5,514 / 7,448 |
| f250_t120_d2 | November 14 | CW | 8,932 / 11,544 | 9,468 / 12,256 |
| f250_t120_d2 | November 14 | All | 11,181 / 14,774 | 11,717 / 15,486 |

These are diagnostics, not a newly imposed selection gate. The frozen pooled
objective passed in all sixteen arms; the existing reference rename gate failed.
No thresholds, complete maps, reference rules or tie-breaks were changed after
examining results.

## Bayesian attribution and practical SNR coverage

Across all eight otherwise identical pairs, direct comparison of actual applied
membership, canonical target, literal target, scorer key, decision time,
current-stable flag and legacy-stable flag found **no difference** between
geometry ordering and Bayesian-derived ordinal ordering. This is stronger than
equal aggregate totals, but does not assert byte equality of whole Spot objects
or of every nonapplied decision. The measured gains belong to the expanded
proposal method; there is no measured applied-output increment from this
Bayesian ranking role on these dates.

The full coverage scan parsed and hashed all 32 ordinal receipt files,
28,997,294,722 bytes. Each ordinal candidate recorded 1,859,139 rescue
assessments across the four dates. These are invocations, not necessarily
distinct incoming IDs and not all archive reports. Eligible below means a
current lifecycle and a nonempty population after temporal permission filtering.

| Gap / CW map, both witness windows | Eligible assessments | All scores available | Geometry fallback | Fully scored populations with at least two candidates | Candidate score attempts |
| --- | ---: | ---: | ---: | ---: | ---: |
| 150 / d1 | 100,535 | 100,153 | 382 | 6,090 | 106,955 |
| 150 / d2 | 153,052 | 152,494 | 558 | 16,450 | 171,999 |
| 250 / d1 | 101,420 | 101,032 | 388 | 6,142 | 107,895 |
| 250 / d2 | 155,525 | 154,958 | 567 | 16,798 | 174,860 |

More than 99.6% of eligible assessments had all required scores. The largest
post-temporal population was six; none exceeded the eight-trial beam. Most
eligible assessments had only one candidate: respectively 94,418, 136,539,
95,251 and 138,664. Ranking cannot choose a different candidate in a singleton.
Multicandidate score availability establishes an opportunity to rank, not that
ordering changed or that two candidates could both pass the unchanged gates.
The scan does not reconstruct those unrecorded counterfactuals.

The model used the focal report's own measured SNR for every compared
candidate. Bayesian estimation supplies smoothed edit-event parameters; the
score is the declared single-alignment ordinal compatibility. It is not a
marginal string likelihood, Bayes factor or RF posterior. The complete
training census, smoothing, source direction, exposure floors and unavailable
strata are recorded in [construction evidence](call-correction-v2-v7-validation.md).
On an unavailable assessment the native method falls back for the whole
population. Receipts do not record which individual candidate lacked a score,
so individual candidate success/failure totals remain unknown. High observed
availability establishes that fallback was uncommon overall. It cannot exclude
fallback concentrated in the consequential ranking opportunities, and does not
establish calibration or model quality.

## What the failed guard means

R1/R2 are frozen RBN-derived reference procedures, not separately observed RF
truth. Across all 2,364,930 focal rows, R1 was strong for 1,085,263 and R2 for
456,322. Strong labels disagreed on 13,171 rows. The full reports retain
cochannel, family-count, conflict, boundary and context-unknown reasons rather
than assigning correctness to unlabeled cases. Reference disagreement rejects
this experiment under its agreed rule; it does not prove each new correction
renamed a genuine station. Pooled stability cannot resolve that disagreement.

Two exact recorded counterexamples from `f150_t60_d1`, with both rankings equal:

- **DK2FGT to DK2FG**, November 14, 14:34:22 UTC, 7028 kHz. The ordinary
  proposal OK7NV failed edit distance. The extra proposal passed with 22/37
  receiver support and 59% confidence. R1 was unknown and R2 labeled G3ZBU
  with 31 families. Current follow-on stability was false; legacy stability
  was true. There were three components; the associated component had zero-based
  index two and one other component was excluded. The population retained the
  associated component plus unresolved dissent from the remaining component.
  This is not a demonstrated licensing/gate bypass.
- **SP7MS to SP7M**, November 14, 17:19:31 UTC, 1811 kHz. Ordinary OK7O failed
  edit distance. The extra proposal passed with 5/11 support and 45% confidence.
  There was one component and no exclusion: the full dissent population was
  retained. R1 labeled OK7O with three families; R2 was unknown because of
  reference conflict. Current stability was true. Broader editable-candidate
  search can therefore authorize a minority proposal without a proven separate
  signal, while obeying the current numerical gates.

These examples expose the limitation of this method family. They do not
establish a normative new threshold or justify silently removing the reference
guard. The second case also defeats the claim that all gains arise from
frequency separation. Current-match support, gate allowance, archive stability
and reference agreement are distinct kinds of evidence.

## Precise disposition and remaining information need

The failed premise is: **this fixed expansion family can deliver more
corrections, preserve pooled stability and introduce zero new strong-reference
renames on the exposed comparison.** The first two conditions held; the third
did not in any complete candidate. The separate premise that this Bayesian
ranking supplies an applied increment was also unsupported on these inputs.
There is no eligible method/model/map to freeze for prospective confirmation
or later live integration. Production resource qualification also remains
unmet and unwaived, as documented in the [qualification report](call-correction-v2-qualification.md).

The next useful design question is whether the method should explicitly compare
**keep the incoming identity, replace it with a supported candidate, and retain
an unresolved competing-signal explanation**, instead of only ordering eligible
replacement strings. That is a recommendation for a later reviewed scope, not
an implemented Bayesian classifier or an approved new decision rule. A useful
study would need falsifiable outcome labels/unknowns for those competing
actions, independent current-report evidence, retained dissent and dependence
controls. It must show incremental gains beyond geometry and preserve a
separate untouched confirmation set. RBN alone still supplies proxy outcomes;
it cannot certify actual transmitted calls or reconstruct historical receiver
sites. CTY geography remains the owner-approved conditional study estimate.
History-only active replacements remain outside the owner's accepted behavior.

No new predictions, refitting, alternative activation criteria, combined maps,
live integration or production settings are authorized by this verdict.

## Evidence bindings and verification limits

All local paths in this section are relative to `.tmp/call-correctionv2/`.
Large artifacts are ignored local evidence, not published repository files.
Native COMPLETE files bind closed outputs and hashes. Each capture/evaluation
kept the run-owned 4-GiB evidence/assets bound; scratch was separately measured
under v6, not included in that cap. The inherited unobserved Custom SCP Close
error limitation is unchanged. This is not a total-study disk or process-RSS cap.

| Evidence | Path | SHA-256 |
| --- | --- | --- |
| Source/input/grid freeze | `v7-grid-exposed-v1/FREEZE.json` | `57e0f0287222f1b01b41639d48fcdafd78bf49600b633425558ab74803ee1c9b` |
| Four disabled controls | `v7-grid-exposed-v1/disabled-parity.json` | `27851a015f0a8af7835ea64f9d3885a84dfd6e81d0f5e7a6ccc40d21f62dd36b` |
| Frozen fitted model, 21,373 bytes | tracked `cmd/rbn_replay/testdata/ordinal_fitted_v7.json` | `968caf263f3bf44c13509ded8b5e48faf0bd39b1d7cc42dcd9d1e6c0a45e5e1a` |
| Full sixteen-candidate selection | `v7-selection-v1/selection.json` | `0b5250517b053f1e18d22f72edc7f951d2f14be529ad9fc5b47b9d0a471df13a` |
| Full ordinal availability census | `v7-ordinal-coverage-v1/coverage.json` | `87f6a75ec8bc1e3b4ff28bc439ec9459c73f50435263adc22111a9670634707c` |
| Separate all-eight receipt arithmetic | `v7-paired-recount-v1/all-eight-verification-summary.json` | `8936a2f73b886abc1869d687185c84c70863890cfbd8b6a429e3c798e3a33677` |
| Worker capture verification v2 | `v7-grid-execution-verification-v2/verification.json` | `852a4e6abd441fbf2b5ddf6255f0d6e583f3de2a5f8d6431c4b251f7e809b999` |
| Two recorded diagnostic cases | `v7-paired-recount-v1/v7-eval-f150_t60_d1-v1/two-id-recorded-capture-diagnostic.json` | `3c659734d4c1b92ac88a8d706f2f03446b5de440099ad38be4c8bc7cebcf3b38` |

The selector binds all eight complete native evaluations, plans and aggregate
hashes. Native paired files contain every incoming ID, and the native evaluator
reconciles the old 60/2/1000 manifests before using the unchanged 15/3/500 scorer.
A separate initially non-steered arithmetic reviewer derived emitted receipt
and flag counts before consulting native aggregates. This verifies arithmetic
and pairing predicates, not independent RF labels or independent raw rescoring.
The separate geometry/ordinal arithmetic check had earlier native context and
is explicitly not a pristine/blind review. Its literal cases and full direct
stream comparisons still provide useful separate arithmetic evidence.
Across all eight pairs, primary comparison retained 6,148/6,160 raw matches
and twelve explicitly reconciled vocabulary differences, with zero numeric or
exact-order discrepancies. Direct comparison matched all 2,240 checks. These
are eight settings over the same cohort, not 18,919,440 independent RF reports.
The final recount evidence uses 3,120,539 bytes under its four-MiB bound.

The first execution-controller proof had hash/parse snapshot and late-failure
publication defects found in source review. It is telemetry only. The corrected
verification-only v2 reads/hash-checks the same bounded bytes and revokes COMPLETE
on late failure; its self-check and actual 60-capture verification passed. Native
captures were not rerun or altered. The first four active captures were checked
by the lead separately. Root plan-assembly assertion-side effects and coverage
short-write/readback issues were also corrected and rechecked before their
authoritative outputs. These findings are not concealed as universally clean
first attempts. Two pure evaluators ran concurrently after captures closed;
250-Hz evaluator children used `GOMEMLIMIT=5GiB` as a soft GC target only, with
no predictor, asset, threshold or metric changes. It is not an RSS bound or a
production performance result.

The complete current-source ordinary and race package coverage each passed
14,940 cases with 99 documented skips across all 76 packages. The ordinary
three-minute package timeout required a normal-limit continuation over the
remaining packages; the unchanged disjoint prefix and tail form the complete
ordinary coverage, not a successful original invocation. Full vet, Staticcheck
and lint passed; lint reported zero issues. Final native method/evaluation wire
fuzzing passed 57,730 executions in approximately sixteen seconds, with two
workers. Skips include opt-in resource/soak/production qualification and external
fixtures; none is silently reported as a pass. These checks establish software
evidence within their scope, not production capacity or empirical RF accuracy.
See [construction and final validation](call-correction-v2-v7-validation.md)
for source-to-scope mapping and review disposition.

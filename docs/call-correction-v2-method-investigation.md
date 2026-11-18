# Call Correction v2 Method Investigation

Authority: [approved v5](call-correction-v2-scope-ledger-v5.md), E0–E3.
This is preliminary exposed-data evidence, not a completed feasibility verdict,
selected replacement model, implementation approval or activation qualification.

## What the evidence can establish so far

The existing frozen ON predictions produce more applied corrections and a
slightly higher pooled follow-on stability fraction than repaired OFF. The
[qualification report](call-correction-v2-qualification.md#owner-requested-stability-reanalysis)
keeps the exact counts, date/mode results and conflicting reference evidence.
The reproducible E0 audit now checks those same receipts without making new
predictions. It does not establish that a broader candidate method will work.

The exposed tuning exports contain enough correct-match observations to
investigate a fuller observation model. Their weak references, normalization,
selection and alignment limits prevent treating the existing error counts as
calibrated RF probabilities. Whether the live retained evidence can distinguish
the missed signals remains an E1 question.
The selected stability criterion can count same-second evidence already known
at decision time. A future fitted score must distinguish satisfying this
archive criterion from forecasting new corroboration or estimating RF identity.

## Reference and opportunity denominators

A separate read-only reviewer re-counted the two frozen repaired-OFF tuning
streams. The lead verified their hashes and arithmetic. The source producer is
`00061b226e717a5d2c66b61c5e1d7e2be5596932`, plan
`8cdba5fde284f8bf7ad34ede758afc9bcd3856bbf99a44c9321376f868864c42`,
candidate `cw2_r450_a100_d0` and reference version `ccv2-reference-v1`.

| UTC date | Focal receipts | Not both references strong | Strong but disagree | Strong and agree | Canonical full-call matches | Unit distance 1 / 2 / 3+ |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| November 7 | 458,495 | 401,813 | 2,820 | 53,862 | 53,432 | 222 / 90 / 118 |
| November 14 | 882,021 | 793,245 | 4,308 | 84,468 | 83,683 | 306 / 121 / 358 |
| Total | 1,340,516 | 1,195,058 | 7,128 | 138,330 | 137,115 | 528 / 211 / 476 |

These predicates use exported `r1` and exported terminal `r2`, not `r2_frozen`.
Strong agreement requires both `strong` flags and equal exported identities.
The subject is `subject_identity`; the reference is `r1.identity`, with no
additional analysis normalization. Distance is unit-cost string Levenshtein
with insertion, deletion and substitution, without transposition. Unavailable
or discordant labels are not silently counted as correct or erroneous calls.
This conditional subset does not estimate error prevalence over all receipts.
No asset alphabet or unknown-character mapping is applied to these counts.
The not-both-strong group contains 605,745 neither-strong, 475,187 R1-only and
114,126 R2-only receipts. Exported terminal R2 includes the producer's decision
cutoff screening; the analysis does not silently substitute its frozen form.

The agreeing subset has 675,647 reference characters and 675,405 subject
characters. Unchanged calls alone contribute 669,588 exact character matches.
A deterministic minimum-cost alignment gives 672,791 matches, 2,274
substitutions, 582 deletions and 340 insertions, from reference to subject.
It builds suffix costs and walks forward, preferring match, substitution,
deletion, insertion on equal cost. These counts depend on that alignment;
the training/runtime prefix-cost end-backtrace can attribute ambiguous optimal
alignments differently. No alignment parity or physical error-event claim is
made, and insertion counts alone do not define insertion opportunities.

Within these 138,330 receipts, `raw.DXCall` differs from `subject_identity`
241 times; 238 also match the reference after canonicalization. These are
representation changes, not decoder-error labels. Exported `raw.DXCall` is
already parser-normalized, so the character accounting is over canonical
identity strings rather than untouched decoder glyphs.

The current confusion asset has SHA256
`e6412deb8864f032696e99ddf6fb888a1de185608b9ef84a28af5892ca8de50d`.
The inspected producer `C:/src/rbnanalytics/rbn_train/confusion.py` skips MATCH
operations in both builders and exports error counts without match/exposure
arrays. The current [consumer](../spot/confusion_model.go) also skips match
terms; the [resolver application](../spot/resolver_confusion.go) scores only the
top cohort tied on raw and weighted support. These observations explain why
the asset is insufficient for a complete calibrated channel or posterior.
They do not authorize changing its existing tie-break use.
These totals are not complete mode/SNR-stratified opportunity tables. Archive
numeric zero is a measured report, and this projection does not establish the
live missing-SNR population. Those distinctions remain explicit in the
[replay evidence limits](rbn_replay.md#bayes-evaluation-and-evidence-limits).

## Focal-SNR census and reference outcomes

A subsequent full stream census groups the same exposed receipts by mode and
the focal parsed SNR using the existing asset intervals. A separate computation
that did not see the lead's new receipt/script reproduced all 336 date/mode/bin
count comparisons. This is a focal-report table, not retained candidate-receiver
SNR coverage or current confusion tie-break eligibility.

| Mode | Focal SNR, dB | All focal rows | Both strong and agree | Nearby misses | Output reference gain / harm |
| --- | --- | ---: | ---: | ---: | --- |
| CW | (−999,0] | 2,755 | 177 | 0 | 3 / 0 |
| CW | (0,6] | 108,894 | 8,192 | 18 | 128 / 13 |
| CW | (6,12] | 238,475 | 23,998 | 30 | 212 / 27 |
| CW | (12,18] | 242,402 | 27,478 | 7 | 117 / 36 |
| CW | (18,24] | 204,260 | 24,612 | 5 | 97 / 32 |
| CW | (24,999] | 332,320 | 39,578 | 11 | 135 / 59 |
| RTTY | (−999,0] | 51 | 3 | 0 | 0 / 0 |
| RTTY | (0,6] | 3,684 | 125 | 0 | 0 / 0 |
| RTTY | (6,12] | 38,633 | 1,747 | 0 | 4 / 2 |
| RTTY | (12,18] | 51,465 | 3,421 | 0 | 1 / 5 |
| RTTY | (18,24] | 43,760 | 3,213 | 1 | 3 / 4 |
| RTTY | (24,999] | 73,817 | 5,786 | 3 | 10 / 10 |

Gain/harm means semantic identity agreement with the same strong-agree archive
reference before/after the existing OFF method, restricted to output rows.
It is not a comparison of a new method against OFF, nor proof of RF errors.
The complete transition table keeps its fixed denominator:

| Subject matches reference | Final matches reference | Output receipts |
| --- | --- | ---: |
| Yes | Yes | 136,768 |
| No | Yes | 710 |
| Yes | No | 188 |
| No | No | 496 |

Another 168 strong-agree receipts are non-output license rejections and remain
outside that output matrix. The 188 contradictions comprise 167 CW and 21 RTTY
receipts, with 48 on November 7 and 140 on November 14. These known negative
cases matter alongside missed corrections; adding candidates alone does not
address them. This table does not supply correction-only precision or the
precision of additional corrections; those require their own applied/paired
denominators. The owner's primary estimator remains existing follow-on
stable/applied receipts, with reference evidence retained as diagnostics.

All 1,340,516 exported focal measurements are present and inside the asset's
numeric domain: 1,128 are zero and 1,678 negative. Missing/out-of-domain focal
categories contain zero rows. This does not imply that retained receiver rows
have no missing or conflicting measurements. R2 and R2Frozen are identical
objects on this particular two-date dataset, with zero decision-cutoff masks;
the source contracts remain distinct. Deterministic alignment yields 340
inserted characters at 249 used gaps among 813,977 reference gap positions.
These are alignment-dependent canonical-string units, not measured physical
insertion events or a calibrated insertion probability.

A fresh, separate non-steered scientific reviewer received the neutral goal,
source/asset/reference definitions and frozen file hashes, without the lead's
conclusion, intended model, ledger or expected counts. The reviewer established
the counting/claim boundary and interval vectors; the lead checked the source
and arithmetic. The reviewer did not perform the complete census. A different
separate computation supplied the full population crosscheck described above.
There is no material disagreement after disposition: focal descriptive counts
are feasible, but actual retained tie eligibility, safe candidate availability
and calibrated RF correctness remain unestablished. An independent RF-accuracy
oracle is missing.

Representative independently derived interval vectors use exact categorical
membership in supported CW/RTTY numeric bins; these are asset conventions, not
empirically optimal physical thresholds:

| Input | Expected result | Failure detected |
| --- | --- | --- |
| Present report −999 / −998 dB | Outside / first bin | Lower endpoint is excluded |
| Present report 0 / 1 dB | First / second bin | Zero is measured, upper endpoint inclusive |
| Present report 6 / 7 dB | Second / third bin | Adjacent boundary classification |
| Present report 999 / 1000 dB | Last bin / outside | No upper-domain extrapolation |
| Retained missing/conflicting measurement | No measurement contribution | Unknown is not numeric zero |

The complete independent vectors and source-qualified claim limits are retained
in `v5-e2-snr-scientific-review-v1.json`, SHA256
`c7c4343543781b55590fc653a51536eb64baadfc4407a3d94d9a609a8dda876c`.
The census is `v5-e2-snr-opportunities-v1.json`, SHA256
`ec3161fd2d6b8292edeb5326b7dd046f6a91d814ed06cbea8a819cb0f1f7028e`;
its 336-count crosscheck is `v5-e2-snr-count-crosscheck-v1.json`, SHA256
`d3832353b2b00836ebc769f73ef56aa0de718278dc1fa4216680fe0d21dd26f5`.
Paths use the local evidence root below. No training, asset replacement or
current-method improvement is claimed by this census.

## Nearby opportunities and the missing observation

Among 739 agreeing-reference disagreements at distance one or two, 664
receipts attain the reference in 614 frozen episodes; 75 do not, in 73
episodes. Two episodes contain both outcomes. Attainment requires output plus
an applied semantic identity correction whose final identity equals the
reference; identity equality alone is insufficient.

Of the 75 misses, 24 selected the reference but did not deliver it, 26 selected
another identity and 25 exported no selected identity. Increasing the candidate
pool alone cannot resolve the first group. The exports do not contain the full
focal candidate membership, so the other groups do not prove candidate absence.

A complete exported-gate accounting binds all 75 IDs to selection v2, each raw
ordinal/SHA and the two complete source decision files. Within the 24 already
selecting the reference, eight logged `recent_plus1_reject_distance_or_family`,
seven logged `recent_plus1_reject_winner_recent_insufficient` and seven logged
`gate_min_reports`. The remaining two passed the exported gate but logged
`resolver_license_rejected`; neither was applied. These are CW receipts, with
14 distance-one and ten distance-two reference differences. The export does not
expose every effective threshold or recent-support lookup, so this accounting
does not infer which policy change would recover them.

Of the 26 selecting another identity, 22 logged a maximum-distance rejection,
two applied a different identity and two logged `resolver_same_call`. Of the
25 with no selected identity, seven logged no snapshot, 17 state split and one
neighbor conflict; none evaluated the correction gate. Eighteen of that group
still exported a nonzero snapshot reporter count. Exported zero gate counts
therefore do not establish absent retained support.

The ignored local receipt `v5-e2-near-miss-gates-v1.json` has SHA256
`d622505497e663d77e0fd9a8551dcd7d351da4d7ee2a61c747780fb99de11ead`;
its script has SHA256
`1645f0349e1db177bb75e8667b7b95525c03e17523ebac3c3cb279664302b1bb`.
The lead verified these bindings and inspected the procedure. This is a
delegated export accounting, not a second replay, retained-state reconstruction,
attainable-gain estimate or RF correctness finding.

Two diagnostic examples contrast causal raw history with exported selection:

| Focal receipt, November 7 UTC | Prior A raw history | Terminal selection |
| --- | --- | --- |
| Ordinal 174575, `R9AA`, 09:14:28, 14005.9 kHz | Nine exact-400-Hz-bucket `R9AAB` receipts at −10…−1 seconds; 18 older `M7MNM` receipts at −224…−205 seconds | R1/R2 name `R9AAB` with 14/16 families; selected `M7MNM`, support 14/15 total, distance rejection |
| Ordinal 520537, `DL3VWF`, 16:55:44, 7021.0 kHz | Seventeen exact-bucket `DL3KWF` receipts at −280…−2 seconds; competing `DL3DRN` has 41 receipts/40 simple families, mainly 7021.3 kHz | R1/R2 name `DL3KWF` with 12/13 families; selected `DL3DRN`, support 34/34 total, distance rejection |

The bucket test uses the actual floating half-up source expression. These are
raw-history observations, not reconstructed retained votes. Admission,
supersession, eviction, publication timing and actual candidate ranks remain
unknown until E1 captures the evidence actually consumed by the selector.
Full causal history is an information-sensitivity comparison, not permission
to use information unavailable to the live decision.

## Raw archive SNR representation check

A separate bounded lexical census opened only the four exposed archives and
verified their frozen hashes before and after reading. It counted 5,066,942 raw
records: 5,066,938 integer-format SNR tokens and four missing tokens accompanying
the saved producers' four bad records. Decimal, exponent, nonfinite and other
tokens were absent, including `-6.0`. Every accepted CW/RTTY row had integer SNR.

| Date | All raw records | Integer SNR | Missing | Accepted CW | Accepted RTTY |
| --- | ---: | ---: | ---: | ---: | ---: |
| Nov 7 | 1,004,689 | 1,004,688 | 1 | 995,175 | 2,606 |
| Nov 14 | 1,858,683 | 1,858,682 | 1 | 1,379,016 | 479,452 |
| Nov 15 | 1,680,668 | 1,680,667 | 1 | 1,160,661 | 519,692 |
| Nov 16 | 522,902 | 522,901 | 1 | 520,729 | 1,707 |

These counts cover all receivers and include accepted beacons; they are not the
focal-A correction population or retained-candidate eligibility. Negative values
(3,330 raw tokens) and zero (3,332) are measured integers. The offline parser's
negative-float conversion edge has no observed exposure in this corpus. This
does not establish its absence from other inputs or prove that all retained
receiver SNRs remain present/nonconflicting.

The script mirrors current ZIP/header selection, enforces eight million rows
and 512 MiB per date, and reconciles its supported ASCII parse projection with
saved valid/bad totals. It performs no prediction or fitting. Evidence:
`v5-e2-raw-snr-lexical-census-v1.json`, SHA256
`d0d400aa231d1b77e24f477f56efd10fbdf02c963b2ac5ffe8c958d01201da1a`;
script SHA256 `dc3ccb4ca6fb3ca919a59e0de825ac132c3eb5d8fa57569adc596b356ab9c379`.
The lead checked these bindings and the source/procedure; this was a delegated
lexical census, not a second independent full recount or an RF accuracy oracle.

## Independent Bayesian contract review

A fresh, non-steered scientific reviewer examined current observational source
and the descriptive aggregates without the lead's preferred design, Scope
Ledgers, implementation plans or other reviewers' conclusions. No Nov17 data,
fitting, predictors or implementation were used. The supplied artifact receipts
were not independently revalidated in that review. The lead checked the material
derivations and retained the report summary as
`v5-e2-bayesian-contract-independent-v1.md`, SHA256
`6d71357130138c50ba99f2457a39eda66314ead0b116e15918e64c001eaba5a3`.

The review establishes mathematical distinctions, without choosing a method:

- A current call-report witness is separate from an available numeric SNR.
- Candidate presence, statistical ranking, ordinary gate allowance and accepted
  correction output are different denominators.
- Stability is a binary outcome for each candidate. Two real nearby calls can
  both obtain three qualifying follow-on rows, so their stability probabilities
  need not sum to one. A categorical probability of the focal transmitter's
  identity requires different hypotheses, signal-assignment assumptions and an
  outside-candidate possibility.
- Full observation likelihood needs matches, event/non-event denominators,
  dependence, missingness and alignment treatment. Current edit-category scores
  and support/history log ratios do not establish that model or calibration.

Bayes' conditional-probability identity does not by itself establish which
observations constitute justified likelihoods or prior odds; overlapping history
and current evidence can count the same information twice. See
[NIST's Bayesian methodology](https://www.itl.nist.gov/div898/handbook/apr/section2/apr1a.htm).

Representative independent synthetic vectors illustrate the claim boundaries;
these do not calibrate RBN behavior. Numeric tolerance is `1e-9`.

| Inputs/domain | Expected result | Intended failure detection |
| --- | --- | --- |
| Two exhaustive identities: priors 0.2/0.8; event likelihoods 0.6/0.1, dimensionless | Posterior `0.12/(0.12+0.08)=0.6` | Likelihood mistaken for posterior |
| Candidate-set coverage 0.6; within-set candidate probability 0.9 | Absolute probability `0.54` | Conditional normalization presented as absolute certainty |
| Equal priors, likelihood ratio 9; an exact duplicate versus a second conditionally independent event | Duplicate: `0.9`; independent: `81/82=0.98780487804878` | Unjustified receiver/report independence |
| Each of calls A/B has three raw rows well inside the selected time/frequency bounds | Both stability outcomes equal 1, categorically | False exclusivity imposed on stability |
| Measured 0 dB versus absent measurement | Distinct states; no error-rate ordering follows without calibration | Missing SNR treated as zero or adverse evidence |

**Independent oracle missing** for RF-correctness probabilities, appropriate
Bayesian strength/thresholds and attainable safe correction counts. RBN references
and the selected stability criterion do not supply a physical truth set. The
review supports exploring Bayesian proposal/comparison aids with explicit
assumptions and empirical checks. It does not support calibrated RF probability
claims, guaranteed gains or an active method verdict.

Calibration must concern an explicit target and population; ranking success
alone is insufficient. Repeated selection on exposed data limits generalization
evidence. See [Gneiting and Raftery (2007)](https://sites.stat.washington.edu/raftery/Research/PDF/Gneiting2007jasa.pdf)
and [Taylor and Tibshirani (2015)](https://pmc.ncbi.nlm.nih.gov/articles/PMC4485109/).
Actual retained candidates, signal evidence and gate causes remain the E1/E2
dependency before selecting a concrete E3 design.

## Frozen diagnostic inputs and remaining work

The capture selection freezes all 75 nearby misses and stratified controls
before E1 execution: 271/283/244/240 IDs for November 7/14/15/16, respectively,
1,038 total in preserved v1, below 512 per date. Categories include attained near and far
references, original matches, split/no-selected exports, mode, parsed zero and
negative SNR, aliases and incomplete references. This sample is not a population
recall or calibration denominator. November 17 contents remain unopened.

Before any E1 capture, selection v2 preserves every v1 ID and all 75 mandatory
misses, then adds 69 unique known-reference-harm controls: up to 24 from each
complete date/mode harm stratum on November 7/14. It freezes 295/328/244/240
IDs, 1,107 total, still within 512/date and 64 detailed records/ID. The new
strata are subsets of the approved original-correct controls, reviewed as a
validation refinement. The exact semantic predicate, source hashes, complete
eligible counts, SHA256 seed/input/tie-break and overlap handling are recorded
before capture. V1 is unchanged; these outcome-selected cases are neither
representative accuracy data nor untouched tests and cannot establish future
harm coverage.

Local evidence is under `.tmp/call-correctionv2/`:

- `v5-e2-preliminary-accounting-v2.json`, SHA256
  `3fc844a60898cafe2f95a0dfa3b40e221dccee8732f09a78e2cfcd50edaee4af`.
- `v5-exposed-case-selection-v1.json`, SHA256
  `a96ca4cd501440b4f15fee4883306486b6850b0694215c3378a333f9faeab2fd`.
- Active `v5-exposed-case-selection-v2.json`, SHA256
  `b1d23891d173cc7ebc34fa7a8c590116c08be01ab5de2f1062be4b304ef876dd`.

The owner resolved the [storage guarantee gap](call-correction-v2-scope-ledger-v5.md#post-approval-material-finding-e1-storage-ceiling)
and approved its [v6 amendment](call-correction-v2-scope-ledger-v6.md). E1 has
resumed. Its shared diagnostic unit and replay storage/wiring are implemented
and checked through the complete final functional lane; actual captures remain. No candidate
generator, fitted model or active correction-method change is implemented
under v5/v6. The retained-versus-history study and E3 method recommendation
still depend on successful capture.

## Shared diagnostic checkpoint

`internal/correctionflow` now exposes diagnostic selector, temporal-release
and decoder-Evaluate entry points over the same ordinary execution cores.
Existing public callers keep diagnostics disabled. Captures own their strings,
nested aliases, receiver rows, five-key read/witness vectors, actual temporal
window/candidate sets and executed rule results. They retain no publication,
view or request pointer and add no lookup, rule execution, clock, Step/drain,
callback or I/O. Capture errors remain separate from ordinary decisions and
request retirement. A normal generation mismatch is a recorded refusal.

Consumed receiver rows include the actual publication recency window and
SNR presence/conflict fields. Their originating archive ordinal is unavailable;
the interface does not guess one from call, receiver and time. Compact views
are explicitly unavailable. Unvisited checks remain unvisited. Rule rows inside
the existing aggregate-map iteration can differ in order between runs; the
trace describes actual execution and does not promise byte-identical ordering.
Ordinary ranking retains its existing sorting.

Final shared-unit source passed 275 native ordinary and 275 native race cases,
zero skips, plus targeted vet, Staticcheck and lint. Twenty-two pre-change
benchmark shapes, three repetitions each, retain identical measured allocation
counts with diagnostics disabled. Dense-case allocated bytes measured
163,460–163,468 B/op versus 163,456–163,459 before; no exact byte, CPU, p99 or
whole-process resource equivalence is claimed. These are intermediate package
checks, not the complete final E1 lane or production qualification.

Frozen baseline fixture SHA256 remains
`a3f737095ca9c37078606f697240267f9c26e673743529409c4f27cb5c542a9f`.
The final allocation receipt `v6-e1-shared-allocations-v3.json` has SHA256
`a87c7495430866ca0b25b25862e8a4f82119fed2bb7f20d39df07bd3bec14bc9`.
A separate source-quality reviewer found no production blocker; the lead
adopted the recency-window refinement and ordering caveat and verified the
final source/checks. This is separate code review, not an additional
independent scientific verdict. The earlier non-steered scientific review
and its reference/RF limitations above remain controlling.

## Artifact primitives checkpoint

The next E1 unit implements the run-owned artifact quota and scratch measurement
primitives in `cmd/rbn_replay`. The operative artifact allowance is 4 GiB with
16 MiB reserved from ordinary data writes for metadata/failure. Requested writes
are charged cumulatively before the underlying sink; deletion does not refund
bytes and rename does not charge again. Short writes and path, preflight,
copy, Sync, Close and rename failures preserve the first failure. Only the
explicit FAILED path may attempt output after failure. JSON rows include their
newline in the 64 KiB limit; logical metadata is bounded at 8 MiB.

The writer accepts a deliberately restricted JSON subset. It refuses custom
encoding hooks, embedded-field promotion and options beyond `omitempty`.
Exact `time.Time` is the bounded exception. The active Go 1.27.1 toolchain
enables JSONv2 by default even when `GOEXPERIMENT` is empty; review checked that
backend's scalar, byte-element, map-key and tag-option behavior. Fixtures test
refusal before encoder callbacks rather than depending on inactive legacy
tag parsing. Copied destination bytes/hash and source ownership are checked.

Scratch traversal measures regular logical file lengths with bounded batches,
depth, path bytes, entries and handles. Read/close, link/topology, overflow or
bound failures return no complete partial total. This primitive does not yet
establish backend applicability, its actual cleanup stage or run completion.
It proves neither peak disk usage nor durable database closure.

Final targeted replay-package checks passed 696 ordinary and 696 race cases,
zero skips, plus vet, Staticcheck and lint. The source-only reviewer and lead
dispositioned preflight and oracle findings before these final checks.
`v6-e1-primitives-checkpoint-v1.json`, SHA256
`bcc76e59f3433af81d91ab88b7c0fa8960f30508109bccafebdb7f5bb220c964`,
binds the final source and logs. This is an unwired intermediate unit; actual
capture, all-writer integration, the complete final E1 lane and E2 causality
results remain pending.

## Replay integration checkpoint

The bounded selection/journal/trace foundation and the replay caller/storage
integration are implemented. Native replay-package checks passed 915 ordinary
and 915 race cases, zero skips, with targeted vet, Staticcheck and lint passing.
The ordinary package result precedes a test-only Step oracle refinement; its
stronger processed-evidence/empty-queue assertion and three subcases separately
passed in the final source. A separate source-quality reviewer and the lead
found no remaining material integration issue. These are intermediate checks;
the complete selected lane and exposed captures remain required.

Capture occurs at existing initial/pipeline/release selectors, Observe and
Evaluate calls. It records explicit per-stage call counts, including zeros,
and actual confirmation/license/stabilizer/outer-learning results. Selected
records reserve one of their 64 detailed records for terminal execution.
Every Observe attempt is journaled against the original pending row; wanted
events join only after checked closure and complete counter reconciliation.
Errors are checked after the existing Step, drain batch or sample append.
No diagnostic error selects a different ordinary correction outcome.

All first-party artifact writers and both asset/license copy paths use the
shared cumulative owner. Its finite path/active-handle inventory verifies
closed, synced outputs and permits only exact preverified license-copy deletion.
The actual backend supplies scratch applicability/settings. Cleanup precedes
measurement, join and closed-output hashes; the report precedes COMPLETE-last.
Report/accounting stages exclude their own later bytes explicitly, for final
external reconciliation. Qualification readers refuse diagnostic marker
presence even when null, false or malformed.

Review corrected two test oracles: open-handle cleanup may make files hashable
without clearing the original failure, and enqueue acceptance does not prove
that Step consumed evidence. Separate preterminal trace-limit and actual frame
counts protect against silently omitted captures. The optional absent diagnostic
request retains its intentional nil/no-error meaning, documented with the same
narrow lint exception used for optional model loading.

`v6-e1-integration-checkpoint-v1.json`, SHA256
`dd56e7a529a47edc55ac0d4426860c34bc3e08ed99a438be0fb0d5a2d39dc4a3`,
binds source freeze v3 and the local logs. Capture plans/executables and empirical
parity/accounting proof are not implied by this checkpoint. No replacement
method or model is active.

The final combined E1 Go source passed the full native ordinary and race lanes:
each covered all 76 packages (61 executed, 15 build-only), with 14,786 passes
and 99 skips. Exact skip identities match retained E0 evidence. Common case
identities reconcile after only the existing source-proven peer clock-name and
distinct race/nonrace topology-test handling. Full vet, Staticcheck, lint,
code-map freshness and 162 local links in the seven changed Markdown files
passed. Source freeze v3 still matches every checked Go file. Final proof
`v6-e1-final-validation-v1.json` has SHA256
`71b133c0b3efdc58954dbeeace14d05cfda6d54b1b900af7334e30860f5323f4`.
These functional checks precede real exposed capture parity and accounting;
they establish no RF accuracy, model activation or production resource verdict.

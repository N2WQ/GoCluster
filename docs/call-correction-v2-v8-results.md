# Call Correction v2: v8 Development Results

The new selective C method and receiver-compatibility B method are coded and
verified, but **both fail the agreed development objective and frozen reference
guard**. Keep activation off. More corrections did not preserve the existing
follow-on stability rate on these data. B is more selective than C, without
establishing improvement over ordinary correction.

Authority: [approved v8](call-correction-v2-scope-ledger-v8.md). Native code freeze:
`cc225f08d5a64e78de98555e425a4d4fae8f4af0`; no refit. The full ordinary and race
lanes each passed 15,152 cases with 99 skips across 76 packages; vet,
Staticcheck and lint passed. [Validation and scope mapping](call-correction-v2-v8-validation.md).

## Complete Comparison

Eight actual captures completed: four disabled controls plus C/B on exposed
November 7/14. All four disabled streams match the original fixed SHA256/count
authorities byte for byte: 2,364,930 decisions. Each active method retains all
1,340,516 focal input IDs across its two development dates. Native evaluation
reconciles archive population, immutable IDs, source/executable/config/assets,
method receipts and final outcomes. The active dates were exposed and were
training inputs; they are not untouched confirmation or RF truth.

The unchanged stability definition requires three follow-on raw rows within
15 minutes and 500 Hz. Exact fractions, rather than rounded percentages, decide
the objective:

| Method | Applied receipts | Stable / applied | Stability | Net vs ordinary | Objective pass |
| --- | ---: | --- | ---: | ---: | --- |
| Ordinary | 22,076 | 16,587 / 22,076 | 75.135894% | — | baseline |
| C selective | 22,521 | 16,869 / 22,521 | 74.903423% | +445 | No |
| B receiver compatibility | 22,443 | 16,821 / 22,443 | 74.949873% | +367 | No |

C drops 0.232471 percentage points below ordinary; B drops 0.186021 points.
B's pooled rate is 0.046450 points above C, while applying 78 fewer corrections.
This is not an ordinary-baseline improvement. B versus C also has mixed daily
rates: lower on November 7 and higher on November 14.

| Date | Ordinary stable/applied | C stable/applied | C net | B stable/applied | B net |
| --- | --- | --- | ---: | --- | ---: |
| November 7 | 5,406 / 7,302 | 5,513 / 7,460 | +158 | 5,485 / 7,424 | +122 |
| November 14 | 11,181 / 14,774 | 11,356 / 15,061 | +287 | 11,336 / 15,019 | +245 |

## Baseline Changes And References

| Against ordinary | Added receipts | Lost receipts | Retargeted | Retimed | Affected baseline ID union | R1 new strong disagreeing renames | R2 new strong disagreeing renames |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| C | 446 | 1 | 0 | 0 | 1 | 119 | 48 |
| B | 367 | 0 | 0 | 0 | 0 | 108 | 48 |

B versus C adds one receipt and loses 79, for net -78 and an affected baseline
ID union of 79; it does not retarget or retime a common applied receipt. Its one
addition restores the ordinary baseline's stable N0TT → W0TT receipt on
November 7 (raw ID ending `:919179`), which C loses. Of the 79 C-only receipts
B removes, 49 satisfy the fixed stability proxy; removal is not automatically
evidence of better correction.

C has two new identity renames outside its declared CW/1–3 cells; B has none.
On November 7, IDs ending `:797324` and `:797469` change NJ8M → NJ8M/VO2 through
the ordinary route. Their underlying C decision records store configured
evaluation distance four; this is not a direct C proposal exceeding its cap.
Ordinary baseline gates reject recent-plus-one as
`subject_not_weaker`; B retains NJ8M and reports unavailable SNR for its
additional assessment. These final changes remain in the full denominator.
The paired receipts distinguish direct extra mutation from final outcomes, but
do not alone establish the precise causal history/state mechanism.

The RBN strong-reference disagreements are **not independently confirmed RF
errors**. They still fail the unchanged zero-new-contradiction guard. R1 and R2
also disagree with each other on 7,128 shared strong-label rows in this full
population. The evaluation preserves those disagreements and unknowns.

Among the 446 C additions, R1 is strong for 140 and R2 for 69; neither is strong
for 276. Among B's 367 additions, the corresponding counts are 126, 62 and 217.
These reference-covered populations overlap; the disagreement counts must not
be presented as a full-population RF error rate.

| Active cell | C jointly strong incremental episodes | B jointly strong incremental episodes | C R1/R2 coverage | B R1/R2 coverage |
| --- | ---: | ---: | --- | --- |
| CW/1 | 1 | 0 | 5.00% / 5.00% | 0.00% / 2.78% |
| CW/2 | 3 | 3 | 13.48% / 10.11% | 13.75% / 11.25% |
| CW/3 | 2 | 2 | 7.84% / 17.65% | 4.88% / 19.51% |

Every cell fails the retained 200-joint-episode minimum, 50 on each of two dates
and 80% coverage for each reference. The two higher-distance cells also have
new strong episode contradictions. Cell episode counts and whole-population
disagreeing-receipt counts are different units and must not be added together.
V2 explicitly leaves eligibility unassessed/false; no method is selected or
promoted by these development results.

Reference coverage is unchanged across the three predictor states:
620,645 R1-strong and 259,584 R2-strong rows out of 1,340,516. All unknowns:

| Unknown reason | R1 | R2 |
| --- | ---: | ---: |
| Cochannel | 188,900 | 386,742 |
| Insufficient families | 442,553 | 366,104 |
| Receiver conflict | 14,757 | 62,785 |
| Reference conflict | 72,170 | 258,662 |
| Unavailable context | 1,491 | 1,491 |
| Boundary | 0 | 5,148 |

Within each reference column these unknown reasons are disjoint: their sum plus
that reference's strong rows equals 1,340,516. R1 and R2 populations can overlap.
Conditional CTY representative geography does not reconstruct receiver sites,
and better RF labels cannot be inferred from the archive agreement calculation.

## What The Model Actually Did

The receipt audit is over additional-method invocations, not all input rows or
corrected stations. Ordinary successful corrections remain terminal.

| Diagnostic across both dates | C | B |
| --- | ---: | ---: |
| Additional assessment invocations | 1,088,426 | 1,088,427 |
| Invocations retaining a C nomination | 58,312 | 58,312 |
| Extra gate calls | 66,178 | 2,623 |
| Direct accepted extra mutations | 444 | 367 |
| Replacement validation rejections | 2 | 2 |

B scored all hypotheses successfully on 860,114 invocations. Its available
decisions chose KEEP 647,883 times, an ineligible opponent 209,589 times and an
eligible proposal 2,642 times. Temporal permission further withheld 19 selected
proposals. Its observed maximum was 216 Score calls; the guarded maximum-input
test/benchmark reaches the declared 432 bound.

Unavailable evidence remains visible: 73,981 invocations had unusable SNR on a
positive scoring observation, 50,893 had unavailable ordinal evidence and 565
exceeded the observed-identity limit. Other common association, basis, loss/
protected-state and lifecycle refusals retain their separate reasons. Model
availability does not mean eligibility, gate allowance or correctness.

C's direct accepted mutations differ from its final added/lost counts. The
predictors use existing history/temporal feedback and authoritative final
validation; precise causal attribution for each difference was not established.
The model performs real selection and declines additional rescues; it has not
demonstrated an acceptable final method here.
No thresholds or gates were retuned to turn this failed result into a pass.

## Artifact Bounds And Provenance

All captures completed under the 4,294,967,296-byte evidence/assets limit. Maximum
charged capture bytes were 3,174,224,335 (B, November 14). Scratch was reported
separately; the largest final logical scratch measurement was 51,382,346 bytes.
These are not peak disk, allocated blocks, RSS or durable-close guarantees.

Ignored artifacts under `.tmp/call-correctionv2/`:

- `v8-development-exposed-v1/FREEZE.json`: SHA256
  `b1a98f91d57e4be73f52ccb465e29bbb38c8e7c8a8a486fc8eb9337239abcf6f`.
- `v8-development-evaluation-plan-v1.json`: SHA256
  `bef9f1ea5eed4e376d9213d06d9271eaa478a4fef53562b45d6626ea34ac25ca`.
- `v8-development-evaluation-v1/COMPLETE.json`, `aggregates.json`,
  `paired_union.jsonl` and `applied_receipts.jsonl`.
- `v8-receipts-20261107.json`, `v8-receipts-20261114.json`, and the native
  evaluation, capture, full-lane and benchmark logs.

The fixed ordinal asset remains SHA256
`968caf263f3bf44c13509ded8b5e48faf0bd39b1d7cc42dcd9d1e6c0a45e5e1a`.
No November 17+ archive contents, names or hashes were opened. No model was
installed, and no deployment, activation, restart, merge or push occurred.

## Disposition

Retain the implemented default-off live C boundary and replay-only B for further
algorithm development. This study does not support enabling either method on
the production population. Existing release/reference/resource requirements
remain unchanged and unwaived. The inherited resource qualification failure was
not reclassified as a pass. More production data is a later evidence opportunity,
not evidence that these current failed checks succeeded.

# Call correction v10 replay evidence

The approved work used the November 7 and 14, 2026 development archives.
These dates were already exposed. The measurements describe the recorded RBN
population and replay outputs. Transmitted identity and performance on
held-back inputs remain unverified.

## Paired scoring

The new evaluator reproduced the frozen v8 pooled counts, reference guard
outcomes, paired decision stream hash, and applied receipt stream hash. For
added identity changes with an ordinary emitted call:

| Method | Focal row | Both stable | Ordinary only | Replacement only | Neither stable |
| --- | --- | ---: | ---: | ---: | ---: |
| C | Included | 129 | 125 | 142 | 36 |
| C | Excluded | 122 | 119 | 149 | 42 |
| B | Included | 96 | 102 | 127 | 29 |
| B | Excluded | 90 | 98 | 133 | 33 |

Both identities use the candidate decision time and the original input
frequency. The previous [v8 results](call-correction-v2-v8-results.md) retain
the release disposition. The supplementary report is in
`.tmp/v10/historical-v8/aggregates.json`.

## CW fallback comparison

The four captures used the same replay executable and the same disabled
correction method. The private CW table retained the original bytes and
appended 25 normalized reporter entries at `0.950`. The archived CW census
found 19,040 of 1,001,943 reports missing from both tables on November 7
and 2,878 of 1,379,162 on November 14. These are raw report counts.

The control was captured anew from integrated main. A direct check against
the archived v8 controls found matching terminal IDs in the same order.
The fresh November 7 control differed on 66 diagnostic rows, with identical
emitted identities. November 14 differed on 51 rows and retained
`TM140FHBK` once where the historical control emitted `TM140FH`.
Neither frozen reference had a strong label for that input. The reason for
the baseline drift remains undetermined. The weight comparison uses the
fresh control for both dates. The historical paired-scoring reproduction
uses the immutable v8 records.

| Date | Focal inputs | CW focal inputs | Applied corrections in each arm | Stable corrections in each arm | Retimed outputs |
| --- | ---: | ---: | ---: | ---: | ---: |
| November 7 | 458,495 | 457,336 | 7,302 | 5,406 | 2 |
| November 14 | 882,021 | 671,770 | 14,773 | 11,180 | 32 |
| Total | 1,340,516 | 1,129,106 | 22,075 | 16,586 | 34 |

Both arms emitted the same calls for the same input IDs. There were zero
added, lost, retargeted or retimed applied corrections. There were 17 changed
CW decision records on November 7 and 75 on November 14. These changes were in
decision reason, timing, or both. One November 14 CW record moved from an
ordinary same-call decision to a neighborhood conflict and still emitted its
original call. R1 and R2 coverage counts were equal between arms on each date.

November 7's candidate had nine more `P` and nine fewer `V` outputs.
November 14's candidate had four fewer `P` and four more `V` outputs.
These are changes in aggregate glyph counts. All other glyph counts matched.
The four captures completed below the 4 GiB per-run evidence limit. The
maximum charged bytes were 1,585,233,017.

The experiment gives no observed identity-correction benefit from changing
the CW fallback to `0.950`. Retain the existing live policy on this evidence.
The low observed exposure also limits what these two dates can say about
other operating periods.

The frozen comparison plan, capture receipts, and its hash-bound
`comparison.json` and `changed_decisions.jsonl` are under `.tmp/v10/`.
The capture executable was built from `d37d0f7e`. A separate join of both
capture pairs confirmed the identity, timing and decision-reason counts.

## Frequency slot boundary

The deterministic fixture shifts all reports in frequency using the shipped
400 Hz slot width and Morse comparison. With four reports for the proposed
call and two for the original, the same-slot winner survives selection. Across
the boundary, the original call is a nominee and the 50% ratio rule splits the
merged selection. With three reports against two, the exact-slot guard and the
neighborhood ratio each split their respective placement. In the distinct-
station fixture, the unrelated neighboring bucket cannot enter the subject's
neighborhood. The test exercises selection authority. Passing selection still
leaves the ordinary correction gates to evaluate.

The fixture is in
[`neighborhood_boundary_test.go`](../internal/correctionflow/neighborhood_boundary_test.go).

## Validation

The native Windows ordinary lane passed 15,395 cases across 76 packages
(62 executed, 14 build-only), with 100 reported skips. The focused checks
cover paired scoring boundaries, immutable-ID refusal, the real resolver
weight transition, asset parity, and exact versus neighborhood split rules.
Vet, Staticcheck, lint, and the crawler-entry and code-map checks passed.
The implementation and final evidence review were lead-owned.

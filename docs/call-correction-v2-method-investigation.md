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

## Frozen diagnostic inputs and remaining work

The capture selection freezes all 75 nearby misses and stratified controls
before E1 execution: 271/283/244/240 IDs for November 7/14/15/16, respectively,
1,038 total, below 512 per date. Categories include attained near and far
references, original matches, split/no-selected exports, mode, parsed zero and
negative SNR, aliases and incomplete references. This sample is not a population
recall or calibration denominator. November 17 contents remain unopened.

Local evidence is under `.tmp/call-correctionv2/`:

- `v5-e2-preliminary-accounting-v2.json`, SHA256
  `3fc844a60898cafe2f95a0dfa3b40e221dccee8732f09a78e2cfcd50edaee4af`.
- `v5-exposed-case-selection-v1.json`, SHA256
  `a96ca4cd501440b4f15fee4883306486b6850b0694215c3378a333f9faeab2fd`.

E1 is stopped at the [storage guarantee gap](call-correction-v2-scope-ledger-v5.md#post-approval-material-finding-e1-storage-ceiling).
No shared diagnostic interfaces, candidate generator, fitted model or runtime
behavior change has been implemented under v5. The owner selected the revised
budget policy; its [v6 amendment](call-correction-v2-scope-ledger-v6.md) still needs
exact approval before the retained-versus-history study and a defensible E3
method recommendation.

# Support Card: Confidence Glyph Meaning

## Match

Use when a telnet user asks what `?`, `S`, `P`, `V`, `C`, or `B` means in a spot
line, or whether a confidence field is a probability.

## First Safe Check

Route to GoCluster confidence documentation and current command/help docs. Do
not infer meanings from other cluster software.

## Must Include

- Explain only the documented GoCluster confidence meaning.
- For `P`, say it is a confidence glyph, not an external-cluster field or a
  probability.
- If the user asks about current HELP output, retrieve `commands/README.md`.
- Resolver evidence percentages measure a candidate's distinct receiver support
  share, using reliability weights when available. Repeated neighborhood
  buckets do not add receivers; losing candidates still contribute dissent.
- Support shares are not calibrated probabilities. Current key/window evidence
  does not establish that receivers heard the same transmission episode.
- Invalid applicable configured reliability/model assets fail startup or offline
  evaluation with the responsible key/path. Nonfinite weights, negative model
  counts, invalid edges and overflowing totals are refused. Weight zero still
  requires a valid enabled model; disabled models ignore their path. Valid finite
  inputs retain existing clamps/ranking, without proving scientific validity.
- Percentages describe the emitted call. A rejected candidate leaves `B` and
  the retained call's own weighted share; no eligible support displays `--%`
  in DIAG CONF. Accepted `C` output carries the replacement's share.
- Split protection survives neighborhood and temporal selection. The narrow
  guard uses the closest eligible frequency pair; conflicting equal-time
  frequency metadata causes conservative abstention until superseded or expired.
- Temporal release uses current eligible timestamped evidence; saved counts
  cannot authorize correction. Proposed, accepted identity changes and attempted
  handoffs are separate counts. A handoff attempt is not a client receipt.
- FT2/FT4/FT8 bursts end at the earlier quiet-gap/hard deadline. Equality and
  later arrivals start a new burst and cannot raise an expired burst's glyph.
  Existing recent-band/static S floors remain; FT glyphs have no resolver percentage.
- Custom SCP historical corroboration counts each receiver only when its own
  newest stored observation meets the applicable age-score requirement and is
  not future at the query's stored second. Geographic cells count only from
  those qualifying observations. A fresh receiver does not refresh older
  receiver/cell evidence.
- Resolver and stabilizer use separate age-score/cell requirements on one
  canonical history store. Receiver qualification applies the consumer minimum
  alongside the common minimum; plus1/edit-neighbor add their own rails. Partial
  eligible counts remain visible in Bayesian diagnostics. Stabilizer settings
  do not silently raise resolver or FT requirements. The custom S floor retains
  its dedicated thresholds.
- The custom S floor checks exact support first, then independently qualifies
  each relevant family member against all family requirements. Any complete
  member success suffices; counts/cells are never pooled. A higher-scoring failure
  cannot hide another success. Static recognition is separate, and output family
  suppression is unchanged by this historical lookup rule.
- Enabled historical receiver/cell gates must fit
  `call_correction.custom_scp.max_spotters_per_key`. Impossible consumed gates
  fail configuration/runtime/replay initialization; direct boolean queries
  return false rather than weakening the request. Disabled unused gates and the
  separate FT store do not inherit this cap. Partial count queries remain visible.
- Future learned static membership is retained but cannot
  supply recognition or the S floor before its stored second. Eligible static
  counts differ from retained cardinality; independent history can still qualify.
- Retained observations and currently qualifying historical support are different
  populations. Score/future filtering does not delete within-horizon history;
  existing horizon pruning and cleanup remain. Retained counts alone do not prove
  qualification. Historical/static recognition is also separate from current
  resolver consensus and is not a calibrated correctness probability. See
  [historical freshness](../../spot/README.md#historical-receiver-freshness).

## Must Avoid

- Do not claim compatibility with DXSpider or other cluster software unless
  GoCluster docs explicitly say so.
- Do not invent numeric probability semantics.

## Sources

- `customgpt/troubleshooting-index.md`
- `README.md`
- `spot/README.md`
- `commands/README.md`

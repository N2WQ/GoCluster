# ADR-0176: Path Prediction Admission Reuse

- Status: Accepted
- Date: 2026-06-11
- Decision Origin: Design

## Context

PATH filtering and PATH glyph/diagnostic formatting both need the same
per-client path prediction. Before this decision, broadcast admission computed
only a path class for active PATH filters, while display formatting recomputed a
full prediction later for glyphs or `SET DIAG PATH`.

That doubled predictor cache reads and fallback checks for clients that both
filter by PATH and display PATH output. It also made operator counters such as
VOACAP p50 comparison and sparse p50/VOACAP diagnostics more vulnerable to
counting the admission lookup and display lookup as separate samples.

## Decision

When a broadcast client has active PATH filtering and the filter is not
`REJECT PATH ALL`, admission computes one full `pathPrediction`, derives the
filter class from it, and attaches the prediction to the accepted
`spotEnvelope`.

Writer formatting may reuse the envelope prediction only after revalidating the
client/spot inputs that affect prediction:

- user grid and encoded user cell
- DX grid and encoded DX cell
- band and mode
- noise class
- effective path observation floor
- beacon versus non-beacon prediction mode
- same current UTC hour and a very short admission-to-formatting age

If any revalidation check fails, formatting recomputes the prediction.

Admission prediction compute may keep the existing fallback/cache side effects
that are required to classify the spot. Final display accounting remains owned
by formatting:

- `Path predictions (5m)`
- sparse p50/VOACAP diagnostic counters
- solar override counters

The no-PATH-filter fast path must not compute or attach a path prediction during
broadcast admission. Queue drops must not record final display counters because
the spot was never formatted.

## Alternatives considered

1. Keep separate admission and display lookups.
   - Rejected because it repeats predictor/cache work for PATH-filtered clients
     and can double-sample fallback comparison side effects.
2. Always compute and attach predictions for every accepted spot.
   - Rejected because clients without PATH filters would pay admission CPU,
     heap, and allocation cost even when only normal formatting is needed.
3. Attach only the path class from admission.
   - Rejected because display would still need a full prediction to produce
     glyphs and diagnostics, preserving the duplicate expensive lookup.

## Consequences

### Benefits

- PATH-filtered clients with PATH display or `SET DIAG PATH` can reuse the
  admission prediction during formatting.
- No-PATH-filter clients keep their admission fast path.
- Display counters remain tied to formatted spots, so queue drops do not inflate
  `Path predictions (5m)`.
- Fallback and p50/VOACAP comparison counters are less likely to count both the
  admission lookup and the display lookup for the same accepted spot.

### Risks

- `spotEnvelope` now has an optional pointer field for admission predictions.
  The pointer is populated only for PATH-filtered accepted spots and remains
  bounded by the existing per-client spot queue.
- Reuse depends on conservative revalidation. Some accepted spots will still
  recompute if client state changes between admission and formatting.
- Admission fallback/cache side effects still occur for PATH filtering even if
  the spot is later dropped before formatting. That preserves pre-existing
  classification behavior but keeps a distinction from final display counters.

### Operational impact

- No telnet command, YAML, protocol, glyph, or diagnostic token changes.
- Operators should see fewer duplicate p50/VOACAP compare samples for clients
  that use PATH filters plus PATH display/diagnostics.
- Slow clients that drop queued spots still do not contribute final display
  prediction counters.

## Links

- Related issues/PRs/commits: current p50/VOACAP comparison branch
- Related tests: `telnet/server_path_prediction_reuse_test.go`
- Related benchmarks: `BenchmarkPathPredictionEnvelopeNoFilterDisplay`,
  `BenchmarkPathPredictionEnvelopePathFilterDisplay`,
  `BenchmarkPathPredictionEnvelopePathFilterNoDisplay`
- Related docs: `docs/decisions/ADR-0168-voacap-p50-cache-comparison.md`,
  `docs/decisions/ADR-0175-sparse-p50-voacap-outcome-diagnostics.md`
- Related TSRs: TSR-0025, TSR-0028, TSR-0029
- Supersedes / superseded by: none

# ADR-0132: Path Receiver Cap Shadow Candidates

- Status: Accepted
- Date: 2026-05-10
- Decision Origin: Design

## Context

Receiver contribution caps now use decayed effective count semantics, but the
right `receiver_max_effective_count` value still needs live evidence. Iterating
one cap value per run is slow because traffic mix, band conditions, and user
prediction volume change between runs.

The operator needs a defensible way to compare a small set of candidate count
caps under identical live traffic without changing active glyph behavior and
without adding unbounded retained state.

## Decision

Add `receiver_shadow_max_effective_counts` to `path_reliability.yaml`. The key
is required, must contain exactly three positive strictly increasing values, and
is used only for receiver-cap shadow diagnostics.

Each candidate has an independent gate-only capped lane that tracks decayed
effective count and weight through the same fine/coarse and receive/transmit
sample-selection path as active predictions. Candidate lanes do not retain SNR
histograms and do not compute alternate p50 values or alternate glyphs.
ADR-0133 extends this initial gate-only design with optional candidate p50
histograms.

When `receiver_contribution_mode` is `shadow`, five-minute propagation logs add
a separate `Path cap shadow (5m)` line with `capN_pass`, `capN_low_count`,
`capN_low_weight`, and `capN_block` fields for each configured candidate. The
existing `Path predictions (5m)` line remains backward compatible.

Candidate retained state is allocated behind the existing optional bucket
sidecar so the base bucket size does not grow for raw buckets.

## Alternatives considered

1. Re-run the node once per candidate cap. Rejected because changing traffic and
   propagation conditions make the runs hard to compare.
2. Simulate candidate caps from the existing active capped lane. Rejected
   because slot replacement and per-receiver admission can diverge by count cap.
3. Track candidate p50 histograms and glyphs. Rejected for this scope because
   the current tuning question is gate behavior, and per-candidate histograms
   would multiply retained state and hot-path work.

## Consequences

### Benefits

- Three cap values can be compared under the same live prediction traffic.
- Active glyph and PATH-filter behavior is unchanged.
- Low-count and low-weight candidate failures are separated, making cap tuning
  more interpretable.
- The existing prediction aggregate remains compatible with older parsers.

### Risks

- The shadow line answers gate behavior only; it does not say what each
  candidate's alternate p50 or glyph would have been.
- Candidate lanes add per-update work when receiver-cap tracking is enabled.
- Operators must remember that `capN_block` is meaningful only in `shadow`
  mode, where raw selected evidence is still the active display path.

### Operational impact

- Startup now requires `receiver_shadow_max_effective_counts` in
  `path_reliability.yaml`.
- The daily propagation report parser understands and summarizes
  `Path cap shadow (5m)` lines.
- Slow-client handling, broadcast queues, peer wire format, archive format,
  telnet command syntax, and shutdown behavior are unchanged.

## Links

- Related issues/PRs/commits:
- Related tests: `pathreliability/config_test.go`,
  `pathreliability/receiver_test.go`, `pathreliability/snr_histogram_test.go`,
  `telnet/server_prediction_stats_test.go`,
  `internal/propreport/report_test.go`
- Related docs: `data/config/path_reliability.yaml`,
  `pathreliability/README.md`, `README.md`, `docs/OPERATOR_GUIDE.md`,
  `data/config/PATH_PREDICTIONS.md`, `customgpt/common-questions.md`
- Related TSRs:
- Supersedes / superseded by: extends ADR-0095, ADR-0129, and ADR-0130; extended
  by ADR-0133; does not supersede them.

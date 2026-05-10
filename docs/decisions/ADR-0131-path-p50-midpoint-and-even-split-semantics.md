# ADR-0131: Path P50 Midpoint And Even-Split Semantics

- Status: Accepted
- Date: 2026-05-10
- Decision Origin: Troubleshooting chat

## Context

Active path reliability uses fixed-bin p50 scoring for glyphs and PATH filters.
The original active p50 implementation returned each selected bin's lower edge
and chose the weaker bin whenever cumulative histogram weight landed exactly on
the 50% boundary. That preserved bounded retained state, but it made balanced
weak/strong evidence look weaker than a typical middle-path interpretation.

The path glyph is intended to summarize the typical selected path experience,
not the weakest half-boundary value. The method must keep fixed bucket storage
and must not retain exact observations.

## Decision

Finite SNR histogram bins use their midpoint as the p50 representative. The
underflow and overflow bins remain clamped to `-24` and `24`.

When p50 lands exactly on a median boundary and a stronger non-empty bin exists,
the returned p50 is the average of the selected bin representative and the next
stronger non-empty bin representative. Otherwise p50 returns the selected bin
representative.

Receiver-cap enforcement, fine/coarse selection, receive/transmit merging,
noise penalties, observation floors, weight floors, and fixed histogram
retention remain unchanged.

## Alternatives considered

1. Keep lower-edge p50. Rejected because it systematically biases finite bins
   downward and makes exact balanced bimodal evidence choose the weaker bin.
2. Store raw observations and calculate exact medians. Rejected because retained
   memory and prediction work would no longer be bounded by fixed bucket state.
3. Interpolate within every selected bin. Rejected because the store does not
   retain intra-bin distributions; midpoint representatives are the bounded,
   deterministic approximation the retained data can support.

## Consequences

### Benefits

- Finite-bin p50 no longer rounds every selected bin down to the weaker edge.
- Exact even splits and balanced bimodal paths report the typical middle between
  the two boundary populations.
- Retained bucket size and update-path allocation behavior are unchanged.

### Risks

- Some balanced weak/strong paths can move from LOW to MEDIUM when the midpoint
  average crosses a threshold.
- Diagnostic p50 values can differ from historical lower-edge values even when
  the glyph class is unchanged.
- Exact splits between clamped underflow and overflow bins average the clamped
  representatives; this is bounded but can look optimistic for maximally
  bimodal evidence.
- Near-50/50 but not exact bimodal distributions still choose the dominant side;
  smoothing those cases would require a different algorithm.

### Operational impact

- No config keys or telnet command syntax change.
- Normal path glyphs and PATH filters can become less conservative for exact
  median-boundary cases.
- `SET DIAG PATH` remains the operator tool for inspecting selected count,
  weight, age, and cap effects.

## Links

- Related issues/PRs/commits:
- Related tests: `pathreliability/snr_histogram_test.go`,
  `pathreliability/receiver_test.go`, `pathreliability/store_bench_test.go`
- Related docs: `README.md`, `pathreliability/README.md`,
  `data/config/PATH_PREDICTIONS.md`, `customgpt/troubleshooting-index.md`
- Related TSRs:
- Supersedes / superseded by: Supersedes the lower-edge p50 representative and
  exact-boundary behavior in ADR-0126.

# ADR-0169: Bidirectional Noise-Aware VOACAP Fallback

- Status: Accepted
- Date: 2026-06-10
- Decision Origin: Scope Ledger v1

## Context

The p50 bucket predictor is directional: DX-to-user receive evidence is blended
with user-to-DX transmit evidence, and the user's `SET NOISE` class subtracts a
receive-side penalty from the DX-to-user leg. The VOACAP fallback previously
ran one user-to-DX deck and evaluated the cached SNR without the user's receive
noise penalty. That made sparse-path fallback decisions semantically different
from the p50 method they corroborate.

ADR-0165 intentionally keeps the VOACAP deck at a quiet baseline so local noise
is applied by GoCluster rather than encoded in the deck. The missing piece was
applying that receive-side penalty to fallback decisions and comparing a
bidirectional VOACAP path against sparse or sufficient p50.

## Decision

For each logical VOACAP fallback job, run two directed Method-30 decks:

1. DX-to-user, used as the receive leg.
2. User-to-DX, used as the transmit leg.

Cache the raw hourly records for both directions in one logical fallback cache
entry. Cache keys remain independent of user noise class because noise is
request-specific presentation policy, not model output.

At lookup time, compute the effective VOACAP SNR with the same configured
receive/transmit weights used by p50:

```text
effective = merge_receive_weight * (DX_to_user - receive_noise_penalty)
          + merge_transmit_weight * user_to_DX
```

Use this effective SNR for:

- VOACAP closed fallback threshold checks.
- VOACAP-aligned sparse p50 class comparison.
- Opportunistic sufficient-p50 cache comparison.
- `vcap` and `valn` diagnostics, rounded for fixed-width display.

This is a weighted dB blend of two directed VOACAP median forecasts. It is not
an exact weighted-median histogram calculation; using exact weighted median
semantics with current `0.6/0.4` weights would mostly select the receive leg and
make the transmit VOACAP run unhelpful.

## Alternatives considered

1. Keep single-direction user-to-DX VOACAP fallback and only add receive noise.
   - Rejected because there is no receive leg to apply that penalty to, and the
     fallback would still disagree with p50's bidirectional model.
2. Run both directions and use an exact weighted median.
   - Rejected because with the current receive weight above 0.5, the effective
     value would usually collapse to the receive-direction SNR.
3. Add new YAML weights specifically for VOACAP.
   - Rejected for this slice because the requested behavior is to use the same
     blending ratio as p50, not introduce a second calibration surface.

## Consequences

### Benefits

- Aligns VOACAP fallback direction and noise semantics with p50.
- Keeps sufficient bucket p50 authoritative.
- Keeps VOACAP raw cache reusable across users with different noise classes.
- Makes `VOACAP p50 compare (5m)` compare p50 against the same effective
  fallback value that display/filter decisions would use.

### Risks

- Each logical fallback job runs two VOACAP processes sequentially, so external
  process time and queue pressure can roughly double for cache misses.
- The compact `vcap` and `valn` diagnostics show only rounded effective SNR,
  not the two raw directional SNRs.
- Weighted dB blending is a model calibration choice; future field evidence may
  justify a different VOACAP-specific blend or reliability-based comparison.

### Operational impact

- Runtime fallback remains delayed, nonblocking, queue-bounded, and cache
  bounded.
- Existing fallback stage counters retain their meanings; `RunSuccess` counts a
  complete bidirectional job.
- Closed fallback and aligned sparse-p50 glyphs may become more conservative
  for noisy users because their receive-side noise class now applies to VOACAP.
- VOACAP output files now include receive/transmit direction suffixes for each
  logical fallback job.

## Links

- Related code: `pathreliability/voacap_fallback.go`, `telnet/server.go`
- Related config: `data/config/path_reliability.yaml`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `customgpt/support-cards/path-reliability.md`
- Related tests: `pathreliability/voacap_fallback_test.go`,
  `telnet/path_settings_test.go`, `telnet/server_prediction_stats_test.go`
- Related ADRs: ADR-0161, ADR-0162, ADR-0163, ADR-0165, ADR-0168
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0161 and ADR-0163 fallback
  semantics, ADR-0162 cache record semantics, ADR-0165 GoCluster-side noise
  application, and ADR-0168 comparison semantics

# ADR-0165: VOACAP Ham Median Card Template

- Status: Accepted
- Date: 2026-06-09
- Decision Origin: Design

## Context

The production VOACAP fallback builds a fixed Method-30 input deck in Go. The
deck used a residential-noise, near-zero-angle, 90% required-reliability
`SYSTEM` card while GoCluster separately applies receive-side noise penalties
and compares VOACAP median SNR against the same p50-style class thresholds used
by path reliability.

The fixed card should match that architecture instead of double-counting local
noise or implying that the fallback is based on 90% reliability.

## Decision

Keep the VOACAP deck template fixed in code rather than exposing new YAML knobs.

Continue using:

```text
COEFFS    CCIR
FPROB      1.00 1.00 1.00 0.00
METHOD       30    0
```

Change the `SYSTEM` card to:

```text
SYSTEM       1. 153. 3.00  50. 10.0 3.00 0.10
```

The value `153` is the quiet-noise baseline; user noise class remains a
GoCluster-side receive penalty. The `3.00` degree takeoff angle is the generic
ham-radio baseline. Required reliability is `50` so VOACAP's reliability
settings align with the median/p50 SNR surface that GoCluster parses from the
Method-30 `SNR` row.

## Alternatives considered

1. Keep the previous `SYSTEM` line.
   - Rejected because it mixed residential noise with GoCluster's own noise
     offsets and retained a 90% reliability target while the fallback compares
     median SNR classes.
2. Expose the fields as YAML knobs.
   - Rejected for this slice because the values are model calibration, not
     routine operator policy.
3. Use an extreme noise-free baseline.
   - Rejected because it would make VOACAP too optimistic for a generic ham
     fallback.

## Consequences

### Benefits

- Avoids double-counting receive-side noise.
- Makes VOACAP deck assumptions match p50 median class comparisons.
- Uses a more defensible generic ham-radio takeoff angle.

### Risks

- VOACAP fallback SNRs may shift relative to previous output files.
- The `REQ.SNR` field remains `10.0`; current glyph behavior parses median
  `SNR`, but future reliability-based logic would need a separate decision.

### Operational impact

Existing sufficient bucket p50 evidence remains authoritative. When bucket
evidence is insufficient, cached VOACAP fallback output may classify some paths
differently because the generated deck now uses quiet baseline noise, a
3-degree takeoff angle, and 50% required reliability.

## Links

- Related code: `internal/voacap/deck.go`
- Related tests: `internal/voacap/forecast_state_test.go`
- Related testdata: `internal/voacap/testdata/voacapx.dat`
- Related ADRs: ADR-0160, ADR-0161, ADR-0163
- Related TSRs: none

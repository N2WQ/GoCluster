# ADR-0160: VOACAP FT8 SNR Output Contract

- Status: Accepted
- Date: 2026-06-09
- Decision Origin: Design

## Context

The VOACAP forecast experiment now needs structured output that can later feed
the same p50 FT8 SNR reliability surface used by the current path-reliability
method. VOACAP method-30 output reports `SNR` in dB-Hz for 50% availability,
while WSJT-X FT8 reports use dB relative to a 2500 Hz reference bandwidth.

The current slice remains experiment-only. It should make the conversion
deterministic without introducing production cache schema, path-reliability
fallback behavior, H3 endpoint generation, or blending policy.

## Decision

Parse VOACAP method-30 `FREQ`, `SNR`, and optional `REL` rows into
`internal/voacap` prediction records. Skip VOACAP's leading best-frequency
column and emit one record per positive configured frequency cell.

Convert each VOACAP SNR value to integer FT8-equivalent SNR as:

```text
ft8_snr_db = round(voacap_snr_dbhz - 10 * log10(2500))
```

The experiment command prints the parsed prediction records after a successful
forecast. A forecast is treated as usable only after VOACAP runs successfully
and the output parses successfully.

## Alternatives considered

1. Store floating-point FT8-equivalent SNR.
   - Rejected for the experiment output contract because later bucket storage
     is expected to use one-dB bins and integer SNR keeps the surface compact.
2. Use a hard-coded subtract-34 conversion.
   - Rejected in code because computing `10 * log10(2500)` records the actual
     reference-bandwidth contract while producing the same rounded integer
     result.
3. Parse and emit VOACAP reliability as the primary score.
   - Rejected because the desired downstream substitute is p50 FT8 SNR. `REL`
     remains optional context only.

## Consequences

### Benefits

- Produces deterministic one-dB FT8-equivalent SNR values for each parsed
  forecast hour and center frequency.
- Keeps future storage compact by avoiding floating-point SNR output.
- Prevents the forecast state baseline from advancing when output exists but
  cannot be parsed into usable prediction records.

### Risks

- VOACAP prints frequency values with its own display precision, so parsed
  frequencies may be rounded relative to the YAML center-frequency input.
- The parser is method-30 specific and intentionally fails on mismatched
  frequency/SNR/REL row shapes.
- Calibration against observed FT8 reports remains a later decision.

### Operational impact

None for the live cluster. The output contract is used only by the VOACAP
experiment command and `internal/voacap` tests in this slice.

## Links

- Related code: `internal/voacap/output.go`,
  `cmd/voacap_ssn_forecast_watch/main.go`
- Related tests: `internal/voacap/output_test.go`
- Related ADRs: ADR-0157, ADR-0158, ADR-0159, ADR-0126
- Related TSRs: none

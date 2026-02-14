# ADR-0005: Mode-Specific Spotter Reliability Selection for Call Correction

- Status: Accepted
- Date: 2026-02-13

## Context

Call correction currently supports a single global spotter-reliability table. The
new RBN prior bundle provides separate reliability files for CW and RTTY, and
the operator requirement is to apply mode-specific reliability immediately.

Using one global table for both modes can mis-weight reporters because decoder
quality and operating conditions differ between CW and RTTY. This can increase
false corroboration in one mode and suppress valid corroboration in the other.

## Decision

Adopt mode-specific reliability selection in the correction hot path:

1. Add optional config inputs:
   - `call_correction.spotter_reliability_file_cw`
   - `call_correction.spotter_reliability_file_rtty`
2. Load three reliability maps at startup:
   - global fallback map
   - CW map
   - RTTY map
3. In correction reporter filtering:
   - for CW spots, use CW map when present; else global
   - for RTTY spots, use RTTY map when present; else global
   - for all other modes, use global map
4. Keep `min_spotter_reliability` semantics unchanged.
5. Keep behavior deterministic and bounded:
   - all lookups are in-memory map reads
   - no new unbounded caches

## Alternatives considered

1. Keep a single global reliability file.
- Rejected: loses mode-specific signal quality and conflicts with operator intent.

2. Merge CW/RTTY files offline into one weighted global file.
- Rejected: simplifies runtime but discards per-mode distinctions we need.

3. Infer mode-specific reliability from confusion model only.
- Rejected: confusion statistics help candidate scoring but do not replace per-reporter reliability filtering.

## Consequences

### Benefits

- Better reliability gating fidelity for CW/RTTY corrections.
- Immediate use of the generated prior bundle without format conversion.
- Backward-compatible fallback when mode-specific files are absent.

### Risks

- Misconfigured file paths can silently reduce to global behavior; startup logs now
  need to be monitored for per-mode load success.
- Memory increases slightly from maintaining two extra maps.

### Operational impact

- No wire/protocol changes.
- Operator-visible config surface increases by two optional fields.
- Correction outcomes may differ by mode due to mode-specific reporter filtering.

## Links

- Code:
  - `config/config.go`
  - `main.go`
  - `spot/reliability.go`
  - `spot/correction.go`
- Tests:
  - `main_test.go`
  - `spot/correction_test.go`
- Config/docs:
  - `data/config/pipeline.yaml`
  - `README.md`
  - `docs/decision-log.md`

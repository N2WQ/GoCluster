# ADR-0031 - Skew Selection Uses Absolute Skew Threshold

Status: Accepted  
Date: 2026-02-26  
Decision Makers: cluster maintainers  
Technical Area: config, skew, main, cmd/rbnskewfetch  
Decision Origin: Design  
Troubleshooting Record(s): none  
Tags: skew, configuration, ingestion

## Context
- The skew correction table previously selected entries by reported spot count (`min_spots`).
- Product intent is to select entries by skew magnitude regardless of spot-count metadata.
- Selection rules must remain deterministic and operator-configurable from YAML.

## Decision
- Replace `skew.min_spots` with `skew.min_abs_skew`.
- Keep an entry only when `abs(entry.SkewHz) >= min_abs_skew`.
- Remove all filtering based on `correction_factor == 1`.
- Default `min_abs_skew` to `1` when omitted or non-positive in config normalization.

## Alternatives Considered
1. Keep `min_spots` as primary selector
   - Pros: uses source confidence proxy.
   - Cons: does not match required behavior.
2. Require both `min_spots` and `min_abs_skew`
   - Pros: tighter gating.
   - Cons: extra operator complexity and conflicting intent.
3. Hard-code threshold and remove knob
   - Pros: simpler config.
   - Cons: removes operator control.

## Consequences
- Positive outcomes:
  - Selection now aligns directly with requested policy.
  - Config surface matches runtime behavior (`min_abs_skew` only).
- Negative outcomes / risks:
  - Entry set may differ materially from prior deployments that tuned `min_spots`.
  - Existing configs using only `min_spots` will no longer influence selection.
- Operational impact:
  - Operators tune one threshold based on absolute skew value.
  - Startup/refresh logs now report `min_abs_skew`.
- Follow-up work required:
  - None required for this change set.

## Validation
- Updated unit tests for skew filtering and CSV parsing behavior.
- Updated config tests for `min_abs_skew` default and override.
- Repository checks passed: `go test ./...`, `go vet ./...`, `staticcheck ./...`.

## Rollout and Reversal
- Rollout plan:
  - Deploy with `skew.min_abs_skew` set in YAML (default is `1`).
- Backward compatibility impact:
  - `skew.min_spots` is no longer used.
- Reversal plan:
  - Reintroduce `min_spots` gating in `skew.FilterEntries` and config wiring if needed.

## References
- Issue(s): none
- PR(s): none
- Commit(s): pending
- Related ADR(s): ADR-0017 (scheduler mechanics unaffected)
- Troubleshooting Record(s): none
- Docs: `README.md`, `data/config/data.yaml`

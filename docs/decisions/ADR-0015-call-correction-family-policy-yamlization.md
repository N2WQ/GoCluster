# ADR-0015 - YAML-Driven Call-Correction Family Policy Knobs

Status: Accepted
Date: 2026-02-23
Decision Makers: Cluster maintainers
Technical Area: config, spot/correction, main/telnet fan-out
Tags: call-correction, configuration, determinism, operations

## Context
- ADR-0014 introduced slash/truncation family handling and telnet family suppression.
- Several family-policy controls remained hardcoded in algorithm/runtime paths:
  - truncation containment constraints (`length delta == 1`, `shorter length >= 3`, prefix/suffix matching),
  - truncation advantage relaxation target (`min_advantage -> 0`) and validation rails,
  - telnet family suppressor fallback bounds.
- Operators requested full YAML control so policy tuning is explicit and reviewable in one section, without source edits.

## Decision
1. Add `call_correction.family_policy` as the canonical configuration section for family behavior:
   - `family_policy.slash_precedence_min_reports`
   - `family_policy.truncation.*`
   - `family_policy.truncation.relax_advantage.*`
   - `family_policy.telnet_suppression.*`
2. Wire correction runtime to consume the configured family policy directly:
   - truncation family detection now uses policy fields (enabled, length delta, shorter-length floor, prefix/suffix toggles),
   - truncation advantage relaxation now uses policy fields (enabled, effective min advantage, validation requirements).
3. Wire telnet family suppressor runtime bounds from YAML:
   - suppression enable flag,
   - cache window and max entries,
   - frequency tolerance fallback.
4. Keep backward compatibility for legacy `call_correction.slash_precedence_min_reports` by mapping it into `family_policy` when the new key is absent.

## Alternatives Considered
1. Keep hardcoded truncation/telnet defaults in runtime logic
   - Pros:
     - Fewer config fields.
   - Cons:
     - Operators cannot tune behavior without code changes.
2. Expose only slash threshold in YAML and leave truncation/telnet internals fixed
   - Pros:
     - Smaller config surface.
   - Cons:
     - Does not satisfy operational requirement for full policy control.
3. Remove legacy key immediately (no compatibility mapping)
   - Pros:
     - Cleaner schema.
   - Cons:
     - Avoidable config breakage for existing deployments.

## Consequences
- Benefits:
  - Family-policy behavior is transparent and operator-tunable in one YAML block.
  - Runtime behavior is deterministic from config, not implicit constants.
  - Telnet suppression bounds are explicit and bounded by configuration.
- Risks:
  - Larger config surface can be misconfigured.
- Mitigations:
  - Defaults/clamps are applied during config normalization.
  - Legacy slash key remains supported and mirrored.
  - Unit tests cover defaults and override precedence.

## Links
- Related ADRs:
  - `docs/decisions/ADR-0014-call-correction-family-policy.md`
- Code:
  - `config/config.go`
  - `spot/correction.go`
  - `main.go`
  - `telnet_family_suppressor.go`
- Tests:
  - `config/call_correction_stabilizer_test.go`
  - `spot/correction_test.go`
  - `main_test.go`
  - `telnet_family_suppressor_test.go`
- Config/docs:
  - `data/config/pipeline.yaml`
  - `README.md`


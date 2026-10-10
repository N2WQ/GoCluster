# ADR-0052 - Region-Aware Final Mode Inference and UNKNOWN Filter Token

Status: Accepted
Date: 2026-03-07
Decision Makers: Maintainers
Technical Area: spot, main output pipeline, filter, telnet, docs
Decision Origin: Design
Troubleshooting Record(s): none
Tags: mode-inference, iaru-region, filter-contract, observability

## Context
- ADR-0051 replaced the old within-band heuristic with a whole-band default label, but that still over-applied `USB/LSB` or `CW` where regional band plans are mixed.
- Operators want the final frequency-based step to use IARU regional context, not a single global band default.
- The final step must remain product-policy output, not trusted evidence, so regional outcomes must not seed the reusable DX or digital caches.
- Some frequencies remain too ambiguous even with region context. In those cases the telnet mode field must be blank, while mode filters still need deterministic allow/block semantics.

## Decision
- Replace the ADR-0051 final band-default table with a region-aware final classifier driven by two repo-owned tables:
  - `data/config/iaru_regions.yaml` maps DXCC/ADIF to `R1/R2/R3`.
  - `data/config/iaru_mode_inference.yaml` maps `region + band + frequency range` to `cw_safe`, `voice_default`, or `mixed`.
- Store the resolved DX IARU region on spot metadata so downstream stages do not repeat the lookup.
- Hydrate DX metadata before shared output-stage mode assignment when a spot has no explicit mode and still needs final classification.
- Final classifier semantics are:
  - `cw_safe` emits `CW`,
  - `voice_default` emits `USB/LSB` by explicit band-sideband policy,
  - `mixed` emits no mode.
- Unknown-region behavior is conservative:
  - only cross-region `cw_safe` intersections may emit `CW`,
  - unknown region never emits `voice_default`.
- Keep `60m` and `6m` effectively mixed for product behavior.
- Blank final outcomes keep `Spot.Mode` empty and the telnet mode field blank.
- Telnet/filter control-plane semantics treat blank-mode spots as `UNKNOWN`:
  - `PASS/REJECT MODE UNKNOWN` is supported,
  - default mode selections include `UNKNOWN`,
  - legacy saved explicit mode allowlists are migrated at load time to include `UNKNOWN` unless the user explicitly blocks it,
  - `PASS MODE ALL` allows blank-mode spots.
- Provenance is extended to distinguish `regional_cw`, `regional_voice_default`, `regional_mixed_blank`, and `regional_unknown_blank`.
- Only trusted evidence classes remain reusable. Regional outcomes, including emitted `CW/USB/LSB`, must not seed reusable inference state.
- Do not add IARU region as a persisted gridstore field in v1. Derive it from CTY/DXCC metadata already carried in memory/persistence.

Documentation correction, 2026-10-10: the earlier opening recipe named
unsupported `RESET MODE`. Use `PASS MODE ALL`. This corrects command guidance;
the accepted inference and UNKNOWN policies above remain unchanged.

## Alternatives Considered

1. Keep the ADR-0051 whole-band default table
   - Pros:
   - Minimal additional metadata/config surface.
   - Keeps all ambiguous spots labeled.
   - Cons:
   - Still overstates certainty on mixed regional segments.
   - Cannot distinguish a true conservative CW segment from a broad phone default.
2. Leave ambiguous final outcomes internal-only and invent a display-only mode elsewhere
   - Pros:
   - Preserves strict semantic purity between truth and presentation.
   - Avoids introducing a filter token for blank modes.
   - Cons:
   - Requires wider downstream changes and breaks the current telnet/filter expectations.
   - Does not satisfy the operator requirement for deterministic filter behavior on blank-mode spots.
3. Persist IARU region in gridstore
   - Pros:
   - Avoids recomputing region from CTY/DXCC during metadata refresh.
   - Cons:
   - Duplicates derivable state, adds schema/recovery surface, and provides little runtime value compared with existing metadata/cache paths.

## Consequences
- Positive outcomes:
  - Final frequency-based labeling now reflects regional band-plan differences, especially on 160/80/40m.
  - Mixed/unknown cases remain visibly blank instead of forcing misleading phone/CW labels.
  - Filters can include or exclude blank-mode spots deterministically via `UNKNOWN`.
- Negative outcomes / risks:
  - Some spots that previously showed `USB/LSB` or `CW` now show blank when the regional classifier resolves to `mixed`.
  - DXCC-based region mapping is still a product policy approximation for a small set of geographically broad entities.
- Operational impact:
  - Mode inference stats now distinguish regional CW, regional voice defaults, mixed blank outcomes, and unknown blank outcomes.
  - New-user/default filter profiles include `UNKNOWN` so blank-mode spots remain visible unless explicitly blocked.
  - Existing saved explicit mode allowlists are auto-migrated on load/login so blank-mode spots remain visible without a one-off admin rewrite.
- Follow-up work required:
  - Keep README/config docs aligned with the new regional tables and blank-mode filter semantics.

## Validation
- Tests/benchmarks/analysis that justify this decision.
  - `go test ./spot ./filter ./telnet`
  - Added regression coverage for:
    - DXCC/ADIF to IARU region mapping,
    - regional CW/voice/mixed classification,
    - conservative unknown-region CW fallback,
    - cache non-seeding for regional defaults,
    - default filter visibility plus PASS/REJECT MODE UNKNOWN,
    - load-time migration of legacy saved mode allowlists.
- What evidence would invalidate this decision later?
  - If operators need entity-exact IARU region handling beyond the current DXCC/ADIF mapping surface.
  - If blank telnet mode output creates interoperability problems for downstream consumers that cannot use `UNKNOWN` filtering.

## Rollout and Reversal
- Rollout plan:
  - Deploy with the new config files and README/help updates.
  - Watch regional mixed/unknown counts and confirm default filters still show blank-mode spots as expected.
- Backward compatibility impact:
  - Final frequency fallback behavior changes, telnet mode can now be blank, and PASS/REJECT MODE now recognizes `UNKNOWN`.
- Reversal plan:
  - Restore the prior band-default table and remove the `UNKNOWN` filter token if operator feedback shows the blank-mode contract is unacceptable.

## References
- Issue(s):
  - none
- PR(s):
  - none
- Commit(s):
  - pending
- Related ADR(s):
  - `ADR-0051` (superseded)
- Troubleshooting Record(s):
  - none
- Docs:
  - `README.md`
  - `data/config/README.md`
  - `data/config/iaru_regions.yaml`
  - `data/config/iaru_mode_inference.yaml`

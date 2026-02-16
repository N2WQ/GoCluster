# ADR-0010 - S Glyph Confidence Floor Includes Recent-On-Band Admission

Status: Accepted
Date: 2026-02-16
Decision Makers: Cluster maintainers
Technical Area: main/spot confidence rendering
Tags: confidence, call-correction, observability

## Context
- Confidence glyph `S` was previously assigned only when a call was present in
  known-calls (`MASTER.SCP`) and confidence remained `?`.
- We now maintain recent-on-band evidence keyed by `(call, band, mode)` and use
  it for correction min-reports bonus.
- Operators requested the `S` confidence floor to also reflect this recent
  evidence signal.

## Decision
- Extend `applyKnownCallFloor` so `S` is assigned when confidence is still `?`
  and either:
  - call is in known-calls (`MASTER.SCP`), or
  - call is admitted by recent-on-band evidence in the configured window with
    required unique spotter count.
- Preserve existing guardrails:
  - only for confidence-capable modes (CW/RTTY/USB/LSB),
  - no overwrite when confidence is not `?`,
  - no beacon override.

## Alternatives Considered
1. Keep `S` as SCP-only
   - Pros:
     - Stable legacy semantics.
   - Cons:
     - Ignores high-quality recent local evidence.
2. Add a new glyph for recent-on-band
   - Pros:
     - Distinguishes source of floor signal.
   - Cons:
     - Increases user-facing complexity and filter semantics.
3. Promote all recent-on-band calls regardless of mode/band scoping
   - Pros:
     - More `S` assignments.
   - Cons:
     - Weakens signal quality and can misrepresent confidence.

## Consequences
- Positive outcomes:
  - `S` now reflects both static prior validity and dynamic recent corroboration.
  - Better operator visibility for likely-good calls that were not in SCP.
- Negative outcomes / risks:
  - `S` semantics broaden; historical interpretation shifts from SCP-only.
  - Misconfiguration of recent thresholds may over-promote `S`.
- Operational impact:
  - No protocol changes; one confidence glyph rule update.
  - Existing recent-on-band knobs influence `S` assignment when enabled.
- Follow-up work required:
  - Monitor `S` volume and downstream filtering behavior after rollout.

## Validation
- Unit tests cover:
  - SCP promotion remains intact.
  - recent-on-band promotion works.
  - mode mismatch and disabled recent feature do not promote.
  - non-`?` confidence still not overridden.

## Rollout and Reversal
- Rollout plan:
  - Enable with current recent-on-band settings and observe `S` distribution.
- Backward compatibility impact:
  - User-visible confidence semantics changed for `S`.
- Reversal plan:
  - Revert to SCP-only check in `applyKnownCallFloor`.

## References
- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s): `docs/decisions/ADR-0008-call-correction-recent-band-bonus.md`, `docs/decisions/ADR-0009-call-correction-stacked-prior-recent-bonus.md`
- Docs: `README.md`

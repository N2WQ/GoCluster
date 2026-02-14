# ADR-0008 - Call Correction Recent-On-Band Bonus (Min-Reports Only)

Status: Accepted
Date: 2026-02-14
Decision Makers: Cluster maintainers
Technical Area: spot/correction
Tags: call-correction, consensus, bounded-state

## Context
- We want to reduce unattempted corrections for likely-good calls that are one short on `min_reports`.
- Existing hard gates (`advantage`, `confidence`, `freq_guard`, `cooldown`) already provide strong safety and should remain unchanged.
- Operators requested minimal configuration knobs and explicit separation from SCP-based priors.

## Decision
- Add a new recent-on-band corroboration signal keyed by `(call, band, mode)`.
- Add exactly four config knobs:
  - `recent_band_bonus_enabled`
  - `recent_band_window_seconds`
  - `recent_band_bonus_max`
  - `recent_band_record_min_unique_spotters`
- Apply recent-band bonus only to `min_reports` during candidate evaluation.
- Keep all other hard gates unchanged and still required.
- Keep SCP/prior bonus logic independent (no coupling).
- Record recent-on-band observations from accepted post-gate spots (after correction/harmonic/license gates).
- Use a bounded, sharded in-memory store with periodic cleanup and per-key spotter caps.

## Alternatives Considered
1. SCP-coupled recent bonus
   - Pros:
     - Stricter precision for edge cases.
   - Cons:
     - Violates requirement to keep recent-on-band separate from SCP.
2. Weighted-support integration
   - Pros:
     - More flexible scoring.
   - Cons:
     - Adds complexity and extra tuning knobs; higher operator burden.
3. Hard-gate changes (for example relaxing confidence/advantage)
   - Pros:
     - More corrections.
   - Cons:
     - Higher false-positive risk; changes deterministic safety contract.

## Consequences
- Positive outcomes:
  - More one-short candidates can be attempted without weakening hard gates.
  - Recent evidence is mode- and band-specific, improving relevance.
  - Minimal operator surface area (four knobs only).
- Negative outcomes / risks:
  - Additional heap and CPU overhead for maintaining recent-on-band state.
  - If configured too aggressively, may increase attempts that still fail downstream gates.
- Operational impact:
  - New YAML settings in `data/config/pipeline.yaml`.
  - Decision path now includes `recent_band_bonus` when this signal contributes.
- Follow-up work required:
  - Monitor correction attempt/apply/reject trends and adjust window/threshold defaults if needed.

## Validation
- Add unit tests for:
  - one-short admission with recent-on-band bonus;
  - no admission when unique-spotter threshold is not met;
  - hard gates still blocking (advantage) even when recent bonus applies.
- Add store unit tests for admission, expiry, and normalization behavior.

## Rollout and Reversal
- Rollout plan:
  - Enable with conservative defaults (`window=12h`, `bonus_max=1`, `min_unique_spotters=2`).
- Backward compatibility impact:
  - No protocol format changes; correction output remains deterministic under existing hard gates.
- Reversal plan:
  - Set `recent_band_bonus_enabled: false` to disable behavior without code rollback.

## References
- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s): `docs/decisions/ADR-0003-call-correction-anchor-gating.md`, `docs/decisions/ADR-0007-call-correction-topk-prior-bonus-observability.md`
- Docs: `README.md`, `data/config/pipeline.yaml`

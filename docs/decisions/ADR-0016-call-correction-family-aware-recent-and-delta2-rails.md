# ADR-0016 - Family-Aware Recent/Stabilizer Keys, Suppressor Edge Guard, and Truncation Bonus Rails

Status: Accepted
Date: 2026-02-23
Decision Makers: Cluster maintainers
Technical Area: main/telnet fan-out, spot/correction, config
Tags: call-correction, telnet, recent-band, truncation, bounded-state

## Context
- Operators still observed contradictory telnet output for related call families, especially around:
  - slash-vs-bare variants (`W1AW` vs `W1AW/5`)
  - truncation variants (`W1AB` vs `W1ABC`)
- Recent-on-band and stabilizer admission used exact call keys, which reduced family coherence.
- Telnet family suppression used a single rounded frequency bucket, which could miss boundary-adjacent variants.
- `max_length_delta=2` was configured in runtime YAML, but no dedicated strict rails existed for distance-2 truncation families.
- A requested length-based bonus needed to be constrained to truncation families and hard-gate safe.

## Decision
1. Make recent-on-band and stabilizer checks family-aware using correction identity keys:
   - use canonical vote key plus base key for lookups/recording.
   - keep bounded recent-band storage and existing per-key spotter caps.
2. Fix suppressor bucket-edge misses:
   - evaluate same-mode entries in adjacent frequency bins.
   - add absolute frequency-delta guard (within configured tolerance) before family suppression applies.
3. Add truncation-only capped length bonus:
   - applies only when a truncation family relation is detected and candidate is more specific.
   - applies only to `min_reports` effective support.
   - does not bypass `advantage`, `confidence`, `freq_guard`, or `cooldown`.
4. Add optional delta-2 truncation rails:
   - optional extra confidence requirement.
   - optional candidate validation requirement (and optional subject-unvalidated requirement).
   - intended for deployments using `max_length_delta >= 2`.

## Alternatives considered
1. Keep exact-call recent/stabilizer keys
   - Pros:
     - no behavior change.
   - Cons:
     - preserves slash/truncation incoherence in recent-heard gating.
2. Expand suppressor bucket width globally
   - Pros:
     - fewer edge misses.
   - Cons:
     - higher over-suppression risk and weaker frequency locality.
3. Global length bonus across all candidates
   - Pros:
     - simpler scoring.
   - Cons:
     - over-broad; not family-aware and risks unrelated longer-call bias.

## Consequences
- Positive outcomes:
  - Better family coherence between recent-on-band, stabilizer, and telnet suppression.
  - Fewer boundary misses in telnet family suppression.
  - Higher recall on truncation near-threshold cases without broad gate weakening.
  - Safer delta-2 operation under explicit stricter rails.
- Risks:
  - Slightly higher correction pressure in truncation families when length bonus is enabled.
  - Slightly more recent-band key cardinality due vote/base dual recording.
- Mitigations:
  - Bonus remains min-reports-only and capped.
  - Hard gates remain unchanged.
  - Recent-band store remains bounded and sharded; suppressor cache remains bounded.

## Links
- Related ADRs:
  - `docs/decisions/ADR-0014-call-correction-family-policy.md`
  - `docs/decisions/ADR-0015-call-correction-family-policy-yamlization.md`
- Code:
  - `main.go`
  - `spot/correction.go`
  - `telnet_family_suppressor.go`
  - `config/config.go`
- Tests:
  - `main_test.go`
  - `spot/correction_test.go`
  - `telnet_family_suppressor_test.go`
  - `config/call_correction_stabilizer_test.go`
- Config/docs:
  - `data/config/pipeline.yaml`
  - `README.md`
  - `docs/decision-log.md`

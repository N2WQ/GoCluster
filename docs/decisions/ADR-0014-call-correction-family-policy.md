# ADR-0014 - Call-Correction Family Policy: Slash Threshold, Truncation Advantage Rail, and Telnet Family Suppression

Status: Accepted
Date: 2026-02-22
Decision Makers: Cluster maintainers
Technical Area: spot/correction, main/telnet fan-out
Tags: call-correction, telnet, precedence, bounded-state

## Context
- Operators reported repeated dual-output cases where both a less-specific and a more-specific call variant were emitted to telnet users.
- Two high-impact families were identified:
  - slash family (for example `W1AW` vs `W1AW/7`, `N2WQ` vs `KP4/N2WQ`)
  - one-character truncation family (for example `W1AB` vs `W1ABC`, `A1ABC` vs `WA1ABC`)
- Existing slash precedence required full `min_consensus_reports` to trigger, which was too strict in thin clusters.
- Existing hard-gate behavior required positive advantage even in truncation ties where the longer call had independent validation and the shorter call did not.
- Telnet output could still emit both variants before correction converged, even with the stabilizer.

## Decision
1. Add a unified correction-family detector:
   - `slash` family: same correction `BaseKey` with exactly one slash-explicit member.
   - `truncation` family: no slash on either call, edit-length delta exactly 1, and strict prefix/suffix containment.
2. Add explicit slash-precedence threshold:
   - new config `call_correction.slash_precedence_min_reports` (default `2`).
   - slash precedence uses this threshold instead of `min_consensus_reports`.
3. Add truncation-only advantage relaxation rail:
   - for truncation family only, set effective `min_advantage=0` when:
     - candidate is the more-specific (longer) form,
     - longer form is validated by SCP or recent-on-band admission,
     - shorter form is not validated by SCP or recent-on-band.
   - all other gates remain unchanged (`min_reports`, `confidence`, `freq_guard`, `cooldown`).
4. Add telnet-only family suppression:
   - maintain bounded recent family state in mode/frequency buckets.
   - suppress less-specific family variants when a more-specific variant is already dominant in the same bucket/window.
   - suppression is output-only (no correction-trace contamination); archive and peer output contracts are unchanged.

## Alternatives Considered
1. Keep slash precedence tied to `min_consensus_reports`
   - Pros:
     - No new config knob.
   - Cons:
     - Fails to prioritize slash identity under common thin-cluster conditions.
2. Relax advantage globally for all distance-1 candidates
   - Pros:
     - Larger correction rate increase.
   - Cons:
     - Over-broad; risks overriding real corroboration differences outside truncation family.
3. Implement only output suppression and skip correction-side changes
   - Pros:
     - Lowest algorithm risk.
   - Cons:
     - Leaves avoidable no-winner outcomes and weaker slash prioritization in correction traces.

## Consequences
- Positive outcomes:
  - Better slash identity retention with independent corroboration floor (`2` reporters).
  - Better recovery in validated truncation ties without broad gate weakening.
  - Fewer contradictory telnet outputs for same-family calls.
- Negative outcomes / risks:
  - Slightly higher correction pressure in truncation families; mitigated by strict validation rail.
  - Added bounded in-memory state for telnet family suppression.
- Operational impact:
  - New operator knob: `call_correction.slash_precedence_min_reports`.
  - Telnet-visible suppression behavior changes for same-family variants.
  - No archive/peer protocol or schema changes.

## Validation
- Unit tests for:
  - family detector behavior on slash and truncation examples.
  - slash-precedence threshold independence from `min_consensus_reports`.
  - truncation advantage relaxation positive and safety-rail negative cases.
  - telnet family suppression behavior (slash, truncation, promotion, bucket isolation).
- Required checker suite:
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...` for touched concurrent telnet suppressor path.

## Rollout and Reversal
- Rollout plan:
  - deploy with default `slash_precedence_min_reports=2`.
  - monitor correction decision reasons (`advantage`, `min_reports`) and telnet-visible duplicate-variant rate.
- Backward compatibility impact:
  - call-correction/telnet behavior changes as described above; no wire-format changes.
- Reversal plan:
  - set `slash_precedence_min_reports` high (effectively disabling slash bias) and/or revert the family policy changes.

## Links
- Code:
  - `spot/correction.go`
  - `main.go`
  - `telnet_family_suppressor.go`
  - `config/config.go`
- Tests:
  - `spot/correction_test.go`
  - `telnet_family_suppressor_test.go`
  - `main_test.go`
  - `config/call_correction_stabilizer_test.go`
- Docs/config:
  - `data/config/pipeline.yaml`
  - `README.md`
  - `docs/decision-log.md`

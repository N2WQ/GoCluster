# ADR-0043 - Custom SCP Admission Restricted to V Confidence

Status: Accepted
Date: 2026-02-28
Decision Origin: Design

## Context

- Custom SCP runtime evidence is used in confidence-floor promotion and recent-support rails.
- If non-`V` outputs (`S`, `P`, `C`, `?`) are admitted back into custom SCP, the store can reinforce its own derived outcomes.
- This can create feedback loops where SCP-backed confidence influences future SCP membership growth without equivalent independent evidence quality.

## Decision

- Restrict custom SCP runtime admission to spots with confidence glyph `V` only.
- `RecordSpot` ignores all non-`V` confidence values.
- Existing mode, SNR, horizon, and resource-bound gates remain unchanged.

## Alternatives considered

1. Admit `V` and `C`
- Pros: faster growth of evidence.
- Cons: still allows correction-derived feedback.

2. Admit `S`, `P`, `V`, `C`
- Pros: maximal evidence intake.
- Cons: strongest self-reinforcement risk.

3. Keep prior behavior and rely only on SNR/horizon thresholds
- Pros: no behavior change.
- Cons: does not directly address loop risk.

## Consequences

- Benefits:
  - Breaks direct `S -> SCP` and `C -> SCP` feedback loops.
  - Keeps custom SCP anchored to strongest-confidence outputs.
- Risks:
  - Slower evidence accumulation in sparse environments.
- Operational impact:
  - Lower admission volume to SCP.
  - More conservative static membership growth.

## Links

- Supersedes/extends: `docs/decisions/ADR-0039-custom-scp-runtime-evidence-and-shared-pebble-resilience.md`
- Code:
  - `spot/custom_scp_store.go`
  - `spot/custom_scp_store_test.go`
  - `README.md`

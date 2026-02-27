# ADR-0039 - Custom SCP Runtime Evidence and Shared Pebble Resilience Helper

Status: Accepted
Date: 2026-02-27
Decision Origin: Design
Technical Area: main output pipeline, spot/correction, config, persistence
Tags: custom-scp, recency, confidence, resolver, stabilizer, pebble, resilience

## Context

- Existing confidence-floor static membership relied on `MASTER.SCP` and recent-on-band runtime memory.
- Operators requested replacing `MASTER.SCP` with a runtime-learned local database while preserving deterministic correction semantics.
- We need long-horizon recency (up to 13 months), mode-bucket granularity, and bounded resource behavior.
- Gridstore already has strong Pebble resilience controls (checkpoint/integrity/restore), and we want the new custom-SCP store to reuse the same resilience orchestration shape.

## Decision

1. Add a custom-SCP Pebble store as the new static confidence-floor source when `call_correction.custom_scp.enabled=true`.
   - Static membership is runtime-learned from accepted observations.
   - `MASTER.SCP` loading/scheduler path is bypassed for confidence-floor static membership when custom-SCP is enabled.
2. Add long-horizon recency evidence storage with fixed mode buckets:
   - `VOICE={USB,LSB}`, `CW`, `RTTY`.
   - Non-bucket modes are ignored for custom-SCP evidence.
3. Apply custom-SCP evidence in all three decision surfaces:
   - resolver recent-plus1 rails,
   - stabilizer recent support checks,
   - confidence `S` floor.
4. Enforce configured CW/RTTY SNR floors:
   - `min_snr_db_cw`, `min_snr_db_rtty` with `>0` meaning enabled.
   - when enabled and `HasReport=false`, evidence is not recorded.
   - voice has no SNR knob.
5. Use H3 res-1 for geographic diversity counting in custom-SCP evidence.
6. Introduce `internal/pebbleresilience` shared helper and use it for resilience operations in both grid and custom-SCP orchestration.

## Alternatives considered

1. Keep `MASTER.SCP` as static source and add custom-SCP as secondary
   - Pros: smaller migration risk.
   - Cons: contradicts hard-replace operator requirement and creates dual-source ambiguity.
2. Keep in-memory recent-on-band only
   - Pros: simpler implementation.
   - Cons: no persistence, no long-horizon evidence, weaker restart determinism.
3. Build independent resilience logic per Pebble store
   - Pros: no shared abstraction design.
   - Cons: duplicated checkpoint/restore logic and higher drift risk.

## Consequences

- Positive:
  - Local runtime-learned static membership replaces external SCP dependency for `S` floor.
  - Resolver/stabilizer/`S` floor use one evidence backend with bounded knobs.
  - Shared resilience helper reduces duplicate checkpoint/restore code.
- Risks:
  - Custom-SCP policy tuning is more complex than legacy static membership.
  - Local runtime-learned static membership can drift if thresholds are too permissive.
- Mitigations:
  - Conservative defaults and bounded caps.
  - Explicit reject-reason observability in decision paths.
  - Shared checkpoint/integrity/restore workflow.

## Links

- Related ADR(s):
  - `docs/decisions/ADR-0010-s-glyph-recent-on-band-floor.md`
  - `docs/decisions/ADR-0034-resolver-recent-plus1-corroborator-rail.md`
- Code:
  - `internal/pebbleresilience/manager.go`
  - `spot/custom_scp_store.go`
  - `main.go`
  - `config/config.go`
  - `spot/recent_support_store.go`
- Docs:
  - `README.md`
  - `data/config/pipeline.yaml`
  - `docs/decision-log.md`

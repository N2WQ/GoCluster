# ADR-0003: Call Correction Anchor Gating and Config-Driven Determinism

- Status: Accepted
- Date: 2026-02-07

## Context

The call-correction path in `spot/correction.go` had two operational risks:

1. Anchor-selected corrections could apply before the full consensus gate chain (`min_reports`, `advantage`, `confidence`, `freq_guard`, `cooldown`) was evaluated.
2. Runtime behavior had config-to-code drift:
   - `freq_guard_*` config values were not mapped into `CorrectionSettings`.
   - `recency_seconds_cw` / `recency_seconds_rtty` were defined in config but not used in correction window selection.
   - Runner-up selection used single-pass map iteration state, making `freq_guard` sensitive to iteration order.

These issues reduced determinism, made operator tuning less predictable, and obscured root-cause analysis when corrections were applied or rejected.

## Decision

Adopt a single deterministic gating contract for both correction paths:

1. Anchor-selected candidates are evaluated through the same gate chain as consensus-selected candidates.
2. Quality score mutation occurs only after a fully-gated apply decision.
3. Winner/runner-up selection uses deterministic ranking (`support desc`, `lastSeen desc`, `call asc`) to ensure stable runner-up frequency/support pairing.
4. Runtime settings are sourced from config:
   - Wire `freq_guard_min_separation_khz` and `freq_guard_runnerup_ratio` into `CorrectionSettings`.
   - Use mode-specific recency windows (`recency_seconds_cw`, `recency_seconds_rtty`) in correction evaluation.
   - Size correction-index cleanup window using the maximum effective recency to preserve bounded retention.
5. Add trace observability with explicit `decision_path` (`anchor` or `consensus`) in correction traces and SQLite decision records.

## Alternatives considered

1. Keep anchor fast path ungated, but raise quality threshold.
- Rejected: reduces frequency of anchor applies but preserves unsafe bypass semantics.

2. Remove anchor path entirely and rely on majority only.
- Rejected: safer but loses useful low-latency correction behavior under stable signals.

3. Add anchor-specific relaxed gates.
- Rejected: increases policy complexity and operator ambiguity; harder to reason about determinism.

## Consequences

### Benefits

- Deterministic and auditable correction semantics across paths.
- Reduced risk of anchor-driven self-reinforcement without corroboration.
- Config-driven behavior aligns operator intent with runtime behavior.
- `freq_guard` decisions are stable under map iteration variability.

### Risks

- Some anchor corrections previously applied will now be rejected by gates, reducing correction rate under sparse corroboration.
- SQLite schema evolves with a new `decision_path` column; migration handling is required for existing daily DB files.

### Operational impact

- No protocol format changes.
- Operator tuning remains in `pipeline.yaml`; key threshold changes are explicit.
- Decision logs can now be segmented by apply path for troubleshooting and tuning.

## Links

- Code:
  - `spot/correction.go`
  - `main.go`
  - `spot/decision_logger.go`
  - `config/config.go`
- Tests:
  - `spot/correction_test.go`
  - `spot/decision_logger_test.go`
  - `main_test.go`
- Config/docs:
  - `data/config/pipeline.yaml`
  - `README.md`
  - `docs/decision-log.md`

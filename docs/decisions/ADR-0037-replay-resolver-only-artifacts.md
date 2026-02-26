# ADR-0037 - Replay Resolver-Only Artifact Schema

Status: Accepted  
Date: 2026-02-26  
Decision Origin: Design  
Technical Area: `cmd/rbn_replay`, replay artifacts, replay run history

## Context

- Replay had dual-method artifact outputs (current-path vs resolver), including comparison metrics (`agreement_pct`, `dw_pct`, `sp_pct`, `uc_pct`) and method split stability summaries.
- Resolver-primary is now the only method targeted for replay evaluation and switchover evidence.
- Keeping dual-method schema after resolver-only direction caused ambiguity and drift in tooling, reports, and run-memory snapshots.

## Decision

- Move replay artifacts to **resolver-only schema**.
- Remove legacy comparison artifacts from replay JSON/CSV outputs and history snapshots, including:
  - `comparable_decisions`
  - `agreement_pct`, `dw_pct`, `sp_pct`, `uc_pct`
  - `method_stability.current_path` / `method_stability.resolver`
  - legacy-confidence comparison counters (for example `legacy_unknown_now_p`)
  - disagreement sample CSV artifact
- Keep replay A/B instrumentation as resolver-centric counters only (`output`, `resolver`, `stabilizer_delay_proxy`).
- Keep `stability` as the resolver-applied output-path follow-on stability summary.
- Require resolver-primary configuration for replay runs:
  - `call_correction.enabled=true`
  - `call_correction.resolver_mode=primary`

## Alternatives considered

1. Keep dual-method schema and ignore unused fields  
   - Pros: no consumer break.  
   - Cons: preserves obsolete semantics and analysis drift.
2. Add a mode flag to emit both schemas  
   - Pros: migration path.  
   - Cons: higher maintenance surface and parity risk.
3. Keep dual schema but deprecate fields in docs only  
   - Pros: minimal code change.  
   - Cons: does not remove operational ambiguity.

## Consequences

- Benefits:
  - Replay outputs are aligned with resolver-only evaluation intent.
  - Lower replay overhead by removing disagreement sampling and dual stability collection.
  - Cleaner run-history comparison and simpler artifact interpretation.
- Risks:
  - Breaking compatibility for existing consumers/scripts reading removed fields.
- Operational impact:
  - Existing historical runs remain readable but new runs emit resolver-only schema.
  - `compare-runs.ps1` must use resolver-only fields.

## Links

- Supersedes: `docs/decisions/ADR-0024-replay-dual-method-stability-metrics.md`
- Related: `docs/decisions/ADR-0025-replay-run-history-and-local-comparison.md`
- Related: `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md`
- Replay docs: `docs/rbn_replay.md`

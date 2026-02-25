# ADR-0024 - Replay Dual-Method Recall/Stability Metrics

Status: Accepted
Date: 2026-02-24
Decision Makers: Cluster maintainers
Technical Area: cmd/rbn_replay, replay evidence artifacts
Decision Origin: Design
Troubleshooting Record(s): n/a
Tags: replay, resolver, metrics, phase3-evidence

## Context

- Replay already produced resolver-vs-current disagreement/alignment metrics (`agreement`, `DW/SP/UC`) and current-path follow-on stability.
- Phase 3 method selection requires side-by-side evidence for both methods on the same replay input.
- Without resolver-side applied/stability metrics, replay cannot support direct “which method wins” decisions.

## Decision

- Extend replay outputs to include method-level stability summaries for:
  - `current_path`
  - `resolver`
- Add these summaries to:
  - `gates.json` under `overall.method_stability`
  - `manifest.json` under `results.method_stability`
- Keep existing `overall.stability` / `results.stability` as current-path summary for backward compatibility.
- Define resolver “applied” for replay metrics as:
  - resolver snapshot state is `confident` or `probable`,
  - snapshot has non-empty winner,
  - winner differs from the pre-correction subject call for the replay spot.

## Alternatives Considered

1. Keep only disagreement/alignment metrics
   - Pros: no schema changes.
   - Cons: still cannot compare method recall/stability directly.
2. Replace old `stability` field with dual-method only
   - Pros: cleaner schema.
   - Cons: breaks existing consumers expecting `stability`.
3. Add resolver metrics in a separate output file
   - Pros: isolates new data.
   - Cons: splits critical evidence across artifacts.

## Consequences

- Positive outcomes:
  - Replay can directly compare method-level recall proxy (`total_applied`) and stability (`stable_pct`).
  - Phase 3 method selection evidence becomes explicit in a single artifact set.
- Risks:
  - Resolver “applied” is a replay proxy, not an on-wire cutover behavior.
  - Metric consumers must avoid interpreting resolver applied count as production output.
- Operational impact:
  - No runtime production behavior changes.
  - Replay artifact schema is extended, not replaced.

## Links

- Decision index: `docs/decision-log.md`
- Replay docs: `docs/rbn_replay.md`
- Related ADRs:
  - `docs/decisions/ADR-0022-phase2-signal-resolver-shadow-mode.md`
  - `docs/decisions/ADR-0023-replay-analysis-tool-consolidation.md`

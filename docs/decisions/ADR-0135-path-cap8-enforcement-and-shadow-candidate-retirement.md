# ADR-0135: Path Cap8 Enforcement And Shadow Candidate Retirement

- Status: Accepted
- Date: 2026-05-11
- Decision Origin: Troubleshooting chat

## Context

Receiver-cap shadow diagnostics compared count caps 5, 6, and 8 under live
traffic while active path glyphs still used raw selected evidence. The
diagnostic served its purpose: recent propagation logs showed cap8 had the
lowest would-block and p50-weaker rates among the candidate caps in the
operator-reviewed window.

The shadow-candidate implementation also added parallel retained bucket state,
candidate histogram work, telnet atomics, and separate propagation log lines.
Keeping those lanes after choosing a cap would make the active path prediction
code harder to reason about without answering a current operational question.

## Decision

Make the checked-in path-reliability policy enforce receiver caps with:

```text
receiver_contribution_mode: enforce
receiver_max_effective_count: 8
```

Retire the alternate receiver-cap candidate diagnostics. Remove the
`receiver_shadow_max_effective_counts` and `receiver_shadow_p50_enabled` YAML
keys from the required config schema and stop calculating, retaining, and
logging candidate cap lanes at runtime.

Keep historical propagation-report parsing for old `Path cap shadow (5m)` and
`Path cap p50 shadow (5m)` lines so older log bundles remain analyzable.

## Alternatives considered

1. Keep shadow candidates after switching cap8 active.
   Rejected because the selected cap is now active policy and the extra lanes
   keep hot-path and diagnostic complexity alive.
2. Switch to cap6 enforcement.
   Rejected because the reviewed diagnostic window showed cap8 producing fewer
   active-glyph disruptions while still enforcing receiver diversity.
3. Remove `shadow` mode entirely.
   Rejected because single-cap shadow remains useful as a safe diagnostic mode;
   only the parallel candidate comparison lanes are being retired.

## Consequences

### Benefits

- Active glyphs and PATH filters now use capped receiver evidence.
- The path store no longer maintains three alternate candidate lanes per
  shadow bucket.
- Propagation logs return to one path-prediction aggregate line for current
  runtime behavior.

### Risks

- Cap8 is less strict than cap5 or cap6; a single receiver can contribute more
  capped effective observations before the receiver-diversity gate matters.
- Historical cap-shadow report sections remain parseable but are no longer
  produced by current runtime logs.
- Operators comparing old and new propagation reports must account for the
  removed candidate shadow lines.

### Operational impact

- Startup no longer requires `receiver_shadow_max_effective_counts` or
  `receiver_shadow_p50_enabled`.
- With `min_observation_count: 21` and `receiver_max_effective_count: 8`,
  enforce mode requires at least three live attributed receiver slots, capped by
  the selected bucket's slot capacity.
- Existing `Path predictions (5m)` counters remain the current operational
  aggregate. `cap_limited` still reports when caps reduced evidence, and
  `cap_would_block` remains meaningful only if an operator manually runs
  single-cap `shadow` mode.

## Links

- Related issues/PRs/commits:
- Related tests: `pathreliability/config_test.go`,
  `pathreliability/receiver_test.go`, `pathreliability/store_bench_test.go`,
  `telnet/server_prediction_stats_test.go`,
  `internal/propreport/report_test.go`
- Related docs: `data/config/path_reliability.yaml`,
  `pathreliability/README.md`, `README.md`, `docs/OPERATOR_GUIDE.md`,
  `data/config/PATH_PREDICTIONS.md`, `customgpt/common-questions.md`
- Related TSRs: TSR-0023
- Supersedes / superseded by: supersedes ADR-0132 and ADR-0133; supersedes the
  shipped cap value portion of ADR-0129.

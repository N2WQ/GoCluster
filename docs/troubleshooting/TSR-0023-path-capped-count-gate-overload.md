# TSR-0023: Path Capped Count Gate Overload

- Status: Resolved
- Date opened: 2026-05-11
- Status date: 2026-05-11

## Trigger

Operator review found that capped evidence failed the count gate often under
`min_observation_count: 21`, even with high overall path data volume.

## Symptoms and impact

When receiver-cap enforcement was evaluated, many predictions became
insufficient for `low_count`. The operational reading of
`min_observation_count` was "minimum selected observations", but enforce mode
was applying that floor to floored decayed capped effective count.

## Hypotheses tested

1. The p50 glyph calculation was too conservative.
2. Path evidence lacked raw selected observations.
3. The count gate was applied to the wrong count lane under capped evidence.

## Evidence

- `Predictor.predictWithMinObservationCount` applied the configured floor to
  `merged.Count`.
- `Store.sampleWithDistribution` made `Sample.Count` raw in `off`/`shadow`
  modes but capped effective count in `enforce` mode.
- `Store.updateCapped` decays capped count and floors it for public samples,
  so the enforce-mode count gate was sensitive to capped count decay rather
  than only raw sample size.
- Four receivers at cap 6 can produce at most 24 capped effective observations;
  modest decay can pull that below a floor of 21.

## Root cause or best current explanation

The raw observation floor and capped receiver-trust floor were collapsed into
one numeric gate. That made `min_observation_count` change meaning by receiver
cap mode and caused legitimate raw selected samples with adequate receiver
diversity to fail as `low_count`.

## Fix or mitigation

The fix keeps raw selected observation count as the only
`min_observation_count` input. Receiver-cap enforcement now uses a separate
derived receiver-diversity gate and reports failures as `low_receiver`/`lowr`.

Short-term mitigation before deploying this fix is to keep
`receiver_contribution_mode: shadow` while reviewing `Path cap shadow` and
`Path cap p50 shadow` logs.

## Why an ADR was or was not required

- ADR required because the fix changes the durable path-reliability evidence
  contract and operator-visible diagnostics.

## Links

- Related ADRs: ADR-0134
- Related issues/PRs/commits:
- Related tests: `pathreliability/receiver_test.go`, `telnet/diag_command_test.go`, `telnet/server_prediction_stats_test.go`, `internal/propreport/report_test.go`
- Related docs: `pathreliability/README.md`, `README.md`, `docs/OPERATOR_GUIDE.md`, `data/config/PATH_PREDICTIONS.md`

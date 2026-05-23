# ADR-0134: Path Observation Floor And Receiver Diversity Gate

- Status: Accepted
- Date: 2026-05-11
- Decision Origin: Troubleshooting chat

## Context

`min_observation_count` was introduced as a raw selected observation floor so
path glyphs would not be emitted from tiny samples. Receiver-cap enforcement
later made `Sample.Count` become the floored capped effective count in enforce
mode. That overloaded one YAML key with two meanings:

- raw selected report count in `off` and `shadow`
- decayed capped effective count in `enforce`

With `min_observation_count: 21` and `receiver_max_effective_count: 6`, four
fresh receivers could contribute at most 24 capped effective observations.
Normal decay could then drop capped count below 21 even though the raw selected
sample and receiver diversity were adequate.

## Decision

Keep `min_observation_count` as a raw selected observation floor in every
receiver contribution mode.

Receiver-cap enforcement now applies a separate receiver-diversity gate. The
selected capped evidence must include at least:

```text
ceil(min_observation_count / receiver_max_effective_count)
```

live attributed receiver slots, capped by the selected bucket's receiver slot
capacity. User `SET PATHSAMPLES` remains a stricter raw observation floor only;
it does not raise the receiver-diversity requirement beyond the cluster default
receiver-cap policy.

Expose receiver-diversity failures separately:

- telnet `SET DIAG PATH`: `lowr`
- propagation logs: `low_receiver`
- cap-shadow logs: `capN_low_receiver`
- propagation report JSON: `avg_low_receiver`

## Alternatives considered

1. Lower `min_observation_count`.
   Rejected because it would tune around the semantic mismatch instead of
   correcting the count contract.
2. Keep applying `min_observation_count` to capped effective count in enforce
   mode.
   Rejected because the same YAML key would keep changing meaning by mode.
3. Add a new YAML receiver-diversity threshold.
   Deferred because the existing `min_observation_count` and
   `receiver_max_effective_count` already define the minimum implied receiver
   diversity without increasing operator burden.

## Consequences

### Benefits

- `min_observation_count` has one operator meaning in all modes.
- Capped evidence still blocks receiver-concentrated paths.
- Logs can distinguish sparse raw samples from insufficient receiver diversity.

### Risks

- Enforce mode can allow paths whose decayed capped count is below
  `min_observation_count`, provided raw observations and receiver diversity are
  adequate.
- Historical `low_count` and `capN_low_count` counters before this decision are
  not directly comparable with the new split counters.
- The receiver-diversity gate uses bounded retained slots, not an exact
  unbounded unique receiver set.

### Operational impact

- No new YAML key is required.
- `SET DIAG PATH` capped diagnostics change from `n<capped>/r<raw>` to
  `n<raw>/c<capped>/rx<receivers>` when receiver caps reduced evidence.
- Propagation log parsers should tolerate the new optional `low_receiver` and
  `capN_low_receiver` fields; legacy logs parse those values as zero.

## Links

- Related issues/PRs/commits:
- Related tests: `pathreliability/receiver_test.go`, `telnet/diag_command_test.go`, `telnet/server_prediction_stats_test.go`, `internal/propreport/report_test.go`
- Related docs: `data/config/path_reliability.yaml`, `pathreliability/README.md`, `README.md`, `docs/OPERATOR_GUIDE.md`, `data/config/PATH_PREDICTIONS.md`, `customgpt/common-questions.md`
- Related TSRs: TSR-0023
- Supersedes / superseded by: supersedes the enforce-mode capped-count floor portion of ADR-0129 and the cap-shadow low-count-only fields in ADR-0132

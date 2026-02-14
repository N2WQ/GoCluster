# ADR-0007: Call-Correction Top-K Evaluation, Strict Prior Bonus, and Decision Counters

- Status: Accepted
- Date: 2026-02-13

## Context

Call correction still produced avoidable `no_winner` outcomes when the top-ranked
candidate failed a gate but lower-ranked candidates were valid. We also needed a
strict mechanism to recover one-short `min_reports` cases for likely-good calls
without weakening hard safety gates. Finally, operators needed direct runtime
visibility into rejection reasons, fallback depth, and prior-bonus usage.

Constraints:
- Keep all existing hard gates intact (advantage/confidence/freq_guard/cooldown).
- Keep all tuning in `pipeline.yaml` (no hard-coded policy values).
- Preserve deterministic and bounded behavior in the correction hot path.

## Decision

1. Add top-K consensus candidate evaluation:
   - New knob: `call_correction.candidate_eval_top_k`
   - Correction evaluates up to K ranked consensus candidates (after anchor), in order.
2. Add strict prior bonus for one-short min-reports cases:
   - New knobs: `call_correction.prior_bonus_*`
   - Bonus applies only to `min_reports` gate, only when one short, with tight distance, and (by default) SCP known-call requirement.
   - Bonus does not bypass advantage/confidence/freq_guard/cooldown.
3. Add correction decision observability counters:
   - Track total/applied/rejected decisions, rejection reasons, path counts, fallback depth, and prior-bonus usage.
   - Expose summary in periodic stats output.

## Alternatives considered

1. Keep top-1 only candidate evaluation.
- Rejected: leaves valid lower-ranked corrections unapplied.

2. Apply prior bonus as global support weighting.
- Rejected: weakens multiple gates and increases false-positive risk.

3. Rely only on offline decision logs for observability.
- Rejected: slows tuning loop and provides weaker live operational signal.

## Consequences

### Benefits

- Fewer false negatives from top-1 candidate failures.
- Controlled improvement in one-short `min_reports` cases using trusted priors.
- Better operator visibility into why corrections were rejected/applied.

### Risks

- Overly permissive top-K/prior configs can increase false positives; mitigated by unchanged hard gates and strict defaults.
- Additional stats dimensions increase cardinality; mitigated by bounded key sets and compact line summaries.

### Operational impact

- Adds operator-facing config knobs in `call_correction`.
- Adds one new stats line in runtime display (`CorrGate` summary).
- No protocol/wire compatibility changes.

## Links

- Code:
  - `spot/correction.go`
  - `main.go`
  - `config/config.go`
  - `stats/tracker.go`
- Tests:
  - `spot/correction_test.go`
  - `main_test.go`
  - `stats/tracker_correction_test.go`
- Config/docs:
  - `data/config/pipeline.yaml`
  - `README.md`
  - `docs/decision-log.md`


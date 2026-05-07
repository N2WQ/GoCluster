# ADR-0120 - Resolver Correction State Hardening

Status: Accepted
Date: 2026-05-06
Decision Makers: Cluster maintainers
Technical Area: spot/signal_resolver, spot/correction, config, correctionflow, replay
Decision Origin: Troubleshooting chat
Troubleshooting Record(s): none
Tags: resolver-primary, concurrency, config, bayes, distance-model

## Context

- Review feedback identified that resolver hysteresis state could outlive the
  candidate it referenced. In that case a later evaluation could keep treating a
  removed winner as authoritative until the key expired.
- Morse/Baudot distance weights were configured by mutating package-level
  globals while correction readers consumed the same globals and cost-table
  slices.
- `CorrectionBayesBonusPolicy.Configured` was intended to distinguish explicit
  runtime wiring from zero-value tests, but configured zero values for several
  neutral Bayes knobs were silently replaced with defaults.
- `weightedPatternCost` returned a single insert cost for any empty source
  pattern and used insert cost for empty target patterns, which was formally
  wrong even though current codebooks do not contain empty character patterns.

## Decision

- Couple resolver hysteresis state to candidate lifetime:
  - candidate removal invalidates matching `stableWinner` and `pendingWinner`;
  - stale fallback repair resets stable state to the current top candidate and
    recomputes runner/margin fields.
- Publish Morse/Baudot distance configuration as immutable snapshots:
  - each configure call normalizes weights, builds a complete rune index and
    cost table, then atomically swaps the snapshot;
  - readers load one snapshot per distance calculation.
- Preserve explicit neutral Bayes zeros when a policy/config is configured:
  - distance weights;
  - `prior_log_min_milli`;
  - advantage weighted deltas;
  - extra confidence deltas.
- Keep safety-positive Bayes fields positive: smoothing values, caps except the
  prior minimum, score thresholds, and maximum prior cap.
- Compute empty-pattern weighted costs from the full raw insert/delete cost and
  run that raw cost through the same scaling path as non-empty pattern costs.

## Alternatives Considered

1. Document "configure only at startup" for Morse/Baudot weights.
   - Pros: smallest diff.
   - Cons: leaves an exported package function with an easy-to-misuse race
     hazard and no mechanical enforcement.
2. Clear resolver stale winners only in the final fallback branch.
   - Pros: localized fix.
   - Cons: split-state and future evaluation paths could still carry stale
     hysteresis state after candidate removal.
3. Treat every configured Bayes zero as a default request.
   - Pros: preserves old behavior.
   - Cons: makes documented neutral/disabled values impossible for specific
     weighted terms.
4. Special-case empty weighted patterns with unscaled raw cost.
   - Pros: simple arithmetic.
   - Cons: creates different semantics from the normal weighted-pattern path.

## Consequences

- Positive outcomes:
  - Resolver snapshots no longer publish removed stable winners.
  - Runtime, replay, and tools can safely reconfigure Morse/Baudot weights
    without racing correction readers.
  - Bayes config can intentionally disable selected weighting or extra rails
    without disabling the entire Bayes block.
  - Pattern-cost behavior is formally correct for future/custom codebooks.
- Negative outcomes / risks:
  - Atomic distance snapshots allocate new tables on each configure call. The
    current call sites configure at startup/tool initialization, so this is not
    a hot-path cost.
  - Explicit Bayes zeros can weaken conservative rails if operators tune
    `pipeline.yaml` without replay evidence.
- Operational impact:
  - No wire/protocol/archive schema changes.
  - Existing shipped config values are unchanged.
  - Private config directories that intentionally set the newly documented
    neutral Bayes fields to `0` now get the literal neutral behavior.

## Validation

- Added resolver tests for candidate removal, cap eviction, and stale fallback
  repair.
- Added distance snapshot tests, including a race-targeted concurrent
  configure/read test.
- Added Bayes normalization/config/correctionflow mapping tests for explicit
  neutral zeros and unsafe zero rejection.
- Added weighted-pattern empty input tests.

## Rollout and Reversal

- Rollout plan:
  - Deploy binary and config comments together.
  - Leave shipped Bayes numeric values unchanged.
- Backward compatibility impact:
  - Existing non-zero configs retain behavior.
  - Configs that set documented neutral Bayes fields to zero now preserve zero
    instead of silently receiving defaults.
- Reversal plan:
  - Revert this ADR's code changes as a unit if resolver stability, distance
    parity, or Bayes tuning behavior regresses.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0032-resolver-primary-family-gate-parity-and-conservative-contested-glyphs.md`
  - `docs/decisions/ADR-0036-resolver-confusion-tiebreak-runtime-replay-parity.md`
  - `docs/decisions/ADR-0044-resolver-bayes-capped-gate-bonus.md`
- Troubleshooting Record(s): none
- Docs:
  - `data/config/pipeline.yaml`
  - `spot/README.md`

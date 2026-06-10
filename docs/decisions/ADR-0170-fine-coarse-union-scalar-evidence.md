# ADR-0170: Fine-Coarse Union Scalar Evidence

- Status: Accepted
- Date: 2026-06-10
- Decision Origin: Design review

## Context

The active p50 path method selects fine and coarse geographic bucket evidence in
each direction before receive/transmit merge. A valid fine report also updates
the matching coarse bucket. The previous blend-band scalar fields summed fine
and coarse `Weight`, `RawWeight`, and `CappedWeight`, while counts already used
the larger layer because one report can update both resolutions.

That mixed convention made scalar evidence mass look larger than the true
overlapping evidence set. It affected `SET DIAG PATH` weight values, the normal
weight gate, and shadow receiver-cap diagnostics. The p50 histogram shape,
however, intentionally gave local fine evidence extra emphasis by adding fine
bins and coarse bins together.

## Decision

Use union semantics for fine/coarse scalar evidence in one direction:

- `Weight = max(fine.Weight, coarse.Weight)`
- `RawWeight = max(fine.RawWeight, coarse.RawWeight)`
- `CappedWeight = max(fine.CappedWeight, coarse.CappedWeight)`
- counts and receiver-capacity fields continue to use max semantics

Keep the p50 histogram blend shape unchanged. Fine bins and coarse bins are
still added together, preserving the existing local-emphasis distribution for
eligible samples.

Fine/coarse blended age now uses local fine mass plus the coarse regional
complement:

```text
fine_weight = fine.Weight
coarse_complement = max(0, coarse.Weight - fine.Weight)
```

The receive/transmit age merge remains unchanged because those directions are
not overlapping populations.

The checked-in config remains weight-gate neutral for scalar union semantics
because:

```text
min_effective_weight <= min_fine_weight *
    min(merge_receive_weight, merge_transmit_weight, reverse_hint_discount)
```

Future calibration that violates this invariant can make scalar union semantics
withhold glyphs as `low_weight`. It cannot create a new visible glyph because
the scalar change only lowers or preserves weight.

## Alternatives considered

1. Keep summed scalar weights and document the 2x local emphasis.
   Rejected because the inflated weight was a real diagnostic and gate
   semantics defect.
2. Change the p50 histogram to explicit complement decomposition.
   Deferred because capped receiver evidence is not a strict fine subset of
   coarse evidence in enforce mode. That design needs a separate ADR and tests
   for raw, shadow, and enforce modes.
3. Change only scalar weights and leave fine/coarse age unchanged.
   Rejected because age would keep describing the old double-counted mass and
   could let stale local fine evidence borrow freshness from regional coarse
   updates.

## Consequences

### Benefits

- `SET DIAG PATH` weight reflects non-double-counted scalar evidence mass.
- Weight gates and shadow `CapWouldBlock` diagnostics no longer get artificial
  help from a fine report counted once in fine and again inside coarse.
- Active p50 distribution shape, bin geometry, midpoint/even-split semantics,
  thresholds, and PATH filter classes stay unchanged for eligible samples.

### Risks

- `SET DIAG PATH w<weight>` can step down on deploy day in the blend band.
- `SET DIAG PATH a<age>` can shift because receive/transmit age merge weights
  change when one direction's selected scalar weight shrinks.
- Fine/coarse union age can make an old local direction stale. This can
  increase `stale` insufficiency counters or shift final class mix when the
  surviving direction has a different p50 class.
- `CappedWeight` max is a conservative, never-double-counting proxy. It is not
  an exact union mass when capped fine and coarse acceptance diverge.

### Operational impact

- No YAML key, telnet command, log field name, p50 threshold, VOACAP behavior,
  or receiver-cap value changes.
- Operators comparing before/after diagnostics should treat lower `w<weight>`,
  changed `a<age>`, higher shadow would-block rates, or higher stale counts as
  expected evidence-contract corrections, not necessarily a propagation change.

## Links

- Related issues/PRs/commits:
- Related tests: `pathreliability/normalize_test.go`,
  `pathreliability/config_test.go`, `pathreliability/receiver_test.go`,
  `telnet/diag_command_test.go`
- Related docs: `pathreliability/README.md`, `README.md`,
  `docs/OPERATOR_GUIDE.md`, `data/config/PATH_PREDICTIONS.md`,
  `data/config/path_reliability.yaml`,
  `customgpt/support-cards/path-reliability.md`,
  `customgpt/troubleshooting-index.md`
- Related TSRs: TSR-0023, TSR-0025
- Supersedes / superseded by: Refines ADR-0126, ADR-0131, ADR-0134, and
  ADR-0139 for fine/coarse scalar evidence and selected-age semantics.

# ADR-0174: Beacon RX-Only Path Prediction Semantics

- Status: Accepted
- Date: 2026-06-11
- Decision Origin: Design

## Context

Active p50 path prediction is directional. Normal spots blend DX-to-user
receive evidence with user-to-DX transmit evidence using
`merge_receive_weight` and `merge_transmit_weight`, with the receive-side
`SET NOISE` penalty applied to the DX-to-user leg. When only one direction is
available, the inherited merge policy applies `reverse_hint_discount` to scalar
weight. The p50 histogram scaling preserves the median class, so the practical
one-direction suppression mainly comes from the normal pooled floors:
`min_observation_count` and the derived receiver-diversity gate.

That model is defensible for ordinary bidirectional operator paths, but it is
not defensible for beacons. A beacon's missing transmit direction is not weak
evidence; it is not applicable. Holding beacons to the same pooled two-direction
floor can leave receive-heavy beacon paths blank even when the receive leg has
enough independent evidence for the per-direction design point.

GoCluster already owns a canonical `Spot.IsBeacon` flag. It is a heuristic
that can be set by source-class beacon state, `/B` calls, known beacon calls,
and beacon comment keywords. This decision intentionally keys on that existing
runtime qualifier rather than creating a second beacon roster or changing
beacon detection.

## Decision

For spots marked `IsBeacon`, path prediction uses RX-only semantics:

- use only the DX-to-user receive bucket
- apply the user's receive-noise penalty
- do not merge or discount transmit evidence
- do not apply `reverse_hint_discount`
- use `beacon_min_observation_count` as the configured raw observation floor
- derive receiver diversity from `beacon_min_observation_count` and
  `receiver_max_effective_count`

The shipped `beacon_min_observation_count` is `11`. With the shipped
`receiver_max_effective_count: 8`, the derived beacon receiver requirement is
2. This preserves the same per-direction trust standard implied by the normal
21-observation pooled floor while removing the accidental two-direction penalty.

User `SET PATHSAMPLES` can raise the raw beacon floor for that user as
`max(beacon_min_observation_count, user setting)`. It does not raise the
receiver-diversity gate, matching the existing user floor contract for normal
paths.

If beacon p50 is insufficient and VOACAP fallback has a current-hour record,
fallback classifies on `ReceiveDB()` and REL-gates on the receive leg. The
bidirectional effective SNR and lower-of-two REL gate remain the normal
non-beacon fallback model.

`SET DIAG PATH` marks beacon RX-only provenance with `brx` for bucket evidence
and `bvcap`, `bvaln`, `bvup`, or `bvop` for beacon VOACAP fallback. Five-minute
propagation logs add beacon-specific final counters while preserving existing
aggregate counters.

Sufficient beacon p50 remains authoritative and does not enter the existing
bidirectional `VOACAP p50 compare (5m)` line. Broader one-direction policy for
non-beacon rare-DX paths remains out of scope pending more diagnostics.

## Alternatives considered

1. Keep normal p50 gates and only remove `reverse_hint_discount`.
   - Rejected because this would leave most beacon suppression in place; the
     primary blocker is the normal observation and receiver floors, not the
     scalar discount.
2. Reuse bidirectional VOACAP fallback for insufficient beacon p50.
   - Rejected because the transmit leg is meaningless for beacon semantics and
     can incorrectly close or downgrade a receive-observed beacon path.
3. Add a strict external beacon roster before changing prediction.
   - Rejected for this slice because the runtime already has one canonical
     beacon qualifier. Detection policy can be revisited separately if field
     data shows false positives.
4. Apply RX-only one-direction semantics to all non-beacon paths.
   - Deferred because rare-DX and other one-direction cases need diagnostics
     before changing global policy.

## Consequences

### Benefits

- Beacon glyphs and PATH filters use the same coherent RX-only model.
- Beacon receive evidence is no longer accidentally held to a pooled
  two-direction floor.
- Beacon VOACAP fallback no longer depends on a meaningless transmit leg.
- Operators can distinguish beacon RX-only results from normal bidirectional
  results in diagnostics and propagation logs.

### Risks

- `IsBeacon` is heuristic. Human comments containing beacon keywords can opt a
  spot into beacon RX-only semantics.
- Beacon counters are not directly comparable with historical non-beacon
  insufficient counters because the floor and direction model differ.
- RX-only beacon VOACAP diagnostics show receive-leg SNR and REL, while normal
  VOACAP diagnostics continue to show the bidirectional effective model.

### Operational impact

- `data/config/path_reliability.yaml` adds required key
  `beacon_min_observation_count`.
- `SET DIAG PATH` can show `brx`, `bvcap`, `bvaln`, `bvup`, or `bvop`.
- `Path predictions (5m)` adds additive beacon counters:
  `beacon_rx`, `beacon_rx_insufficient`, `beacon_rx_<reason>`, and
  `beacon_rx_voacap_*`.
- Non-beacon path prediction, PATH filters, VOACAP fallback, and
  `SHOW PROP` behavior are unchanged.

## Links

- Related code: `pathreliability/predictor.go`,
  `pathreliability/config.go`, `pathreliability/voacap_fallback.go`,
  `telnet/server.go`, `internal/cluster/bootstrap.go`,
  `internal/propreport/report.go`
- Related config: `data/config/path_reliability.yaml`
- Related tests: `pathreliability/beacon_prediction_test.go`,
  `pathreliability/config_test.go`, `telnet/path_settings_test.go`,
  `telnet/diag_command_test.go`, `telnet/server_prediction_stats_test.go`,
  `internal/propreport/report_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `pathreliability/README.md`, `data/config/PATH_PREDICTIONS.md`,
  `customgpt/support-cards/path-reliability.md`
- Related TSRs: none
- Supersedes / superseded by: extends ADR-0126, ADR-0131, ADR-0134,
  ADR-0169, and ADR-0173 without superseding them

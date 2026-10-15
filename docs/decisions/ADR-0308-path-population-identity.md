# ADR-0308: Path Population Identity and Common Resolution

- Status: Accepted
- Date: 2026-10-15
- Decision Origin: Design, Approved v2; serial gates retained by Approved v4

## Context

Fine and coarse H3 mappings are independent. Their evidence can overlap, and
forward/reverse roles can alias one physical regional bucket. Adding these
views gave local reports extra histogram mass and could duplicate receivers.
The ordinary glyph summarizes regional SNR under receive noise; it does not
express QSO probability or calibrated confidence.

## Decision

Capture and evaluate each unique key once at one common accounting reference.
Only copied values leave locks. Query scratch is fixed and temporary; retained
bucket layout and ownership are unchanged.

Ordinary paths select fine only when both distinct fine directions are present,
store-valid, prediction-fresh and at or above fine_only_weight in active decayed
signal weight. Otherwise select available coarse directions without fine fill.
If all coarse candidates are unavailable, fresh fine evidence may supply a
hint below that threshold. Beacon paths select qualifying receive fine,
otherwise available receive coarse, otherwise a fresh receive fine hint.

Keep availability separate from numerical usability. Fresh count-only or
corrupt coarse evidence blocks fine retry. Required selected distribution
failure is terminal; excluded failures do not poison a chosen layer. Prediction
freshness uses the existing ceiling and inclusive boundary; disabled prediction
freshness does not disable store expiry.

Shared evidence is one receive hint: noise once, ordinary signal and histogram
discount once, no beacon discount. Distinct directions retain existing .6/.4
mixture weights. Receiver identity union counts a live hash once, preserving
current epsilon-OR liveness in this slice. Configured capacity stays attached to
physical contributing populations. Wholly count-only queries retain configured
requirements while supplying zero diversity. Recent-support policy is S7.

min_fine_weight retains required validation and its normalization relationship
to fine_only_weight, but has no direct blend effect. No configuration values,
persisted settings, thresholds, forecast precedence or wire format change.

## Alternatives considered

1. Add fine and coarse histograms: duplicates overlapping reports.
2. Mix fine in one direction with coarse in the other: independently mapped
   H3 layers cannot guarantee disjoint populations.
3. Keep per-report identities and subtract exact overlap: broader retained
   state and lifecycle scope than this bounded wave.

## Consequences

### Benefits

Physical evidence and receiver identities have explicit bounded ownership.
Selection, aging and arithmetic failure are directly testable.

### Risks

Removing local extra emphasis can change p50 and glyph class. Shared evidence
can fail the single-direction signal gate and become forecast-eligible. These
are policy consequences, not calibrated accuracy gains. Tracked receiver-slot
rotation remains an approximation.

### Operational impact

No configuration or firewall changes. Ordinary glyphs remain enabled. Existing
sufficient-observed authority and CLOSED fallback precedence are preserved.
Timing and allocation guards remain required; S4's exception does not cover S6.

## Links

- Code: pathreliability/candidate_query.go, store.go, predictor.go, normalize.go.
- Tests: population_selection_test.go, population_identity_test.go,
  population_histogram_test.go, telnet/path_population_selection_test.go.
- Docs: pathreliability/README.md, docs/path-reliability-first-wave-execution.md.
- Supersedes ADR-0170 fine/coarse scalar, histogram-blend and selected-age
  semantics. Complements ADR-0131/0301 histogram semantics and refines physical
  identity counting under ADR-0134.

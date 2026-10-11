# ADR-0291: Correction SNR Presence And Domain

- Status: Accepted
- Date: 2026-10-11
- Decision Origin: Design

## Context

The shared resolver evidence builder discarded report presence, allowing an
absent report's stored integer to act as SNR. Unmatched SNR selected the model's
last bin. Both offline CSV readers converted parsed floats to int without a
finite or representability check.

## Decision

Carry explicit report presence from the Spot into normalized resolver evidence
and the candidate's existing representative report. Preserve newest-timestamp
selection and later-receipt selection at equal timestamps. Missing metadata can
supersede present metadata under that rule. No newest-present fallback or
per-receiver SNR aggregation is introduced.

Omit confusion contribution for a missing representative report. In both public
and prepared scoring, omit contribution for nonfinite SNR and values outside
the model's domain. Preserve every `(lower, upper]` bin. Reported zero is a
present value and contributes when zero is within that domain. These observations
still contribute receiver support and remain in the evidence denominator.

Both offline CSV readers preserve integer parsing first and their existing
`int(v+0.5)` float fallback. Before float conversion, require finiteness and
representability of the truncated transformed value in the native signed-int interval:
the lower power-of-two bound is inclusive and the upper is exclusive. Preserve
negative-decimal asymmetry, finite float precision, accepted zero and entire-row
rejection of blank or unparseable reports. The fraction is discarded toward zero,
so a fractional transformed value just below MinInt may still yield valid MinInt;
do not reject that valid conversion. No CSV presence column or admission
change is made. Accepted CSV rows remain explicitly present reports.

## Alternatives considered

- Treating zero as absent loses real zero reports and does not exclude missing
  metadata with a nonzero stored value.
- Clamping out-of-domain SNR invents a model bin the asset does not define.
- Dropping the whole observation changes receiver accounting rather than its
  optional model contribution.
- Selecting a newest present report or replacing rounding changes independent
  representative or compatibility policy.

## Consequences

Model ranking can change when it previously relied on absent or out-of-domain
inputs. Valid in-domain scores and support percentages retain their meanings.
The model's statistical validity, calibration and RF episode association remain
unestablished. No persisted Spot encoding, output format, retained collection
or worker changes.

## Links

- Approved scope: `docs/call-correction-evidence-scope-ledger-v2.md`, B2.2.
- Execution evidence: `docs/call-correction-evidence-execution.md`.
- Refines [ADR-0036](ADR-0036-resolver-confusion-tiebreak-runtime-replay-parity.md).

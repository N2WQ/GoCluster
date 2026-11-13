# TSR-0131 - VOACAP Receiver Longitude Column Shift

Status: Monitoring
Date Opened: 2026-11-12
Date Resolved: n/a
Owner: Repository maintainer
Technical Area: Native VOACAP CIRCUIT serialization and forecast cache migration
Trigger Source: Chat request
Led To ADR(s): ADR-0347
Tags: voacap, fixed columns, geometry, native oracle, persisted predictions

## RCA Summary

- What happened: a deck could echo the intended endpoint text while the native
  engine computed a different receiver longitude and circuit distance.
- Why: the shared CIRCUIT writer placed one extra space before receiver
  longitude. The receiver hemisphere occupied column 47 instead of column 46.
- What fixed it: remove that space, check the independent native fixed-field
  contract, and retire model-generation-1 predictions whose geometry cannot be
  established from saved records.
- How we know: independent source-format and geometry checks caught a
  Warsaw/Boston circuit of about 3841.6 km where the intended circuit is about
  6550.4 km. The repaired production builder passed actual native body-header
  checks, rather than relying on its echoed input.
- Operator/support answer: do not preserve or serve old-model predictions as
  trusted forecasts. Valid unexpired recognized keys can receive bounded
  one-time immediate admission when requested; queueing can still leave gaps.
  Native qualification establishes the tested geometry contract, not physical
  forecast accuracy or production capacity.

## Triggering Request

- Request date: 2026-11-12.
- Request summary: qualify the native engine before the approved flux-input and
  24-hour forecast change; amend migration after discovering unsafe old geometry.
- Request reference: owner selection to retire old forecasts and rebuild known
  paths immediately, followed by exact `Approved v4`.

## Symptoms and Impact

`BuildPathDeck` is shared by runtime fallback and manual experiments. The old
receiver-longitude field was eleven columns wide where the native reader
expects ten. A west hemisphere could be lost and east inferred. Existing
saved predictions have no geometry provenance that distinguishes affected
circuits from correct ones. Their numeric results cannot establish safety.

The manual experiment's Wolf input and its configuration remain unchanged;
only the shared native CIRCUIT serialization is repaired there.

## Timeline

1. 2026-11-12 - Initial engine probes checked parsed coverage and echoed inputs;
   independent geometry review invalidated that as a sufficient geometry gate.
2. 2026-11-12 - Original native source identified the fixed fields. A corrected
   prototype passed independent coordinate and body-header checks.
3. 2026-11-12 - Owner approved v4 with retirement of old-model predictions.
   The actual production builder then passed 36 isolated native invocations,
   192 rolling-window selections, and ten negative witnesses.

## Hypotheses and Tests

1. Correct endpoint text in echoed input proves native geometry.
   - Evidence: the defective deck echoed intended coordinates while the native
     body header reported the wrong circuit distance.
   - Outcome: Rejected. Validate the parsed native geometry independently.
2. The distance disagreement is normal Earth-radius rounding.
   - Evidence: the Boston/Warsaw discrepancy is thousands of kilometres;
     independently derived intended distance using the native 6370 km radius
     is 6550.35340994 km. Boston/Sydney is 16237.32797161 km.
   - Outcome: Rejected. Radius precision cannot explain the discrepancy.
3. Removing the extra field space satisfies the actual native reader.
   - Evidence: DECRED.FOR uses
     `FORMAT(10X,F5.2,A1,3(F9.2,A1),4X,I5)`. Tests decode fixed columns without
     calling the production formatter. Actual native body headers match the
     independently derived ordinary-path geometry after repair.
   - Outcome: Supported for the qualified cases. Polar and near-coincident
     geometry need their own tolerances; the ordinary-path tolerance is not
     generalized to them.

## Findings

- Root cause: a serialization error at the fixed-column native parser boundary.

Successful execution, expected output rows, and echoed input each failed to
prove the engine used the intended receiver coordinates. Qualification must
include an independent output-side witness and a deliberate unchanged-echo,
corrupted-body negative case.

The migration changes a durable compatibility decision: all old-model
predictions are retired, even if their deletion fails or the process restarts.
The separate bounded known-key marker carries eligibility, never predictions.

## Decision Linkage

[ADR-0347](../decisions/ADR-0347-voacap-flux-and-daily-forecast.md) records the
coordinate contract, model-generation retirement, and request-driven rebuilding
alongside the flux-input and daily-window changes.

## Verification and Monitoring

The local production-builder receipt is
`.tmp/voacap-v3/native4-production/report.json`; the gate receipt is
`.tmp/voacap-v3/GATE_STATUS.json`. They record 36 invocations (28 distinct and
eight repeats), 192 rolling windows, and ten negative witnesses. The earlier
echo-only receipt remains invalidated; it is not a passed geometry gate.
Sampled two-invocation native totals ranged from about 80.3 to 112.2 ms. These
are conditional native timings, not full-worker or production capacity.

Permanent regression coverage includes
`TestBuildPathDeckNativeCircuitColumns`,
`TestNativeCircuitOracleRejectsShiftedHemisphere`, and the v4 legacy retirement
tests. The owner has not authorized deployment or restart. Production recovery,
coverage gaps, queue age, and worker duration remain deployment-time signals.

## References

- Original native source archive:
  [VOACAPW.zip](https://www.greg-hand.com/voacap_source/VOACAPW.zip),
  DECRED.FOR line 202 and original writer VOACAP.FOR line 82.
- [ADR-0347](../decisions/ADR-0347-voacap-flux-and-daily-forecast.md).
- [Runtime path contract](../../pathreliability/README.md).
- [Operator guide](../OPERATOR_GUIDE.md).

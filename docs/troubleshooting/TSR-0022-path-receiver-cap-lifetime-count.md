# TSR-0022: Path Receiver Cap Lifetime Count

- Status: Resolved
- Date opened: 2026-05-10
- Status date: 2026-05-10

## RCA Summary

- What happened: Under receiver-cap enforcement, path p50 glyphs stayed too
  conservative because newer strong receiver reports could not enter capped
  evidence after earlier weak reports filled the cap.
- Why: Receiver slot count was treated as a lifetime `uint32` admission cap
  while capped weight and SNR evidence decayed over time.
- What fixed it: ADR-0130 made receiver slot count and bucket capped count
  decay with capped weight, admitting new reports fractionally when partial
  capacity remains.
- How we know: Targeted reproductions showed shadow/raw evidence improving
  while enforce/capped p50 stayed weak, and `pathreliability/receiver_test.go`
  validated newer strong evidence admission after decay.
- Operator/support answer: If enforce-mode p50 looks frozen on old weak
  receivers, check whether capped count decay is active; shadow mode is the
  short-term diagnostic fallback.

## Trigger

Operator audit reported that path reliability p50 glyphs looked too
conservative under receiver-cap enforcement, with mostly `-` and `<` glyphs and
few `=` or `>` glyphs.

## Symptoms and impact

When `receiver_contribution_mode: enforce` was active, a receiver that had
already contributed five weak reports to a bucket could not contribute newer
strong reports to that bucket's capped p50 evidence. This could keep the capped
p50 weak even after old weak evidence had decayed.

## Hypotheses tested

1. WSPR volume dominated the combined path bucket.
2. Weighted SNR histogram bins decayed incorrectly.
3. Receiver contribution caps froze newer capped evidence after old reports
   reached the per-receiver count cap.

## Evidence

- `pathreliability.Store.updateCapped` decayed capped weight and capped SNR
  bins, but checked `receiverSlot.count` as a lifetime `uint32`.
- `decayReceiverSlots` decayed `receiverSlot.weight` only.
- Targeted reproductions showed `shadow` mode used raw newer strong evidence
  while `enforce` mode stayed on old weak capped p50.
- Added regression coverage in `pathreliability/receiver_test.go` verifies
  newer strong evidence enters after old capped count decays.

## Root cause or best current explanation

The receiver cap count lane was not time-decayed. It behaved as a lifetime
admission cap inside each retained bucket, while the evidence lanes it gated
were decaying. That mismatch froze capped p50 on the first five accepted
reports for a receiver.

## Fix or mitigation

Receiver slot count and bucket capped count now decay with capped weight. New
reports are admitted fractionally when only partial receiver count or weight
capacity remains, keeping capped count, capped weight, and capped SNR bins
coherent.

Short-term mitigation before deploying the fix is to use
`receiver_contribution_mode: shadow`, which preserves raw active glyphs while
still exposing capped diagnostics.

## Why an ADR was or was not required

- ADR required because the fix changes the durable meaning of
  `receiver_max_effective_count` from lifetime accepted reports to decayed
  effective observations.

## Links

- Related ADRs: ADR-0130
- Related issues/PRs/commits:
- Related tests: `pathreliability/receiver_test.go`
- Related docs: `pathreliability/README.md`, `README.md`,
  `docs/OPERATOR_GUIDE.md`, `data/config/PATH_PREDICTIONS.md`

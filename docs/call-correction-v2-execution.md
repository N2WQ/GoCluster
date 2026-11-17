# Call Correction v2 Execution

Authority: [approved Scope Ledger v3](call-correction-v2-scope-ledger-v3.md).
Owner authorization: exact `Approved v3`, 2026-11-17.
Branch: `call-correctionv2`; baseline `3fd37c6cb48d2028756ca18184d4f18a8dc95bb0`.

## Evidence status

Before implementation, separate scientific reviews and an independent scope
adversary challenged the original and materially revised designs. The final v3
challenge had no unresolved material scope blocker. The postapproval detailed
contract-to-test review was completed and its checker refinements adopted before
S0. Reviews are evidence plans, not implementation/validation passes.

The postapproval matrix covers legacy report compatibility, row/SNR binding,
reference isolation and embargo, frozen candidate/provenance, full paired outcome
denominators, per-cell activation boundaries, actual study CLI refusals, independent
parser grammar, harmonic presence, admitted-neighbor selection, receiver SNR joins,
top-cohort-only scoring, immutable/fresh publication, canonical union/display/
lifecycle/temporal delivery, decision-local history capture, cell-map/capacity
consumers, actual Bayesian reachability, preserved temporal objective and paced
asynchronous resource qualification. Exact checker substitutions must preserve
these failure mechanisms and be recorded with executed results.

## Unit accounting

| Unit | Implementation | Validation |
| --- | --- | --- |
| S0 evaluation foundation/provenance | Legacy compatibility and frozen baseline complete; study/qualification fixture in progress | 13 compatibility cases passed; study/capacity not yet qualified |
| S1a legacy report parsing | Pending | Not run |
| S1b harmonic presence | Pending | Not run |
| S2 neighborhood admission | Pending | Not run |
| S3 receiver SNR | Pending | Not run |
| S4 canonical slash authority | Pending | Not run |
| S5 per-decision history | Pending | Not run |
| S6 Bayesian retention/study/activation | Pending | Not qualified |
| S7 final integration/resource/docs/lane | Pending | Not qualified |

## Baseline and study artifacts

Local ignored evidence belongs under `.tmp/call-correctionv2/`; runtime assets and
production data remain at their original paths. Baseline source/config/assets,
workload, and tool fingerprints must be recorded before dependent changes.
Baseline replay executable was built from the unchanged approved production source
before S0 edits. The paced fixture must remain baseline-compatible and its baseline
capture must precede S1-S6 production changes.

The initial ignored `baseline/manifest.json` records 20 copied runtime YAML files,
10 frozen data inputs (136,185,542 bytes), Go 1.27.1 windows/amd64 and the shipped
replay executable hash. The configured local FCC exception-list file was absent;
isolated arms use an explicitly empty list, identically, with that setup difference
recorded. FCC/ISED enforcement and actual license database snapshots are retained.

Executed S0 compatibility check:

```powershell
& .\scripts\test-windows.ps1 -Packages @('./cmd/callcorr_reveng_rebuilt') -Run '^TestCCV2EvaluationCompatibility$' -RequireCases @('dxcluster/cmd/callcorr_reveng_rebuilt::TestCCV2EvaluationCompatibility') -Timeout 180s
```

Observed: one package, 13 passed cases, zero skipped cases, exit 0. Literal cases
cover correct/wrong/missing/extra predictions, duplicated/conflicting labels,
loaded labels without replay rows, zero denominators and sample-size variation.
All legacy numeric formulas and ordered pairing remain unchanged. New population
fields remain invariant when only reservoir sample size changes; undefined RF and
label-event coverage values are explicitly null. This is reporting integrity,
not an RF or Bayesian improvement result.

The RBN-only reference study uses a fixed predictor receiver cohort A and withheld
cohort B. Every paired model arm sees A-only input/history/learning. References
are separately constructed from B observations and strictly later reports, with
focal aliases excluded. Qualification is expressly for this reduced-input archive
population. Full-input shadow/stability/capacity evidence does not inherit an
independent-reference accuracy claim.

No new model has been qualified; no production accuracy, capacity, deployment,
hosted CI, final-lane pass or task completion is claimed by this record.

# Call Correction v2 Scope Ledger v4

Status: Approved by the owner with exact `Approved v4`, 2026-11-17.
This is an additive study-input amendment to [approved v3](call-correction-v2-scope-ledger-v3.md).
V3's runtime units, risks, validation, activation floors and deployment/publication
boundaries remain unchanged.

## Resolved input policy and objective

The owner explicitly selected frozen CTY representative receiver locations for
the conditional archive study, after separate scientific review distinguished
this input uncertainty from the previously accepted RBN reference-label uncertainty.
This authorizes a CTY-derived receiver-location scenario, not reconstructed
historical receiver sites, reported grids, or live location availability/timing.

Real RBN archive CSV rows contain no receiver grid or coordinates. Empty daily
Custom SCP history consequently cannot meet its positive geographic requirements.
The existing live fallback supplies representative CTY coordinates and marks
them derived. The amendment makes that selected scenario explicit and testable
without lowering any history requirement or changing the history backend.

## S0b agreed scope and boundary

- Add deterministic receiver-grid derivation from the already frozen CTY image
  to the existing study-only input hook before A-only learning. Use current live
  receiver normalization and portable CTY precedence; mark estimates derived.
  Unknown matches or invalid coordinates remain unknown, including legitimate
  zero-coordinate handling. Add no receiver lookup registry, persistent cache,
  geographic service or ordinary replay/server behavior.
- Apply identical input rules and frozen CTY/H3 assets to shipped, repaired-off
  and repaired-on arms. Keep raw references, cohort isolation, raw IDs, episode
  geometry, history requirements, six predeclared complete candidates, selection
  rules and activation floors unchanged.
- Require an explicit input scenario in version-2 study plans and reports.
  Reject old/missing/scenario-drifted inputs before raw archive inspection.
  Record per-row derived grid/provenance and complete focal-A known/unknown
  coverage, including warmup and nondelivered outcomes. Bind input covariates in
  paired validation; do not filter unknown rows from study populations.
- Preserve previous artifacts. Rebuild/fingerprint the instrumented shipped
  comparator with original production correction source unchanged, and the one
  identical repaired off/on executable. Preserve the old shipped build's required
  pre-mask configuration seam rather than copying new typed-only code into it.
  Re-freeze controls before tuning; final holdouts remain unopened until the
  complete candidate, executable, config and assets are frozen.

## Verification and claim boundary

An independent scientific review and separate pre-approval scope/test challenge
actually occurred. The challenge found validation refinements, adopted here:

- Literal CTY points: 40 N/74 W produces FN30; 50 N/10 E (Country Files longitude
  -10) produces JO50. A competing portable example, `K1ABC/DL-#`, must retain DL
  precedence rather than collapsing to the base call. Test missing/invalid records
  separately from missing/corrupt frozen assets, which remain setup failures.
- Exercise the actual CSV-to-history route without manually seeded DE grids.
  Resolver-produced V observations from four receivers across two derived cells
  must satisfy the mapped consumer's four-receiver/two-cell requirements.
  Unknown and same-cell controls must not fabricate geographic qualification.
- Reconcile known + unknown with all focal-A decision records across all arms,
  including warmup, suppressed and license-rejected outcomes; exclude B learning.
  Check unchanged raw hashes, episode IDs, R1 and R2Frozen. Terminal R2 may
  legitimately become unknown when a decision exceeds the existing cutoff.
- Reject schema, scenario, asset and paired input-covariate drift. Test the
  ordinary no-study path remains unchanged. Run affected study/replay/config
  checks and race coverage on final source; retain v3's complete final lane and
  unchanged production resource gate.

Postapproval contract-to-test review retains these falsifiers: a helper-only test
cannot substitute for the real CSV hook; delivered-only counts cannot prove input
coverage; shared input imputation must not alter raw references; successful test
fixtures must not silently substitute for missing assets or real archive inputs.

CTY proxies can overstate or understate actual geographic diversity. Common
imputation does not cancel nonlinear eligibility errors. The result can establish
only paired algorithm gains under this fixed CTY-derived scenario and the frozen
RBN reference rules, not site independence, historical live equivalence, RF truth
or calibrated probabilities. Any failed activation floor still selects whole
Bayes off. No runtime model asset installation, deployment or push is authorized.

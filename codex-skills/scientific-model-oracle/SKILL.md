---
name: scientific-model-oracle
description: "Use when actual scientific or model inputs, outputs, semantics, boundaries, classifications, or claims change or need authoritative resolution. Do not trigger for mechanical work that cannot affect model behavior or claims."
---

# Scientific Model Oracle

## Purpose

Establish a model contract independently of the implementation being assessed.
Prevent code and tests from agreeing with each other while encoding the same
scientific or conceptual error.

## Workflow

1. Confirm the phase and review mode.
   - Run after initial Current-State Discovery and before requirements
     ambiguity review, design, or Scope Ledger publication.
   - Use a separate non-steered read-only reviewer when the credibility of the
     normative contract depends on reasoning outside the lead's accumulated
     assumptions. Otherwise the method may remain lead-owned; do not make
     independence a default workflow stage.

2. Define the normative question.
   - State the model behavior or scientific claim being evaluated.
   - Separate current implementation behavior, desired product behavior,
     accepted repository decisions, and external scientific authority.

3. Establish and cite a source hierarchy.
   - Rank applicable authoritative references, accepted ADRs, domain contracts,
     specifications, and explicitly documented assumptions.
   - Record conflicts, gaps, publication/version differences, and the reason a
     source controls.
   - Use current source and tests only as observational evidence. Do not treat
     them as sole normative authority.

4. Derive the model contract.
   - Define variables, units, conversions, valid input domains, and invalid or
     unavailable states.
   - State inclusive and exclusive boundaries, threshold behavior,
     interpolation, rounding, precision, and tolerances.
   - Define classifications, sentinels, and transitions between states.
   - Record uncertainty, calibration limits, and assumptions.

5. Derive independent golden vectors.
   - Include ordinary, boundary, just-below, just-above, invalid, and sentinel
     cases when applicable.
   - Record inputs, expected outputs or classifications, tolerances, derivation,
     and provenance for every vector.
   - Do not copy expectations from the implementation under test or derive
     them solely from its tests or fixtures. If independent derivation is not
     possible, report the missing oracle instead of manufacturing one.

6. Bound the claims.
   - State the strongest claims supported by the available evidence.
   - List attractive but unsupported claims, including precision, accuracy,
     confidence, equivalence, and operational-prediction claims.
   - Do not choose architecture, implementation, product policy, or scope.

7. Enforce the stopping condition.
   - Treat missing or conflicting normative evidence that could change model
     behavior or claims as blocking requirements resolution, design, and Scope
     Ledger publication until explicitly resolved.
   - Leave official decisions, scope, design, validation, and final disposition
     with the lead agent.

## Output

Report the normative question and source hierarchy;
- definitions, units, domains, boundaries, interpolation, rounding, and
  tolerances;
- provenance-independent golden vectors;
- classifications, sentinels, uncertainty, and calibration limits;
- supported and unsupported claims;
- conflicts, missing authority, blocking gaps, and remaining unknowns;
- inspected evidence and a reminder that the lead owns every disposition.

Preserve independent derivation and provenance-independent golden vectors.
Report missing authority as a gap; no fixed heading or result envelope is
required.

# Architecture Boundary

Use this reference to prevent semantic analysis from selecting implementation
mechanisms prematurely.

## Allowed Output

The ambiguity reviewer may state architecture-neutral constraints that follow
from an interpretation, such as:

* replacement and persistence commit must share one linearization order;
* stale writers must be rejected after the selected authority boundary;
* authority verification and persistence commit cannot have a check-then-write
  gap;
* state must remain bounded;
* existing compatibility obligations must be preserved;
* failure behavior must be deterministic and observable;
* the selected behavior must have an independent test oracle.

These constraints describe what any compliant design must achieve.

## Prohibited Output

The ambiguity reviewer must not:

* name, select, rank, reject, or recommend a concrete architecture;
* choose a lock, stripe, queue, coordinator, generation token, registry,
  revision field, compare-and-swap design, state machine, persistence protocol,
  ownership structure, helper type, or test seam;
* call any mechanism the smallest, safest, preferred, or recommended design;
* include touched files or implementation steps tied to one mechanism;
* override or replace `design-challenger`.

Do not allow architectural convenience to decide product semantics.

## Handoff to Design Challenger

Stop semantic analysis when the remaining question is:

> Which mechanism best implements already resolved behavior?

Route that question to `design-challenger` only after:

* the semantic interpretation is explicitly resolved;
* the authority source is recorded;
* architecture-neutral constraints are documented.

## Implementation Uncertainty

Classify a question as implementation uncertainty when:

* external behavior is already explicit;
* compatibility and failure semantics are settled;
* expected tests are known;
* only internal mechanism, structure, or performance tradeoffs remain.

Implementation uncertainty does not require user policy selection unless the
mechanism changes observable behavior or accepted risk.

## Required Output

For each interpretation, report only the constraints that every compliant
architecture must satisfy.

Do not recommend the mechanism.

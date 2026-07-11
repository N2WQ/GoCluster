# Scientific Source Hierarchy

Use this reference to select, rank, reconcile, and cite scientific, mathematical,
statistical, or normative authorities.

## Default Hierarchy

Prefer evidence in this general order:

1. governing standards, specifications, or authoritative normative sources;
2. peer-reviewed or primary scientific literature;
3. authoritative reference implementations, datasets, or manuals;
4. accepted domain conventions;
5. directly relevant empirical calibration or validation data;
6. repository domain contracts and accepted ADRs;
7. current implementation, tests, fixtures, and documentation as observational
   evidence.

The appropriate hierarchy may vary by domain. Explain why a source controls.

## Source Evaluation

For each material source, record:

* authority or publisher;
* publication or version;
* applicable domain;
* assumptions;
* units and reference frames;
* calibration population;
* uncertainty or tolerance;
* whether the source is normative, empirical, explanatory, or observational;
* whether it has been superseded or deprecated;
* conflicts with other sources;
* limits on applying it to GoCluster.

Do not cite a source merely because it uses similar terminology.

## Conflict Handling

When sources disagree:

1. determine whether they govern different domains, assumptions, eras, or
   measurement conventions;
2. determine whether one source is newer, normative, or more directly
   applicable;
3. identify any conversion or interpretation difference;
4. preserve unresolved conflict when no source clearly controls;
5. do not average incompatible authorities merely to produce one answer.

State whether the disagreement is:

* scientific;
* empirical;
* conventional;
* definitional;
* caused by differing domains;
* product-policy-driven.

## Repository Evidence

Repository code, tests, ADRs, comments, and documentation may establish:

* current behavior;
* accepted repository intent;
* historical rationale;
* current configuration;
* existing assumptions.

They do not automatically establish scientific correctness.

Treat old ADRs and generated artifacts as orientation until checked against
current source and stronger authority.

## Missing Authority

When no controlling source exists:

* state the authority gap;
* identify whether the missing evidence is normative, empirical, calibration,
  or product policy;
* avoid manufacturing precision or certainty;
* recommend the smallest evidence-gathering step that could resolve the gap;
* classify the conclusion appropriately, such as `empirical calibration
  required` or `competing scientific authorities unresolved`.

## Required Output

Report a ranked source hierarchy containing only material sources.

For each controlling conclusion, make it possible to trace:

> conclusion → controlling source or derivation → assumptions → applicable
> domain → uncertainty

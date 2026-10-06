# Independent Golden Vectors

Use this reference whenever correctness depends on numeric outputs,
classifications, boundaries, conversions, interpolation, rounding, tolerances,
sentinels, or state transitions.

## Independence Requirement

Golden vectors must not be copied or derived solely from:

* the implementation under test;
* current unit tests;
* implementation-generated fixtures;
* comments or documentation that merely restate the implementation;
* the same algorithm translated into another language.

Prefer:

* analytical derivation;
* governing specification examples;
* authoritative reference datasets;
* trusted reference implementations with independent provenance;
* independently calculated values;
* experimentally validated calibration data.

When independent derivation is unavailable, report `independent oracle missing`
instead of manufacturing expectations.

## Required Vector Fields

For every vector, record:

| Field | Required content |
| --- | --- |
| Case | Human-readable case name |
| Inputs | All material input values |
| Units | Units and reference frames for inputs and outputs |
| Domain | Applicable operating domain and assumptions |
| Expected result | Numeric output, classification, sentinel, or transition |
| Tolerance | Allowed numeric or categorical tolerance |
| Boundary rule | Inclusive/exclusive and tie behavior, when applicable |
| Derivation | Independent derivation or calculation |
| Provenance | Authority, dataset, specification, or independent method |
| Uncertainty | Known uncertainty and calibration limit |
| Failure sensitivity | What wrong model or implementation this vector detects |

## Minimum Case Coverage

Include applicable cases:

* ordinary representative case;
* minimum valid input;
* maximum valid input;
* exact boundary;
* just below boundary;
* just above boundary;
* zero;
* negative input;
* missing input;
* malformed input;
* invalid or out-of-domain input;
* sentinel or unavailable state;
* classification transition;
* interpolation point;
* extrapolation attempt;
* rounding-sensitive value;
* tolerance-sensitive value;
* known reference case;
* uncertainty-sensitive case.

Do not create irrelevant vectors merely to fill a checklist.

## Boundary Cases

For every material threshold or class boundary, include:

1. exact-boundary input;
2. smallest meaningful value below;
3. smallest meaningful value above;
4. expected comparison operator;
5. expected class or output;
6. tolerance and rounding assumptions.

This is required to detect off-by-one, wrong-comparison, and premature-rounding
errors.

## Conversion Cases

For unit or reference-frame conversions, include:

* identity or zero-offset case where applicable;
* one authoritative reference case;
* one independently calculated case;
* a precision-sensitive case;
* dimensional validation.

## Probabilistic and Empirical Models

For probabilistic, statistical, or calibrated behavior:

* distinguish exact mathematical expectations from empirical confidence
  intervals;
* state sample population and calibration domain;
* avoid deterministic expected values where only a distribution is supported;
* include tolerance or acceptance bands;
* identify domain shift and censoring risks.

## Classification Models

For classification vectors:

* state whether classes are ordinal, nominal, probabilistic, or operational;
* include every class boundary;
* include missing and insufficient-evidence states;
* verify classes are mutually exclusive where required;
* verify unsupported probability or confidence implications are not introduced.

## Completion Rule

A scientific review involving numeric or classified behavior is incomplete
until representative independent vectors are reported or the missing oracle is
explicitly documented.

The final response need not include every possible vector. It must include
enough representative vectors to establish the contract and identify the
remaining vector work.

# Scientific Model Contract

Use this reference to derive a precise, testable scientific or model contract.

## Contract Elements

Define every applicable element.

### Variables and symbols

For each variable, state:

* name and symbol;
* physical, mathematical, or statistical meaning;
* scalar, vector, categorical, temporal, or probabilistic type;
* source or derivation.

### Units and reference frames

State:

* input and output units;
* dimensional consistency;
* bandwidth or normalization reference;
* coordinate, temporal, or spatial frame;
* conversions and constants;
* rounding introduced by conversion.

Reject dimensionally invalid operations.

### Valid domain

Define:

* minimum and maximum valid values;
* inclusive and exclusive endpoints;
* valid categories;
* supported modes, bands, regions, time ranges, or populations;
* assumptions required for validity.

### Invalid and unavailable states

Define:

* missing input;
* malformed input;
* out-of-domain input;
* unavailable authority;
* insufficient evidence;
* invalid calculation;
* sentinel values;
* fail-open or fail-closed behavior.

Do not collapse insufficient evidence into an adverse scientific prediction.

### Boundaries and thresholds

For every boundary or threshold, state:

* exact value;
* units;
* inclusive or exclusive comparison;
* tie behavior;
* just-below and just-above behavior;
* source or calibration basis;
* uncertainty;
* whether it is scientifically derived, empirically calibrated, conventional,
  or product policy.

### Interpolation and extrapolation

Define:

* interpolation method;
* valid interpolation interval;
* continuity assumptions;
* extrapolation policy;
* clipping or saturation;
* unsupported regions.

Do not imply validity outside the calibrated or authoritative domain.

### Rounding and precision

Define:

* calculation precision;
* display precision;
* rounding mode;
* order of operations;
* tolerance;
* whether displayed precision exceeds model accuracy.

### Classifications and transitions

For each class, state:

* semantic meaning;
* numeric or logical entry criteria;
* exit criteria;
* precedence;
* tie behavior;
* transition behavior;
* hysteresis, if any;
* whether the class is ordinal, probabilistic, physical, or operational.

Do not use probability or confidence language for uncalibrated ordinal classes.

### Uncertainty and calibration

State:

* uncertainty source;
* statistical or empirical basis;
* calibration population;
* calibration interval;
* expected error or tolerance;
* domain-shift risk;
* confidence representation;
* unsupported precision or universality.

### Invariants

Identify applicable invariants, such as:

* dimensional consistency;
* monotonicity;
* continuity;
* boundedness;
* conservation;
* deterministic ordering;
* classification exclusivity;
* probability normalization;
* stable sentinel behavior.

## Competing Contracts

When multiple contracts are plausible, compare:

* scientific validity;
* authority;
* domain fit;
* unit correctness;
* boundary behavior;
* calibration;
* uncertainty;
* falsifiability;
* operational consequences;
* compatibility with current claims.

Rank them when evidence permits.

## Contract Traceability

Every material rule must be traceable to one of:

* controlling authority;
* independent derivation;
* empirical calibration;
* explicit assumption;
* accepted product policy.

Do not present an assumption or product policy as scientific fact.

## Required Output

The resulting contract should be precise enough that an independent reviewer
could derive test vectors without reading the implementation.

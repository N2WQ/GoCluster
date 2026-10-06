# Scientific Claims and Confidence Review

Use this reference when evaluating scientific, statistical, predictive,
classification, calibration, accuracy, confidence, probability, equivalence, or
operator-facing claims.

## Claim Decomposition

For each material claim, identify:

* exact wording;
* subject and output;
* applicable domain;
* whether the claim is descriptive, predictive, causal, probabilistic,
  comparative, classificatory, or operational;
* evidence required;
* evidence actually available;
* uncertainty;
* unsupported implications.

Separate claims about:

* current implementation behavior;
* scientific validity;
* empirical calibration;
* operational usefulness;
* probability or confidence;
* product policy.

## Confidence Assessment

State confidence using evidence, not rhetorical certainty.

A confidence statement must identify:

* controlling evidence;
* quality and independence of that evidence;
* domain applicability;
* calibration coverage;
* residual disagreement;
* sensitivity to assumptions;
* missing evidence;
* what would change the conclusion.

Avoid numeric confidence percentages unless they are themselves calibrated.

Qualitative confidence may be used when clearly justified:

* high confidence;
* moderate confidence;
* low confidence;
* insufficient evidence.

## Common Overclaims

Actively challenge claims involving:

* precision beyond model accuracy;
* calibrated probability without outcome calibration;
* confidence inferred from sample count alone;
* universality outside the observed domain;
* scientific equivalence based only on matching units;
* causality from correlation;
* operational success from decoder sensitivity;
* physical impossibility from low median conditions;
* literal closure from a heuristic gate;
* accuracy inferred from code and tests agreeing;
* improved prediction without held-out evaluation;
* stability inferred from average-case behavior;
* confidence labels attached to ordinal classes.

## Calibration Review

When calibration is claimed, require:

* defined target outcome;
* representative attempted-event population;
* training and held-out evaluation separation;
* class-conditional outcomes;
* false-positive and false-negative rates;
* calibration curves or equivalent evidence;
* uncertainty intervals;
* segmentation by material operating conditions;
* domain-shift analysis;
* explicit recalibration or expiry policy.

Without this evidence, describe thresholds as heuristic, conventional, or
policy-based rather than calibrated.

## Prediction Review

For predictive claims, distinguish:

* model input conditions;
* forecast horizon;
* median versus percentile behavior;
* conditional probability;
* observed reporting conditions;
* attempted-event success;
* operator-specific outcomes.

Do not equate a model-derived median or score with event probability unless the
mapping is calibrated.

## Scientific Recommendation Versus Product Policy

When the evidence supports one scientific contract but product policy may
differ, state:

> Scientific recommendation: <evidence-supported claim or contract>
>
> Product-policy decision required: <whether to adopt it or intentionally use a
> different behavior with documented limitations>

Do not treat scientific recommendation as product authorization.

## Strongest Supported Claim

Conclude with:

1. the strongest defensible claim;
2. claims that must be narrowed or removed;
3. confidence and uncertainty;
4. applicable domain;
5. evidence still required;
6. what would change the conclusion.

## Disposition Examples

Use dispositions such as:

* `claim supported within bounded domain`;
* `claim supported with bounded uncertainty`;
* `claim exceeds available evidence`;
* `probability implication unsupported`;
* `empirical calibration required`;
* `scientific recommendation pending product-policy decision`;
* `independence limitation reduces confidence`;
* `current wording is operational policy, not scientific fact`.

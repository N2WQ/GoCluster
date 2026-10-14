// File role: Owns bounded arithmetic for path evidence clocks and aging.
// Crawler notes: Differences saturate instead of wrapping; aged mass is formed
// before histogram rounding, including when the unit decay factor underflows.
// Related docs: docs/decisions/ADR-0300-path-observation-time-accounting.md.
// Related tests: accounting_math_test.go, observation_accounting_test.go.
package pathreliability

import "math"

func nonnegativeSecondsSince(now, then int64) int64 {
	if now <= then {
		return 0
	}
	if then < 0 && now > math.MaxInt64+then {
		return math.MaxInt64
	}
	return now - then
}

func ceilNonnegativeInt64(value float64) int64 {
	if !(value > 0) {
		return 0
	}
	if value >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(math.Ceil(value))
}

// agedContribution keeps the effective count separate from weighted mass. A
// unit factor can round to zero in float64 while a large finite input still
// produces usable float32 mass. Scaling the exponent first preserves that mass.
func agedContribution(weight float64, ageSec int64, halfLifeSec int) (mass, count float64) {
	if ageSec <= 0 || halfLifeSec <= 0 {
		return weight, 1
	}
	exponent := -float64(ageSec) / float64(halfLifeSec)
	count = math.Exp2(exponent)
	return decayedMass(weight, ageSec, halfLifeSec, count), count
}

func decayedMass(weight float64, ageSec int64, halfLifeSec int, factor float64) float64 {
	if factor >= 0x1p-1022 || weight == 0 {
		return weight * factor
	}
	fraction, weightExponent := math.Frexp(weight)
	return fraction * math.Exp2(-float64(ageSec)/float64(halfLifeSec)+float64(weightExponent))
}

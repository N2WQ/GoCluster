// File role: Defines recent usable report support independently of signal mass.
// Crawler notes: Reports decay at the bucket accounting reference; cap fractions
// reduce admitted units. These quantities measure volume, not independent
// samples or calibrated confidence. Qualification precedes display weighting.
// Related docs: pathreliability/README.md, docs/path-reliability-first-wave-execution.md.
package pathreliability

import "math"

// ReportSupport carries fractional observed report units. RawUsable includes
// numerically usable input even when receiver caps later reject it. Admitted
// measures accepted cap fractions and is unavailable while cap tracking is off.
// Enforced selects the gate's quantity without substituting raw units for zero
// or unavailable admitted support. Lifetime integer counts remain diagnostics.
type ReportSupport struct {
	RawUsable, Admitted         float64
	AdmittedAvailable, Enforced bool
}

// ActiveUnits returns the unrounded quantity selected by the configured mode.
// Availability is explicit; an unavailable enforced quantity has no raw fallback.
func (support ReportSupport) ActiveUnits() (float64, bool) {
	if support.Enforced {
		return support.Admitted, support.AdmittedAvailable
	}
	return support.RawUsable, true
}

func reportSupportForMode(mode string) ReportSupport {
	return ReportSupport{AdmittedAvailable: mode != ReceiverContributionOff,
		Enforced: mode == ReceiverContributionEnforce}
}

// The inputs describe distinct selected populations. Shared physical buckets
// are collapsed before this sum; display coefficients never scale report units.
func mergeReportSupport(left, right ReportSupport, mode string) ReportSupport {
	support := reportSupportForMode(mode)
	support.RawUsable = left.RawUsable + right.RawUsable
	if support.AdmittedAvailable {
		support.Admitted = left.Admitted + right.Admitted
	}
	return support
}

func (support ReportSupport) meetsMinimum(minimum int) bool {
	units, available := support.ActiveUnits()
	return available && reportUnitsMeetMinimum(units, minimum)
}

func reportUnitsMeetMinimum(units float64, minimum int) bool {
	if units < 0 || math.IsNaN(units) || math.IsInf(units, 0) {
		return false
	}
	if minimum <= 0 {
		return true
	}
	threshold := float64(minimum)
	// Configured integer floors have no user-setting upper bound. Above 2^53,
	// conversion may round downward: choose the least float >= the exact int.
	// A positive int fits below 2^63, whose float value safely fits in uint64.
	if uint64(threshold) < uint64(minimum) {
		threshold = math.Nextafter(threshold, math.Inf(1))
	}
	return units >= threshold
}

package filter

import "dxcluster/strutil"

const (
	DedupePolicyFast = "FAST"
	DedupePolicyMed  = "MED"
	DedupePolicySlow = "SLOW"
)

// IsValidDedupePolicy reports whether value names a supported secondary policy.
func IsValidDedupePolicy(value string) bool {
	switch strutil.NormalizeUpper(value) {
	case DedupePolicyFast, DedupePolicyMed, DedupePolicySlow:
		return true
	default:
		return false
	}
}

// NormalizeDedupePolicy returns a supported policy label, defaulting to MED for
// callers that do not supply a YAML-owned default policy.
func NormalizeDedupePolicy(value string) string {
	return NormalizeDedupePolicyOrDefault(value, DedupePolicyMed)
}

// NormalizeDedupePolicyOrDefault returns a supported policy label, using the
// provided default policy when value is blank or invalid. Invalid defaults fall
// back to MED at non-config-aware boundaries.
func NormalizeDedupePolicyOrDefault(value, defaultPolicy string) string {
	trimmed := strutil.NormalizeUpper(value)
	switch trimmed {
	case DedupePolicyFast, DedupePolicyMed, DedupePolicySlow:
		return trimmed
	default:
		fallback := strutil.NormalizeUpper(defaultPolicy)
		if IsValidDedupePolicy(fallback) {
			return fallback
		}
		return DedupePolicyMed
	}
}

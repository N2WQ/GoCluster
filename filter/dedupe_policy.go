package filter

import "dxcluster/strutil"

const (
	DedupePolicyFast = "FAST"
	DedupePolicyMed  = "MED"
	DedupePolicySlow = "SLOW"
)

// NormalizeDedupePolicy returns a supported policy label, defaulting to MED.
func NormalizeDedupePolicy(value string) string {
	trimmed := strutil.NormalizeUpper(value)
	switch trimmed {
	case DedupePolicyFast, DedupePolicyMed, DedupePolicySlow:
		return trimmed
	default:
		return DedupePolicyMed
	}
}

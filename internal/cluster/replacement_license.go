// File role: Checks replacement eligibility without changing the entering Spot.
// The normal final gate remains authoritative if facts change after this check.
package cluster

import (
	"dxcluster/cty"
	"dxcluster/spot"
	"dxcluster/uls"
)

func replacementLicenseRejected(s *spot.Spot, replacement string, db *cty.CTYDatabase, cache *callMetaCache) bool {
	if s == nil || db == nil {
		return false
	}
	// Jurisdiction follows the operator's home base, not a portable location.
	// Use the same bounded CTY and license caches as ordinary final validation.
	base := uls.NormalizeForLicense(replacement)
	info := effectivePrefixInfo(db, cache, base)
	if info == nil || s.IsBeacon || !uls.LicenseChecksEnabledForADIF(info.ADIF) {
		return false
	}
	return uls.ShouldRejectDXLicense(info.ADIF, base, s.IsBeacon, uls.LookupForADIF(info.ADIF, base))
}

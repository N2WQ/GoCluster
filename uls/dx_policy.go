package uls

// ShouldRejectDXLicense is the common replacement/final-DX rejection policy.
// Facts remain independent of enforcement. Unavailable authority fails open;
// a known absence rejects only covered, enforced, non-beacon station identities
// without a jurisdiction-qualified exception. The caller supplies home-base
// CTY jurisdiction rather than the portable operating location.
func ShouldRejectDXLicense(adif int, call string, beacon bool, result LookupResult) bool {
	return !beacon && LicenseChecksEnabledForADIF(adif) && result.Available && !result.Found &&
		!AllowlistMatch(adif, NormalizeForLicense(call))
}

// File role: Defines the finite US/Canadian mailing-address and base-call
// jurisdiction contracts shared by enrichment, filtering and archive validation.
package spot

// FCCStateCodes returns the complete FCC mailing-address vocabulary. Each
// caller owns its copy; no mutable shared map or runtime registry is retained.
func FCCStateCodes() []string {
	codes := [...]string{
		"AA", "AE", "AK", "AL", "AP", "AR", "AS", "AZ", "CA", "CO",
		"CT", "DC", "DE", "FL", "GA", "GU", "HI", "IA", "ID", "IL",
		"IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO",
		"MP", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV",
		"NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN",
		"TX", "UM", "UT", "VA", "VI", "VT", "WA", "WI", "WV", "WY",
	}
	return codes[:]
}

// IsFCCState accepts canonical codes only. Human commands and FCC imports
// normalize at their boundary; exact machine and stored configurations do not.
func IsFCCState(code string) bool {
	switch code {
	case "AA", "AE", "AK", "AL", "AP", "AR", "AS", "AZ", "CA", "CO",
		"CT", "DC", "DE", "FL", "GA", "GU", "HI", "IA", "ID", "IL",
		"IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO",
		"MP", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV",
		"NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN",
		"TX", "UM", "UT", "VA", "VI", "VT", "WA", "WI", "WV", "WY":
		return true
	default:
		return false
	}
}

// CanadianProvinceCodes returns the province/territory vocabulary used by ISED.
// Each caller owns its copy, as with FCCStateCodes.
func CanadianProvinceCodes() []string {
	codes := [...]string{"AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"}
	return codes[:]
}

// IsCanadianProvince accepts canonical ISED mailing-address codes only.
func IsCanadianProvince(code string) bool {
	switch code {
	case "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT":
		return true
	default:
		return false
	}
}

// StateCodes returns the sorted vocabulary shared by State filters and storage.
// FCC and ISED import validators retain their narrower source vocabularies.
func StateCodes() []string {
	codes := [...]string{
		"AA", "AB", "AE", "AK", "AL", "AP", "AR", "AS", "AZ", "BC",
		"CA", "CO", "CT", "DC", "DE", "FL", "GA", "GU", "HI", "IA",
		"ID", "IL", "IN", "KS", "KY", "LA", "MA", "MB", "MD", "ME",
		"MI", "MN", "MO", "MP", "MS", "MT", "NB", "NC", "ND", "NE",
		"NH", "NJ", "NL", "NM", "NS", "NT", "NU", "NV", "NY", "OH",
		"OK", "ON", "OR", "PA", "PE", "PR", "QC", "RI", "SC", "SD",
		"SK", "TN", "TX", "UM", "UT", "VA", "VI", "VT", "WA", "WI",
		"WV", "WY", "YT",
	}
	return codes[:]
}

// IsState accepts the canonical US/Canadian vocabulary in shared contracts.
// Human commands normalize first; exact machine and stored rules do not.
func IsState(code string) bool {
	return IsFCCState(code) || IsCanadianProvince(code)
}

// IsFCCJurisdiction identifies current ADIF entities whose base calls use FCC
// licensing. Portable location prefixes do not select this jurisdiction.
// Guantanamo Bay uses Navy licensing; deleted Kingman Reef is not included.
func IsFCCJurisdiction(adif int) bool {
	switch adif {
	case 6, 9, 20, 43, 103, 110, 123, 138, 166, 174, 182, 197, 202, 285, 291, 297, 515:
		return true
	default:
		return false
	}
}

// IsCanadianJurisdiction identifies Canada, Sable Island and St. Paul Island
// base calls that use ISED licensing, regardless of a portable location prefix.
func IsCanadianJurisdiction(adif int) bool {
	switch adif {
	case 1, 211, 252:
		return true
	default:
		return false
	}
}

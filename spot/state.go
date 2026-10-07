// File role: Defines the finite FCC mailing-state and base-call jurisdiction
// contracts shared by enrichment, filtering and archive validation.
package spot

// FCCStateCodes returns the complete approved mailing-address vocabulary. Each
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

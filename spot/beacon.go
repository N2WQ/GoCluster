package spot

import "strings"

var beaconCommentKeywords = []string{"NCDXF", "BEACON"}

// Purpose: Detect beacon markers in a comment string.
// Key aspects: Case-insensitive substring match against known keywords.
// Upstream: RefreshBeaconFlag.
// Downstream: strings.ToUpper and strings.Contains.
// commentContainsBeaconKeyword reports true when the comment mentions well-known beacon markers.
func commentContainsBeaconKeyword(comment string) bool {
	if comment == "" {
		return false
	}
	upper := strings.ToUpper(comment)
	for _, keyword := range beaconCommentKeywords {
		if strings.Contains(upper, keyword) {
			return true
		}
	}
	return false
}

// Purpose: Refresh the IsBeacon flag from DX call and comment.
// Key aspects: Checks /B callsign suffix and known comment keywords.
// Upstream: Spot normalization and output pipeline.
// Downstream: IsBeaconCall and commentContainsBeaconKeyword.
// RefreshBeaconFlag recalculates the IsBeacon flag using the DX call and latest comment text.
func (s *Spot) RefreshBeaconFlag() {
	if s == nil {
		return
	}
	s.IsBeacon = IsBeaconCall(s.DXCall) || commentContainsBeaconKeyword(s.Comment)
}

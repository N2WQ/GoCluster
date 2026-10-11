package peer

import (
	"strconv"

	"dxcluster/spot"
)

// Local publication uses the accepted source Spot, independently of local
// correction. An unrepresentable original refuses only this peer variant;
// comment rewriting or modern-to-legacy fallback would change its content.
func formatPC61(s *spot.Spot, origin string, hop int) string {
	return localPeerSpotSentence(s, "PC61", origin, hop)
}

func formatPC11(s *spot.Spot, origin string, hop int) string {
	return localPeerSpotSentence(s, "PC11", origin, hop)
}

// The shared received-frame admission rule also governs generated peer spots.
// Fixed fields borrow the immutable source only during formatting; the returned
// bounded sentence owns its bytes before entering the existing session queue.
func localPeerSpotSentence(s *spot.Spot, kind, origin string, hop int) string {
	if s == nil {
		return ""
	}
	fields := [...]string{
		strconv.FormatFloat(s.Frequency, 'f', -1, 64), s.DXCall,
		s.Time.UTC().Format("02-Jan-2006"), s.Time.UTC().Format("1504Z"),
		s.Comment, s.DECall, origin, s.SpotterIP,
	}
	count := 7
	if kind == "PC61" {
		count = 8
	}
	frame := Frame{Type: kind, Fields: fields[:count], Hop: hop}
	if _, err := validateOriginalPeerSpot(&frame); err != nil {
		return ""
	}
	return originalPeerSpotSentence(&frame, hop, false)
}

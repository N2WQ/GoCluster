package peer

import (
	"fmt"
	"strings"

	"dxcluster/spot"
)

// Purpose: Render a DX spot as a PC61 line for PC9X peers.
// Key aspects: Sanitizes comment and formats dates/times in DXSpider style.
// Upstream: Peer session writer.
// Downstream: sanitizeComment, fmt.Sprintf.
func formatPC61(s *spot.Spot, origin string, hop int) string {
	comment := sanitizeComment(s.Comment)
	spotterIP := strings.TrimSpace(s.SpotterIP)
	return fmt.Sprintf("PC61^%.1f^%s^%s^%s^%s^%s^%s^%s^H%d^",
		s.Frequency,
		s.DXCall,
		s.Time.UTC().Format("02-Jan-2006"),
		s.Time.UTC().Format("1504Z"),
		comment,
		s.DECall,
		origin,
		spotterIP,
		hop,
	)
}

// Purpose: Render a DX spot as a legacy PC11 line.
// Key aspects: Sanitizes comment and formats dates/times in DXSpider style.
// Upstream: Peer session writer for legacy peers.
// Downstream: sanitizeComment, fmt.Sprintf.
func formatPC11(s *spot.Spot, origin string, hop int) string {
	comment := sanitizeComment(s.Comment)
	return fmt.Sprintf("PC11^%.1f^%s^%s^%s^%s^%s^%s^H%d^",
		s.Frequency,
		s.DXCall,
		s.Time.UTC().Format("02-Jan-2006"),
		s.Time.UTC().Format("1504Z"),
		comment,
		s.DECall,
		origin,
		hop,
	)
}

// Purpose: Sanitize comments for PC lines.
// Key aspects: Strips control delimiters and line breaks.
// Upstream: formatPC61, formatPC11.
// Downstream: strings.ReplaceAll.
func sanitizeComment(c string) string {
	c = strings.ReplaceAll(c, "^", " ")
	c = strings.ReplaceAll(c, "\r", " ")
	c = strings.ReplaceAll(c, "\n", " ")
	return strings.TrimSpace(c)
}

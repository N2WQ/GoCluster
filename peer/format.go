package peer

import (
	"fmt"
	"strings"

	"dxcluster/spot"
)

// formatPC61 renders a DX spot as PC61 (preferred for PC9X peers).
func formatPC61(s *spot.Spot, origin string, hop int) string {
	comment := sanitizeComment(s.Comment)
	return fmt.Sprintf("PC61^%.1f^%s^%s^%s^%s^%s^%s^%s^H%d^",
		s.Frequency,
		s.DXCall,
		s.Time.UTC().Format("02-Jan-2006"),
		s.Time.UTC().Format("1504Z"),
		comment,
		s.DECall,
		origin,
		"", // user IP omitted
		hop,
	)
}

// formatPC11 renders a DX spot as PC11 (legacy).
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

func sanitizeComment(c string) string {
	c = strings.ReplaceAll(c, "^", " ")
	c = strings.ReplaceAll(c, "\r", " ")
	c = strings.ReplaceAll(c, "\n", " ")
	return strings.TrimSpace(c)
}

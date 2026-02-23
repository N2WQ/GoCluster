package strutil

import (
	"strings"
)

// NormalizeUpper trims surrounding whitespace and converts to upper case.
// Use for callsigns, modes, and other tokens where case is not significant.
func NormalizeUpper(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

// NormalizeLower trims surrounding whitespace and converts to lower case.
func NormalizeLower(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

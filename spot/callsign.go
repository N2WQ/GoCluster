package spot

import (
	"regexp"
	"strings"
	"unicode"
)

var callsignPattern = regexp.MustCompile(`^[A-Z0-9]+(?:[/-][A-Z0-9#]+)*$`)

// NormalizeCallsign uppercases the string, trims whitespace, and removes trailing dots or slashes.
func NormalizeCallsign(call string) string {
	normalized := strings.ToUpper(strings.TrimSpace(call))
	normalized = strings.ReplaceAll(normalized, ".", "/")
	normalized = strings.TrimSuffix(normalized, "/")
	return strings.TrimSpace(normalized)
}

func validateNormalizedCallsign(call string) bool {
	if call == "" {
		return false
	}
	if len(call) < 3 || len(call) > 10 {
		return false
	}
	if strings.IndexFunc(call, unicode.IsDigit) < 0 {
		return false
	}
	return callsignPattern.MatchString(call)
}

// IsValidCallsign applies format checks to make sure it looks like a valid amateur call.
func IsValidCallsign(call string) bool {
	normalized := NormalizeCallsign(call)
	return validateNormalizedCallsign(normalized)
}

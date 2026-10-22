package filter

import "strings"

// ValidGrid2Prefix accepts exactly one canonical Maidenhead field prefix.
func ValidGrid2Prefix(value string) bool {
	return len(value) == 2 && value[0] >= 'A' && value[0] <= 'R' && value[1] >= 'A' && value[1] <= 'R'
}

// ValidCallsignPattern shares the exact/prefix/suffix admission domain of the
// existing matcher. A lone star is valid and matches any nonempty known call.
func ValidCallsignPattern(value string) bool {
	if value == "" || value != strings.TrimSpace(value) {
		return false
	}
	if strings.Count(value, "*") > 1 || (strings.Contains(value, "*") && !strings.HasPrefix(value, "*") && !strings.HasSuffix(value, "*")) {
		return false
	}
	for i := range len(value) {
		b := value[i]
		if (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '/' || b == '-' || b == '*' {
			continue
		}
		return false
	}
	return true
}

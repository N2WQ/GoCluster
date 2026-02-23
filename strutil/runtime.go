package strutil

import "time"

// FormatAge returns a non-negative age string truncated to seconds.
func FormatAge(now time.Time, at time.Time) string {
	if at.IsZero() {
		return "never"
	}
	age := now.Sub(at)
	if age < 0 {
		age = 0
	}
	if age < time.Second {
		return "0s"
	}
	return age.Truncate(time.Second).String()
}

// IsAllDigitsASCII reports whether s is non-empty and only [0-9].
func IsAllDigitsASCII(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// File role: Own the required terminal timestamp of a human telnet spot.
// Related code: client.go structural parsing and legacy UTC/skew conversion.
// This is a wire convention: terminal prose with identical bytes cannot be
// distinguished from a wire time when an upstream omits the actual field.
package rbn

// consumeHumanWireTime removes only a standalone final uppercase HHMMZ clock.
// Missing, wrapped, fused or invalid fields reject the spot before queueing;
// an earlier comment time is never a fallback. Date/skew conversion stays with
// parseTimeFromRBN so this boundary does not invent a rollover policy.
func consumeHumanWireTime(tokens []spotToken, consumed []bool) (string, bool) {
	last := len(tokens) - 1
	if last < 0 || consumed[last] {
		return "", false
	}
	raw := tokens[last].raw
	if len(raw) != 5 || raw[4] != 'Z' {
		return "", false
	}
	for i := 0; i < 4; i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return "", false
		}
	}
	if (raw[0]-'0')*10+raw[1]-'0' > 23 || (raw[2]-'0')*10+raw[3]-'0' > 59 {
		return "", false
	}
	consumed[last] = true
	return raw, true
}

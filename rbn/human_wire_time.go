// File role: Own human telnet terminal-time grammar and bounded rejection diagnostics.
// Related code: client.go structural parsing and legacy UTC/skew conversion.
// This is a wire convention: terminal prose with identical bytes cannot be
// distinguished from a wire time when an upstream omits the actual field.
package rbn

import (
	"log"
	"strconv"
	"time"
)

const humanWireTimeRejectLogInterval = 30 * time.Second

// reportHumanWireTimeReject counts only the terminal-time gate, not all parse
// failures. No source bytes are retained and no new worker is involved. The
// existing bounded logger may drop an admitted sample; the total is independent.
func (c *Client) reportHumanWireTimeReject(line string) {
	total, emit := c.humanWireTimeRejects.Inc()
	if !emit {
		return
	}
	// QuoteToASCII expands each source byte by at most four bytes. These fixed
	// limits keep the complete production record below 2 KiB, even for invalid
	// UTF-8/control bytes. Head/tail sampling keeps terminal framing evidence.
	const sourceLimit, edgeBytes = 64, 128
	source := c.displayName()
	if len(source) > sourceLimit {
		source = source[:sourceLimit] + "..."
	}
	if len(line) > 2*edgeBytes {
		log.Printf("Human telnet reject source=%s reason=unusable_wire_time total=%d truncated=true line=%s ... %s",
			strconv.QuoteToASCII(source), total,
			strconv.QuoteToASCII(line[:edgeBytes]), strconv.QuoteToASCII(line[len(line)-edgeBytes:]))
		return
	}
	log.Printf("Human telnet reject source=%s reason=unusable_wire_time total=%d truncated=false line=%s",
		strconv.QuoteToASCII(source), total, strconv.QuoteToASCII(line))
}

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

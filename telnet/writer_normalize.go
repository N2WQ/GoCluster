package telnet

import (
	"slices"
	"strings"
)

// appendWriterNormalized preserves normalizeOutboundLine's byte transform
// without retaining an intermediate string. The writer owns batch. Reserve
// the complete record before appending so its existing threshold/overshoot
// policy is unchanged; CRLF detection never crosses a message boundary.
func appendWriterNormalized(batch []byte, message string) []byte {
	lineFeeds := strings.Count(message, "\n")
	if lineFeeds == 0 {
		return append(batch, message...)
	}
	extra := lineFeeds - strings.Count(message, "\r\n")
	batch = slices.Grow(batch, len(message)+extra)
	start := 0
	for {
		relative := strings.IndexByte(message[start:], '\n')
		if relative < 0 {
			return append(batch, message[start:]...)
		}
		at := start + relative
		batch = append(batch, message[start:at]...)
		if at == 0 || message[at-1] != '\r' {
			batch = append(batch, '\r')
		}
		batch = append(batch, '\n')
		start = at + 1
	}
}

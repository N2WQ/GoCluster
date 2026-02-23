// Package linebuffer provides helpers for line-oriented writer buffering.
package linebuffer

import "bytes"

// AppendAndExtractLines appends p into buf, returns complete lines, and keeps
// the incomplete trailing remainder. Lines are split on '\n' and have trailing
// '\r' removed. If the trailing remainder exceeds maxRemainder, it is emitted
// as one line and the remainder is cleared to keep memory bounded.
func AppendAndExtractLines(buf []byte, p []byte, maxRemainder int) (remaining []byte, lines []string) {
	data := append(buf, p...)
	for {
		idx := bytes.IndexByte(data, '\n')
		if idx == -1 {
			break
		}
		lines = append(lines, string(bytes.TrimRight(data[:idx], "\r")))
		data = data[idx+1:]
	}
	if maxRemainder > 0 && len(data) > maxRemainder {
		trimmed := string(bytes.TrimRight(data, "\r"))
		if trimmed != "" {
			lines = append(lines, trimmed)
		}
		data = data[:0]
	}
	return data, lines
}

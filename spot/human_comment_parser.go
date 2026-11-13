// File role: Own human comment preservation and the narrow FT report shorthand.
// Mode/event recognition is shared with comment_parser.go; this file owns human
// report acceptance and rebuilding text from original, input-bounded tokens.
package spot

import "strings"

// humanExplicitReport binds a whole integer to its own compact/adjacent unit.
// Inspect raw tokens: mode, speed or time cleanup must not create adjacency.
// The selected mode owns its source token even when it resembles a report;
// other alias collisions keep the shared scanner's existing precedence.
// A constant amount of local state scans input-bounded tokens in source order.
func humanExplicitReport(tokens []commentToken, selectedMode int) (int, int, int, bool) {
	for index, token := range tokens {
		if index == selectedMode {
			continue
		}
		text := strings.Trim(token.raw, ",;:!.")
		if _, remainder := peelTimePrefix(text); remainder != text {
			text = remainder
		}
		last := index
		if len(text) > 2 && strings.EqualFold(text[len(text)-2:], "DB") {
			text = text[:len(text)-2]
		} else {
			last++
			if last >= len(tokens) || last == selectedMode || !strings.EqualFold(strings.Trim(tokens[last].raw, ",;:!."), "DB") {
				continue
			}
		}
		if index > 0 {
			previous := strings.Trim(tokens[index-1].raw, ",;:!.")
			if strings.EqualFold(previous, "S9") || previous == "59" {
				continue
			}
		}
		digits := text
		if len(digits) > 0 && (digits[0] == '+' || digits[0] == '-') {
			digits = digits[1:]
		}
		if !isAllDigits(digits) {
			continue
		}
		if value, ok := parseSignedInt(text); ok {
			return index, last, value, true
		}
	}
	return 0, 0, 0, false
}

func humanBareReport(tokens []commentToken, mode string, modeIndex int) (int, int, bool) {
	if modeIndex < 0 || modeIndex+1 >= len(tokens) || (mode != "FT8" && mode != "FT4") || !modeWantsBareReport(mode) {
		return 0, 0, false
	}
	// Legacy scanner offsets intentionally retain their Unicode behavior. A
	// shorthand report additionally needs an actual source mode word, so a
	// shifted scanner match cannot manufacture numeric evidence.
	if sourceMode, ok := CommentModeToken(tokens[modeIndex].upper); !ok || sourceMode != mode {
		return 0, 0, false
	}
	index := modeIndex + 1
	// Inspect the original token, not a time-peeled or otherwise transformed
	// recognition token. Cleanup must never create report adjacency or a sign.
	text := strings.Trim(tokens[index].raw, ",;:!.")
	if len(text) < 2 || (text[0] != '+' && text[0] != '-') || !isAllDigits(text[1:]) {
		return 0, 0, false
	}
	value, ok := parseSignedInt(text)
	if !ok || value < -30 || value > 30 {
		return 0, 0, false
	}
	return index, value, true
}

func buildHumanComment(tokens []commentToken, consumed, preserved []bool) string {
	parts := make([]string, 0, len(tokens))
	for index, token := range tokens {
		text := strings.TrimSpace(strings.Trim(token.raw, ",;:!."))
		if consumed[index] && !preserved[index] {
			// Explicit metadata can occupy a time-fused token, such as
			// 1200Z-14dB. Extract the report without also deleting the time.
			if len(text) <= 5 || !isTimeToken(text[:5]) {
				continue
			}
			text = text[:5]
		}
		if text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, " ")
}

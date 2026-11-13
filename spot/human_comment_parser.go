// File role: Own human comment preservation and the narrow FT report shorthand.
// Recognition is shared with comment_parser.go; this file owns only the human
// acceptance rule and rebuilding text from the original, input-bounded tokens.
package spot

import "strings"

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

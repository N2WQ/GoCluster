// File role: Shares history keyword/comma boundaries between typed parsing and
// borrowed telnet prefixes. State is request-local; no validation, CTY lookup, cursor
// mutation or input reference escapes the scan. Literal phrases begin only at a
// standalone COMMENT clause, never at a supported alias value.
package commands

import "dxcluster/filter"

func historyKeywordIsModeValue(domain string, separatorRequired bool, keyword string) bool {
	return domain == "MODE" && !separatorRequired && filter.IsSupportedMode(keyword)
}

type historyClauseState struct {
	domain            string
	separatorRequired bool
}

// advance only classifies boundaries. The existing typed parser owns invalid
// names, repeated clauses, missing values and malformed list separation.
func (state *historyClauseState) advance(word []byte) bool {
	keyword := ""
	for _, candidate := range [...]string{"COMMENT", "BAND", "MODE"} {
		if historyKeywordEqual(word, candidate) {
			keyword = candidate
			break
		}
	}
	if keyword != "" && !historyKeywordIsModeValue(state.domain, state.separatorRequired, keyword) {
		if keyword == "COMMENT" {
			return true
		}
		state.domain, state.separatorRequired = keyword, false
		return false
	}
	if state.domain != "" {
		for _, b := range word {
			state.separatorRequired = b != ','
		}
	}
	return false
}

func historyKeywordEqual(word []byte, keyword string) bool {
	if len(word) != len(keyword) {
		return false
	}
	for i, b := range word {
		if b >= 'a' && b <= 'z' {
			b -= 'a' - 'A'
		}
		if b != keyword[i] {
			return false
		}
	}
	return true
}

// HistoryCommentClauseStarted scans borrowed ASCII arguments after a recognized
// history prefix. Ingress calls it within its byte limit, after every edit. Keep
// its existing NEXT guard and require a space after the actual COMMENT marker
// before permitting printable punctuation. No per-byte parser allocation occurs.
func HistoryCommentClauseStarted(args []byte) bool {
	var state historyClauseState
	for pos := 0; pos < len(args); {
		for pos < len(args) && args[pos] == ' ' {
			pos++
		}
		start := pos
		for pos < len(args) && args[pos] != ' ' {
			pos++
		}
		word := args[start:pos]
		if len(word) == 0 || historyKeywordEqual(word, "NEXT") {
			return false
		}
		if state.advance(word) {
			return pos < len(args)
		}
	}
	return false
}

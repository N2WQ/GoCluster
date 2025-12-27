package spot

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// CommentParseResult captures parsed metadata from a spot comment.
type CommentParseResult struct {
	Mode      string
	Report    int
	HasReport bool
	TimeToken string
	Comment   string
}

type acTokenKind int

const (
	acTokenUnknown acTokenKind = iota
	acTokenMode
	acTokenDB
	acTokenWPM
	acTokenBPS
)

type acPattern struct {
	word string
	kind acTokenKind
	mode string
}

type acMatch struct {
	start   int
	end     int
	pattern acPattern
}

type acNode struct {
	next    map[byte]int
	fail    int
	outputs []int
}

type acScanner struct {
	patterns []acPattern
	nodes    []acNode
}

func newACScanner(patterns []acPattern) *acScanner {
	sc := &acScanner{
		patterns: patterns,
		nodes:    []acNode{{next: make(map[byte]int)}},
	}
	for idx, p := range patterns {
		state := 0
		for i := 0; i < len(p.word); i++ {
			ch := p.word[i]
			next, ok := sc.nodes[state].next[ch]
			if !ok {
				next = len(sc.nodes)
				sc.nodes = append(sc.nodes, acNode{next: make(map[byte]int)})
				sc.nodes[state].next[ch] = next
			}
			state = next
		}
		sc.nodes[state].outputs = append(sc.nodes[state].outputs, idx)
	}

	queue := make([]int, 0, len(sc.nodes))
	for _, next := range sc.nodes[0].next {
		queue = append(queue, next)
	}
	for len(queue) > 0 {
		state := queue[0]
		queue = queue[1:]
		for ch, next := range sc.nodes[state].next {
			fail := sc.nodes[state].fail
			for fail > 0 {
				if target, ok := sc.nodes[fail].next[ch]; ok {
					fail = target
					break
				}
				fail = sc.nodes[fail].fail
			}
			sc.nodes[next].fail = fail
			sc.nodes[next].outputs = append(sc.nodes[next].outputs, sc.nodes[fail].outputs...)
			queue = append(queue, next)
		}
	}
	return sc
}

func (sc *acScanner) FindAll(text string) []acMatch {
	if sc == nil {
		return nil
	}
	state := 0
	matches := make([]acMatch, 0, 8)
	for i := 0; i < len(text); i++ {
		ch := text[i]
		next, ok := sc.nodes[state].next[ch]
		for !ok && state > 0 {
			state = sc.nodes[state].fail
			next, ok = sc.nodes[state].next[ch]
		}
		if ok {
			state = next
		}
		if len(sc.nodes[state].outputs) == 0 {
			continue
		}
		end := i + 1
		for _, pid := range sc.nodes[state].outputs {
			p := sc.patterns[pid]
			start := end - len(p.word)
			if start >= 0 {
				matches = append(matches, acMatch{start: start, end: end, pattern: p})
			}
		}
	}
	return matches
}

func buildMatchIndex(matches []acMatch) map[int][]acMatch {
	if len(matches) == 0 {
		return nil
	}
	index := make(map[int][]acMatch, len(matches))
	for _, m := range matches {
		index[m.start] = append(index[m.start], m)
	}
	return index
}

func classifyToken(matchIndex map[int][]acMatch, trimStart, trimEnd int) (acPattern, bool) {
	if len(matchIndex) == 0 {
		return acPattern{}, false
	}
	for _, m := range matchIndex[trimStart] {
		if m.end == trimEnd {
			return m.pattern, true
		}
	}
	return acPattern{}, false
}

func classifyTokenWithFallback(matchIndex map[int][]acMatch, tok commentToken) (acPattern, bool) {
	if pat, ok := classifyToken(matchIndex, tok.trimStart, tok.trimEnd); ok {
		return pat, true
	}
	for _, m := range getKeywordScanner().FindAll(tok.upper) {
		if m.start == 0 && m.end == len(tok.upper) {
			return m.pattern, true
		}
	}
	return acPattern{}, false
}

var keywordPatterns = []acPattern{
	{word: "DB", kind: acTokenDB},
	{word: "WPM", kind: acTokenWPM},
	{word: "BPS", kind: acTokenBPS},
	{word: "CW", kind: acTokenMode, mode: "CW"},
	{word: "CWT", kind: acTokenMode, mode: "CW"},
	{word: "RTTY", kind: acTokenMode, mode: "RTTY"},
	{word: "FT8", kind: acTokenMode, mode: "FT8"},
	{word: "FT-8", kind: acTokenMode, mode: "FT8"},
	{word: "FT4", kind: acTokenMode, mode: "FT4"},
	{word: "FT-4", kind: acTokenMode, mode: "FT4"},
	{word: "PSK31", kind: acTokenMode, mode: "PSK31"},
	{word: "MSK", kind: acTokenMode, mode: "MSK144"},
	{word: "MSK144", kind: acTokenMode, mode: "MSK144"},
	{word: "MSK-144", kind: acTokenMode, mode: "MSK144"},
	{word: "USB", kind: acTokenMode, mode: "USB"},
	{word: "LSB", kind: acTokenMode, mode: "LSB"},
	{word: "SSB", kind: acTokenMode, mode: "SSB"},
}

var keywordScannerOnce sync.Once
var keywordScanner *acScanner

func getKeywordScanner() *acScanner {
	keywordScannerOnce.Do(func() {
		keywordScanner = newACScanner(keywordPatterns)
	})
	return keywordScanner
}

type commentToken struct {
	raw       string
	clean     string
	upper     string
	start     int
	end       int
	trimStart int
	trimEnd   int
}

func tokenizeComment(comment string) []commentToken {
	tokens := make([]commentToken, 0, 16)
	i := 0
	for i < len(comment) {
		for i < len(comment) && (comment[i] == ' ' || comment[i] == '\t') {
			i++
		}
		if i >= len(comment) {
			break
		}
		start := i
		for i < len(comment) && comment[i] != ' ' && comment[i] != '\t' {
			i++
		}
		end := i
		raw := comment[start:end]
		trimStart := start
		trimEnd := end
		for trimStart < end {
			if strings.ContainsRune(",;:!.", rune(comment[trimStart])) {
				trimStart++
			} else {
				break
			}
		}
		for trimEnd > trimStart {
			if strings.ContainsRune(",;:!.", rune(comment[trimEnd-1])) {
				trimEnd--
			} else {
				break
			}
		}
		clean := comment[trimStart:trimEnd]
		tokens = append(tokens, commentToken{
			raw:       raw,
			clean:     clean,
			upper:     strings.ToUpper(clean),
			start:     start,
			end:       end,
			trimStart: trimStart,
			trimEnd:   trimEnd,
		})
	}
	return tokens
}

var snrPattern = regexp.MustCompile(`(?i)([-+]?\d{1,3})\s*dB`)

func parseSignedInt(tok string) (int, bool) {
	if tok == "" {
		return 0, false
	}
	if strings.Contains(tok, ".") {
		return 0, false
	}
	v, err := strconv.Atoi(tok)
	if err != nil {
		return 0, false
	}
	if v < -200 || v > 200 {
		return 0, false
	}
	return v, true
}

func parseInlineSNR(tok string) (int, bool) {
	lower := strings.ToLower(strings.TrimSpace(tok))
	if !strings.HasSuffix(lower, "db") {
		return 0, false
	}
	numStr := strings.TrimSuffix(lower, "db")
	if strings.Contains(numStr, ".") || numStr == "" {
		return 0, false
	}
	v, err := strconv.Atoi(numStr)
	if err != nil || v < -200 || v > 200 {
		return 0, false
	}
	return v, true
}

func peelTimePrefix(tok string) (string, string) {
	if len(tok) < 5 {
		return "", tok
	}
	prefix := tok[:5]
	if isTimeToken(prefix) {
		return prefix, strings.TrimSpace(tok[5:])
	}
	return "", tok
}

func isTimeToken(tok string) bool {
	if len(tok) != 5 {
		return false
	}
	if tok[4] != 'Z' && tok[4] != 'z' {
		return false
	}
	return isAllDigits(tok[:4])
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func buildComment(tokens []commentToken, consumed []bool) string {
	parts := make([]string, 0, len(tokens))
	for i, tok := range tokens {
		if consumed[i] {
			continue
		}
		clean := strings.TrimSpace(tok.clean)
		if clean == "" {
			continue
		}
		parts = append(parts, clean)
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ")
}

// ParseSpotComment extracts explicit mode tokens, report/time/speed tags, and a cleaned comment.
// When no explicit mode token is present, Mode is left empty so downstream mode
// assignment can apply history and allocation logic.
func ParseSpotComment(comment string, freq float64) CommentParseResult {
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return CommentParseResult{}
	}

	tokens := tokenizeComment(comment)
	consumed := make([]bool, len(tokens))
	matchIndex := buildMatchIndex(getKeywordScanner().FindAll(strings.ToUpper(comment)))

	var (
		mode            string
		report          int
		hasReport       bool
		timeToken       string
		speedValue      string
		speedUnit       string
		pendingNumIdx   = -1
		pendingNumValue int
	)

	for idx := 0; idx < len(tokens); idx++ {
		tok := tokens[idx]
		originalClean := tok.clean
		clean := originalClean
		if timeToken == "" {
			if ts, remainder := peelTimePrefix(clean); ts != "" {
				timeToken = ts
				shift := len(originalClean) - len(remainder)
				clean = remainder
				tokens[idx].clean = remainder
				tokens[idx].upper = strings.ToUpper(remainder)
				tokens[idx].trimStart = tok.trimStart + shift
				tokens[idx].trimEnd = tokens[idx].trimStart + len(remainder)
				tok = tokens[idx]
			}
		}
		if clean == "" {
			consumed[idx] = true
			continue
		}
		if isTimeToken(clean) {
			if timeToken == "" {
				timeToken = clean
			}
			consumed[idx] = true
			pendingNumIdx = -1
			continue
		}

		if pat, ok := classifyTokenWithFallback(matchIndex, tok); ok {
			switch pat.kind {
			case acTokenMode:
				if mode == "" {
					mode = NormalizeVoiceMode(pat.mode, freq)
					consumed[idx] = true
					continue
				}
			case acTokenDB:
				if !hasReport && pendingNumIdx >= 0 {
					report = pendingNumValue
					hasReport = true
					consumed[idx] = true
					consumed[pendingNumIdx] = true
					pendingNumIdx = -1
					continue
				}
				consumed[idx] = true
				continue
			case acTokenWPM, acTokenBPS:
				if speedValue == "" && pendingNumIdx >= 0 {
					speedValue = tokens[pendingNumIdx].clean
					if pat.kind == acTokenWPM {
						speedUnit = "WPM"
					} else {
						speedUnit = "BPS"
					}
					consumed[idx] = true
					consumed[pendingNumIdx] = true
					pendingNumIdx = -1
					continue
				}
			}
		}

		if !hasReport {
			if v, ok := parseInlineSNR(clean); ok {
				report = v
				hasReport = true
				consumed[idx] = true
				continue
			}
		}

		if pendingNumIdx == -1 {
			if v, ok := parseSignedInt(clean); ok {
				pendingNumIdx = idx
				pendingNumValue = v
				continue
			}
		}
	}

	explicitMode := NormalizeVoiceMode(mode, freq)
	if !hasReport && pendingNumIdx >= 0 && explicitMode != "" && modeWantsBareReport(explicitMode) {
		// Treat bare 73/88 as greetings, not SNR, unless explicitly tagged with dB.
		if pendingNumValue != 73 && pendingNumValue != 88 {
			report = pendingNumValue
			hasReport = true
			consumed[pendingNumIdx] = true
		}
	}

	cleaned := buildComment(tokens, consumed)
	if !hasReport && cleaned != "" {
		if m := snrPattern.FindStringSubmatch(cleaned); len(m) == 2 {
			if v, err := strconv.Atoi(m[1]); err == nil {
				report = v
				hasReport = true
			}
		}
	}
	if speedValue != "" && speedUnit != "" {
		speedLabel := speedValue + " " + speedUnit
		if cleaned != "" {
			cleaned = speedLabel + " " + cleaned
		} else {
			cleaned = speedLabel
		}
	}

	return CommentParseResult{
		Mode:      explicitMode,
		Report:    report,
		HasReport: hasReport,
		TimeToken: timeToken,
		Comment:   cleaned,
	}
}

// modeWantsBareReport determines which modes treat bare signed integers as SNR/report.
func modeWantsBareReport(mode string) bool {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "CW", "RTTY", "FT8", "FT4", "MSK144":
		return true
	}
	return false
}

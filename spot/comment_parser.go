package spot

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// CommentParseResult captures parsed metadata from a spot comment.
type CommentParseResult struct {
	Mode      string
	Report    int
	HasReport bool
	TimeToken string
	Comment   string
	Events    EventMask
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
	// Purpose: Build an Aho-Corasick scanner for keyword patterns.
	// Key aspects: Constructs trie, failure links, and output lists.
	// Upstream: getKeywordScanner initialization.
	// Downstream: acCursor.exact.
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

// acCursor belongs to one parse and walks an immutable taxonomy scanner. Token
// ends increase, so exact matching needs only the current state, not a growing
// collection of every substring match. The scanner's transitions and output
// order remain unchanged, including its existing failure-link behavior.
type acCursor struct {
	scanner  *acScanner
	text     string
	state    int
	position int
}

func (c *acCursor) exact(start, end int) (acPattern, bool) {
	if c.scanner == nil || start < 0 || end < start || end < c.position || end > len(c.text) {
		return acPattern{}, false
	}
	for c.position < end {
		ch := c.text[c.position]
		next, ok := c.scanner.nodes[c.state].next[ch]
		for !ok && c.state > 0 {
			c.state = c.scanner.nodes[c.state].fail
			next, ok = c.scanner.nodes[c.state].next[ch]
		}
		if ok {
			c.state = next
		}
		c.position++
	}
	for _, pid := range c.scanner.nodes[c.state].outputs {
		pattern := c.scanner.patterns[pid]
		if len(pattern.word) == end-start {
			return pattern, true
		}
	}
	return acPattern{}, false
}

func classifyTokenWithFallback(cursor *acCursor, tok commentToken) (acPattern, bool) {
	// Keep original byte offsets against the uppercase full comment. Unicode
	// case mapping can change byte widths; correcting those offsets would change
	// existing parser results. A dictionary lookup also loses scanner output
	// suppression when an original span lands inside a longer matched word.
	if pat, ok := cursor.exact(tok.trimStart, tok.trimEnd); ok {
		return pat, true
	}
	// The old fallback fetched the current taxonomy for each token. Preserve
	// that visibility while the full-comment cursor retains its first snapshot.
	fallback := acCursor{scanner: getKeywordScanner(), text: tok.upper}
	return fallback.exact(0, len(tok.upper))
}

func getKeywordScanner() *acScanner {
	// Purpose: Return the taxonomy-owned keyword scanner.
	// Key aspects: Scanner is built once when the immutable taxonomy is loaded.
	// Upstream: ParseSpotComment and classifyTokenWithFallback.
	// Downstream: CurrentTaxonomy snapshot.
	return CurrentTaxonomy().keywordScanner
}

func buildKeywordScanner(t *Taxonomy) *acScanner {
	patterns := make([]acPattern, 0, 3+len(t.modeCommentTokens))
	patterns = append(patterns,
		acPattern{word: "DB", kind: acTokenDB},
		acPattern{word: "WPM", kind: acTokenWPM},
		acPattern{word: "BPS", kind: acTokenBPS},
	)
	modeTokens := make([]string, 0, len(t.modeCommentTokens))
	for token := range t.modeCommentTokens {
		modeTokens = append(modeTokens, token)
	}
	sort.Strings(modeTokens)
	for _, token := range modeTokens {
		patterns = append(patterns, acPattern{word: token, kind: acTokenMode, mode: t.modeCommentTokens[token]})
	}
	return newACScanner(patterns)
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
	// Purpose: Tokenize a comment into word-like segments with trim metadata.
	// Key aspects: Tracks original and trimmed offsets for keyword alignment.
	// Upstream: ParseSpotComment.
	// Downstream: strings.ToUpper and rune trimming.
	// Count using the same ASCII separators as the tokenizer. Exact backing
	// avoids overlapping growth generations on maximum-size peer comments.
	tokens := make([]commentToken, 0, commentTokenCount(comment))
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

func commentTokenCount(comment string) int {
	count := 0
	inside := false
	for i := 0; i < len(comment); i++ {
		separator := comment[i] == ' ' || comment[i] == '\t'
		if !separator && !inside {
			count++
		}
		inside = !separator
	}
	return count
}

var snrPattern = regexp.MustCompile(`(?i)([-+]?\d{1,3})\s*dB`)

func parseSignedInt(tok string) (int, bool) {
	// Purpose: Parse a signed integer token with sanity bounds.
	// Key aspects: Rejects decimals and values outside +/-200.
	// Upstream: ParseSpotComment numeric handling.
	// Downstream: strconv.Atoi.
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
	// Purpose: Parse a compact "±NNdB" report token.
	// Key aspects: Requires trailing "db" and sane bounds.
	// Upstream: ParseSpotComment.
	// Downstream: strconv.Atoi.
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
	// Purpose: Split a leading time token (HHMMZ) from a token.
	// Key aspects: Returns the time token and remaining string.
	// Upstream: ParseSpotComment.
	// Downstream: isTimeToken.
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
	// Purpose: Check whether a token matches HHMMZ format.
	// Key aspects: Requires 4 digits followed by Z.
	// Upstream: ParseSpotComment.
	// Downstream: isAllDigits.
	if len(tok) != 5 {
		return false
	}
	if tok[4] != 'Z' && tok[4] != 'z' {
		return false
	}
	return isAllDigits(tok[:4])
}

func isAllDigits(s string) bool {
	// Purpose: Determine whether a string is all ASCII digits.
	// Key aspects: Rejects empty strings.
	// Upstream: isTimeToken.
	// Downstream: rune iteration.
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
	// Purpose: Rebuild the comment from unconsumed tokens.
	// Key aspects: Skips consumed tokens and trims whitespace.
	// Upstream: ParseSpotComment.
	// Downstream: strings.Join.
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

// ParseSpotComment parses mode/report/time tokens and return a cleaned comment.
// Key aspects: Uses keyword scanner, numeric parsing, and greeting guards (73/88).
// Upstream: All spot parsers (RBN, peer, PSKReporter).
// Downstream: tokenization helpers and NormalizeVoiceMode.
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
	cursor := acCursor{scanner: getKeywordScanner(), text: strings.ToUpper(comment)}

	var (
		mode            string
		report          int
		hasReport       bool
		timeToken       string
		speedValue      string
		speedUnit       string
		events          EventMask
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
		events |= eventFromCommentToken(tok.upper)
		if isTimeToken(clean) {
			if timeToken == "" {
				timeToken = clean
			}
			consumed[idx] = true
			pendingNumIdx = -1
			continue
		}

		if pat, ok := classifyTokenWithFallback(&cursor, tok); ok {
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
		Events:    events,
	}
}

// ParseSpotEvents extracts EVENT families from a preserved comment without
// applying mode/report cleanup. It is used for legacy archive records that
// predate persisted EVENT metadata.
func ParseSpotEvents(comment string) EventMask {
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return 0
	}
	var events EventMask
	for _, tok := range tokenizeComment(comment) {
		events |= eventFromCommentToken(tok.upper)
	}
	return events
}

// Purpose: Decide whether a mode accepts bare numeric reports.
// Key aspects: Limits to CW/RTTY and selected digital modes.
// Upstream: ParseSpotComment.
// Downstream: strings.ToUpper.
// modeWantsBareReport determines which modes treat bare signed integers as SNR/report.
func modeWantsBareReport(mode string) bool {
	return ModeWantsBareReport(mode)
}

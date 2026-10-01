package spot

import (
	"strconv"
	"strings"
)

// Frozen pre-v8 oracle from spot/comment_parser.go, SHA256
// 26B0F348FE844938258A9AA94B2F7B077246641B5D882F091DEEB7D936A4B5E6.
// Only names have changed. Keep the original tokenizer, match collection/index,
// and parser control flow independent of the replacement. Unchanged report,
// event, time and taxonomy helpers remain shared; the scanner constructor is
// deliberately unchanged by v8. This reference is test-only and is not a
// second production parser or a claim of independent scientific evidence.

type legacyCommentMatch struct {
	start   int
	end     int
	pattern acPattern
}

func legacyCommentFindAll(sc *acScanner, text string) []legacyCommentMatch {
	// Purpose: Find all keyword matches within the text.
	// Key aspects: Uses Aho-Corasick state machine to emit overlapping matches.
	// Upstream: legacyCommentParse and legacyCommentClassifyFallback.
	// Downstream: acScanner nodes and output list.
	if sc == nil {
		return nil
	}
	state := 0
	matches := make([]legacyCommentMatch, 0, 8)
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
				matches = append(matches, legacyCommentMatch{start: start, end: end, pattern: p})
			}
		}
	}
	return matches
}

func legacyCommentMatchIndex(matches []legacyCommentMatch) map[int][]legacyCommentMatch {
	// Purpose: Index matches by start position for O(1) lookup.
	// Key aspects: Groups matches by their start offset.
	// Upstream: legacyCommentParse.
	// Downstream: map allocation and append.
	if len(matches) == 0 {
		return nil
	}
	index := make(map[int][]legacyCommentMatch, len(matches))
	for _, m := range matches {
		index[m.start] = append(index[m.start], m)
	}
	return index
}

func legacyCommentClassify(matchIndex map[int][]legacyCommentMatch, trimStart, trimEnd int) (acPattern, bool) {
	// Purpose: Resolve an exact token match from the match index.
	// Key aspects: Requires a match with identical start/end positions.
	// Upstream: legacyCommentClassifyFallback.
	// Downstream: matchIndex lookup.
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

func legacyCommentClassifyFallback(matchIndex map[int][]legacyCommentMatch, tok commentToken) (acPattern, bool) {
	// Purpose: Resolve a token to a keyword pattern with fallback scanning.
	// Key aspects: Checks index first, then scans token text directly.
	// Upstream: legacyCommentParse loop.
	// Downstream: legacyCommentClassify and getKeywordScanner.FindAll.
	if pat, ok := legacyCommentClassify(matchIndex, tok.trimStart, tok.trimEnd); ok {
		return pat, true
	}
	for _, m := range legacyCommentFindAll(getKeywordScanner(), tok.upper) {
		if m.start == 0 && m.end == len(tok.upper) {
			return m.pattern, true
		}
	}
	return acPattern{}, false
}

func legacyCommentTokenize(comment string) []commentToken {
	// Purpose: Tokenize a comment into word-like segments with trim metadata.
	// Key aspects: Tracks original and trimmed offsets for keyword alignment.
	// Upstream: legacyCommentParse.
	// Downstream: strings.ToUpper and rune trimming.
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

func legacyCommentParse(comment string, freq float64) CommentParseResult {
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return CommentParseResult{}
	}

	tokens := legacyCommentTokenize(comment)
	consumed := make([]bool, len(tokens))
	matchIndex := legacyCommentMatchIndex(legacyCommentFindAll(getKeywordScanner(), strings.ToUpper(comment)))

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

		if pat, ok := legacyCommentClassifyFallback(matchIndex, tok); ok {
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

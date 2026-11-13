// History selection and presentation are separate from connection-owned cursors.
// Exact identity uses today's DX normalizer on both query and archive materialization;
// neither CTY country lookup nor an empty result may widen an exact-call search.
package commands

import (
	"errors"
	"strings"
	"time"

	"dxcluster/archive"
	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

type historySelectorKind uint8

const (
	historyNone historySelectorKind = iota
	historyExactCall
	historyEntity
)

type historySelector struct {
	kind historySelectorKind
	call string
	adif int
}

// HistoryQuery is an immutable, classified search. Connections retain this small
// value, never CTY snapshots, predicates, result slices, or archive iterators.
type HistoryQuery struct {
	selector historySelector
	count    int
	comment  string   // At most MaxCommentPhraseBytes; retained with continuation.
	bands    []string // Unique canonical names, bounded by SupportedBandNames.
	modes    []string // Unique canonical names, bounded by SupportedFilterModes.
}

// HistoryCommand distinguishes a fresh search from a connection-local NEXT token.
type HistoryCommand struct {
	Query HistoryQuery
	Token string
}

// ParseHistoryCommand recognizes only existing history spellings, preserving
// dialect restrictions. Invalid requests do not create or replace a search.
func (p *Processor) ParseHistoryCommand(line, dialect string) (HistoryCommand, bool, string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return HistoryCommand{}, false, ""
	}
	var args []string
	label := "SHOW DX"
	argStart := 1
	switch strings.ToUpper(parts[0]) {
	case "SHOW/DX", "SH/DX":
		if normalizeDialectString(dialect) != "cc" {
			return HistoryCommand{}, true, "Use SHOW DX or SH DX for DX history.\n"
		}
		args = parts[1:]
	case "SHOW", "SH":
		if len(parts) < 2 || (!strings.EqualFold(parts[1], "DX") && !strings.EqualFold(parts[1], "MYDX")) {
			return HistoryCommand{}, false, ""
		}
		label = "SHOW " + strings.ToUpper(parts[1])
		argStart = 2
		args = parts[2:]
	default:
		return HistoryCommand{}, false, ""
	}
	if len(args) > 0 && strings.EqualFold(args[0], "NEXT") {
		if len(args) != 2 || !ValidHistoryToken(strings.ToUpper(args[1])) {
			return HistoryCommand{}, true, "Invalid history continuation. Start a fresh SHOW DX search.\n"
		}
		return HistoryCommand{Token: strings.ToUpper(args[1])}, true, ""
	}
	// COMMENT consumes the original remainder, rather than Fields/Join, so
	// interior spaces and literal punctuation survive unchanged. NEXT is handled
	// first: a continuation cannot replace the saved search phrase.
	comment := ""
	var clause historyClauseState
	for i, arg := range args {
		if clause.advance([]byte(arg)) {
			comment = historyCommentRemainder(line, parts[:argStart+i+1])
			if !filter.ValidCommentPhrase(comment) {
				return HistoryCommand{}, true, "Invalid COMMENT phrase. Use 1-64 printable ASCII bytes.\n"
			}
			args = args[:i]
			break
		}
	}
	query, errText := p.prepareHistory(args, label)
	if errText != "" {
		return HistoryCommand{}, true, errText
	}
	// Detach the retained needle from the full command's backing string. Even
	// callers with a larger command budget retain at most 64 phrase bytes.
	query.comment = strings.Clone(comment)
	return HistoryCommand{Query: query}, true, errText
}

// Prefix fields come from this same line; each search advances past the exact
// original token. Only command-edge whitespace is trimmed from the phrase.
func historyCommentRemainder(line string, prefix []string) string {
	for _, token := range prefix {
		i := strings.Index(line, token)
		line = line[i+len(token):]
	}
	return strings.Trim(line, " ")
}

// ValidHistoryToken also bounds token parsing before any connection state lookup.
func ValidHistoryToken(token string) bool {
	if len(token) != 34 || !strings.HasPrefix(token, "H1") {
		return false
	}
	for _, c := range token[2:] {
		if (c < '0' || c > '9') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

func (p *Processor) prepareHistory(args []string, label string) (HistoryQuery, string) {
	base, bands, modes, errText := parseHistorySelections(args)
	if errText != "" {
		return HistoryQuery{}, errText
	}
	req, errText := parseShowHistoryRequest(base, label)
	if errText != "" {
		return HistoryQuery{}, errText
	}
	// Normalization can retain its input in the callsign cache. Detach the
	// selector first so a short uppercase call cannot pin the entire command.
	selector, errText := p.resolveHistorySelector(strings.Clone(strings.ToUpper(req.selector)))
	return HistoryQuery{selector: selector, count: req.count, bands: bands, modes: modes}, errText
}

func (p *Processor) resolveHistorySelector(raw string) (historySelector, string) {
	if raw == "" {
		return historySelector{}, ""
	}
	if p.ctyLookup == nil {
		return historySelector{}, "CTY database is not available.\n"
	}
	db := p.ctyLookup()
	if db == nil {
		return historySelector{}, "CTY database is not loaded.\n"
	}
	adif, err := cty.NewDXCCIndex(db).ResolveCanonical(raw)
	if err == nil {
		return historySelector{kind: historyEntity, adif: adif}, ""
	}
	if !errors.Is(err, cty.ErrUnknownCanonicalPrefix) {
		return historySelector{}, "Conflicting DXCC canonical prefix.\n"
	}
	call := spot.NormalizeSpotDXCallsign(raw)
	if spot.IsValidNormalizedCallsign(call) {
		return historySelector{kind: historyExactCall, call: call}, ""
	}
	// Prefix fallback retains supported CTY lookup, but punctuation and empty
	// segments cannot be treated as a prefix merely because a leading part resolves.
	if !validHistoryPrefix(call) {
		return historySelector{}, "Unknown DXCC/prefix.\n"
	}
	info, ok := db.LookupCallsignPortable(call)
	if !ok || info == nil {
		return historySelector{}, "Unknown DXCC/prefix.\n"
	}
	return historySelector{kind: historyEntity, adif: info.ADIF}, ""
}

func validHistoryPrefix(prefix string) bool {
	if prefix == "" || len(prefix) > 15 {
		return false
	}
	for _, segment := range strings.Split(prefix, "/") {
		if segment == "" {
			return false
		}
		for _, c := range segment {
			if (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
				return false
			}
		}
	}
	return true
}

// ReadHistoryPage composes selection before counting. Decoded archive spots own
// DXCallNorm normalized by NormalizeSpotDXCallsign during materialization.
func (p *Processor) ReadHistoryPage(query HistoryQuery, before []byte, match func(*spot.Spot) bool, now time.Time, done <-chan struct{}) (archive.HistoryPage, error) {
	if p.archive == nil {
		return archive.HistoryPage{}, errors.New("archive unavailable")
	}
	return p.archive.ReadHistoryPage(archive.HistoryRequest{
		Limit: query.count, Before: before, Now: now, Done: done,
		Match: func(s *spot.Spot) bool {
			if s == nil {
				return false
			}
			switch query.selector.kind {
			case historyExactCall:
				if s.DXCallNorm != query.selector.call {
					return false
				}
			case historyEntity:
				if s.DXMetadata.ADIF != query.selector.adif {
					return false
				}
			}
			if !query.matchesBandMode(s) {
				return false
			}
			// Reject unrelated identities before scanning their comments. Explicit
			// selection remains mandatory even when saved filters exempt self-spots.
			if query.comment != "" && !filter.MatchCommentPhrase(s.Comment, query.comment) {
				return false
			}
			return match != nil && match(s)
		},
	})
}

// RenderHistoryPage emits unchanged spot lines, then honest completion/warning
// status. Cumulative flags prevent an older empty page from claiming no matches.
func RenderHistoryPage(page archive.HistoryPage, token string, older, unreadable, hadMatches bool) string {
	return RenderHistoryPageWithFormatter(page, token, older, unreadable, hadMatches, (*spot.Spot).FormatDXCluster)
}

// RenderHistoryPageWithFormatter formats each owned spot with the caller's
// captured display settings; selection and continuation remain unchanged.
func RenderHistoryPageWithFormatter(page archive.HistoryPage, token string, older, unreadable, hadMatches bool, format func(*spot.Spot) string) string {
	var result strings.Builder
	if older {
		result.WriteString(historyOlderHeading)
		result.WriteByte('\n')
	}
	for i := len(page.Spots) - 1; i >= 0; i-- {
		result.WriteString(format(page.Spots[i]))
		result.WriteString("\r\n")
	}
	for _, line := range historyPageStatus(page, token, older, unreadable, hadMatches) {
		if line.text != "" {
			result.WriteString(line.text)
			result.WriteString(line.token)
			result.WriteByte('\n')
		}
	}
	return result.String()
}

const historyOlderHeading = "Older retained history page:"

// ErrHistoryResponseTooLarge refuses a complete page that cannot fit the
// caller's total byte budget, including any existing destination prefix.
var ErrHistoryResponseTooLarge = errors.New("history response exceeds byte budget")

// AppendHistoryPage appends a complete page with CRLF for records and fixed
// text. maxBytes caps the total returned length, including dst's existing prefix.
// appendSpot must preserve its prefix and append one record WITHOUT CRLF within
// the supplied additional-byte budget. Fixed text and every record's CRLF are
// reserved first. Caller backing is reused; maxBytes is not a preallocation size.
//
// On any error the original dst is returned, never a successful partial page.
// Unlike a single-record appender's stronger refusal guarantee, unused private
// backing may already hold unpublished records; the caller must discard it.
func AppendHistoryPage(dst []byte, page archive.HistoryPage, token string, older, unreadable, hadMatches bool, maxBytes int, appendSpot func([]byte, *spot.Spot, int) ([]byte, error)) ([]byte, error) {
	if appendSpot == nil {
		return dst, errors.New("history record appender is nil")
	}
	if maxBytes < len(dst) {
		return dst, ErrHistoryResponseTooLarge
	}
	status := historyPageStatus(page, token, older, unreadable, hadMatches)
	remaining := maxBytes - len(dst)
	// Subtract each fragment separately: even a very large token or caller
	// budget cannot overflow a sum of mandatory text and line endings.
	if older {
		if len(historyOlderHeading) > remaining {
			return dst, ErrHistoryResponseTooLarge
		}
		remaining -= len(historyOlderHeading)
		if remaining < 2 {
			return dst, ErrHistoryResponseTooLarge
		}
		remaining -= 2
	}
	for _, line := range status {
		if line.text == "" {
			continue
		}
		for _, text := range [...]string{line.text, line.token, "\r\n"} {
			if len(text) > remaining {
				return dst, ErrHistoryResponseTooLarge
			}
			remaining -= len(text)
		}
	}
	for range page.Spots {
		if remaining < 2 {
			return dst, ErrHistoryResponseTooLarge
		}
		remaining -= 2
	}
	result := dst
	if older {
		result = append(result, historyOlderHeading...)
		result = append(result, '\r', '\n')
	}
	for i := len(page.Spots) - 1; i >= 0; i-- {
		before := len(result)
		next, err := appendSpot(result, page.Spots[i], remaining)
		if err != nil {
			return dst, err
		}
		if len(next) < before {
			return dst, errors.New("history record appender shortened output")
		}
		added := len(next) - before
		if added > remaining {
			return dst, ErrHistoryResponseTooLarge
		}
		remaining -= added
		result = next
		result = append(result, '\r', '\n')
	}
	for _, line := range status {
		if line.text != "" {
			result = append(result, line.text...)
			result = append(result, line.token...)
			result = append(result, '\r', '\n')
		}
	}
	return result, nil
}

type historyStatusLine struct {
	text  string
	token string
}

// Completion and cumulative-warning choices are shared, not record formatting
// or line endings. A separate token fragment avoids an unbounded concatenation.
func historyPageStatus(page archive.HistoryPage, token string, older, unreadable, hadMatches bool) [3]historyStatusLine {
	var status [3]historyStatusLine
	if unreadable || page.Unreadable > 0 {
		status[0].text = "Warning: unreadable archive records were skipped; matching history may be incomplete."
	}
	if page.End == archive.HistoryExhausted {
		if len(page.Spots) == 0 && !hadMatches && !older && !unreadable && page.Unreadable == 0 {
			status[1].text = "No matching retained spots."
		} else {
			status[1].text = "End of retained history search."
		}
	} else {
		if page.End == archive.HistoryBudgetReached {
			status[1].text = "Search work limit reached; this page is incomplete."
		}
		if token != "" {
			status[2] = historyStatusLine{text: "Continue older history: SHOW DX NEXT ", token: token}
		} else {
			status[2].text = "Older history remains; continuation requires a connected client."
		}
	}
	return status
}

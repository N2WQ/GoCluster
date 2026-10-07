// History selection and presentation are separate from connection-owned cursors.
// Exact identity uses today's DX normalizer on both query and archive materialization;
// neither CTY country lookup nor an empty result may widen an exact-call search.
package commands

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"dxcluster/archive"
	"dxcluster/cty"
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
}

// HistoryCommand distinguishes a fresh search from a connection-local NEXT token.
type HistoryCommand struct {
	Query HistoryQuery
	Token string
}

// ParseHistoryCommand recognizes only existing history spellings, preserving
// dialect restrictions. Invalid requests do not allocate or replace a search.
func (p *Processor) ParseHistoryCommand(line, dialect string) (HistoryCommand, bool, string) {
	parts := strings.Fields(strings.ToUpper(line))
	if len(parts) == 0 {
		return HistoryCommand{}, false, ""
	}
	var args []string
	label := "SHOW DX"
	switch parts[0] {
	case "SHOW/DX", "SH/DX":
		if normalizeDialectString(dialect) != "cc" {
			return HistoryCommand{}, true, "Use SHOW DX or SH DX for DX history.\n"
		}
		args = parts[1:]
	case "SHOW", "SH":
		if len(parts) < 2 || (parts[1] != "DX" && parts[1] != "MYDX") {
			return HistoryCommand{}, false, ""
		}
		label = "SHOW " + parts[1]
		args = parts[2:]
	default:
		return HistoryCommand{}, false, ""
	}
	if len(args) > 0 && args[0] == "NEXT" {
		if len(args) != 2 || !ValidHistoryToken(args[1]) {
			return HistoryCommand{}, true, "Invalid history continuation. Start a fresh SHOW DX search.\n"
		}
		return HistoryCommand{Token: args[1]}, true, ""
	}
	query, errText := p.prepareHistory(args, label)
	return HistoryCommand{Query: query}, true, errText
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
	req, errText := parseShowHistoryRequest(args, label)
	if errText != "" {
		return HistoryQuery{}, errText
	}
	selector, errText := p.resolveHistorySelector(req.selector)
	return HistoryQuery{selector: selector, count: req.count}, errText
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
			return match != nil && match(s)
		},
	})
}

// RenderHistoryPage emits unchanged spot lines, then honest completion/warning
// status. Cumulative flags prevent an older empty page from claiming no matches.
func RenderHistoryPage(page archive.HistoryPage, token string, older, unreadable, hadMatches bool) string {
	var result strings.Builder
	if older {
		result.WriteString("Older retained history page:\n")
	}
	for i := len(page.Spots) - 1; i >= 0; i-- {
		result.WriteString(page.Spots[i].FormatDXCluster())
		result.WriteString("\r\n")
	}
	if unreadable || page.Unreadable > 0 {
		result.WriteString("Warning: unreadable archive records were skipped; matching history may be incomplete.\n")
	}
	if page.End == archive.HistoryExhausted {
		if len(page.Spots) == 0 && !hadMatches && !older && !unreadable && page.Unreadable == 0 {
			result.WriteString("No matching retained spots.\n")
		} else {
			result.WriteString("End of retained history search.\n")
		}
	} else {
		if page.End == archive.HistoryBudgetReached {
			result.WriteString("Search work limit reached; this page is incomplete.\n")
		}
		if token != "" {
			fmt.Fprintf(&result, "Continue older history: SHOW DX NEXT %s\n", token)
		} else {
			result.WriteString("Older history remains; continuation requires a connected client.\n")
		}
	}
	return result.String()
}

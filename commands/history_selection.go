// History list selections narrow the existing archive predicate before counting.
// Query-owned slices are immutable after parsing, retain only detached canonical
// names, and expire with the existing connection cursor; no lookup cache is added.
package commands

import (
	"fmt"
	"slices"
	"strings"

	"dxcluster/filter"
	"dxcluster/spot"
)

func historySelectionToken(token string) bool {
	return strings.EqualFold(token, "BAND") || strings.EqualFold(token, "MODE")
}

// The optional selector/count retain their old grammar before the first clause.
// COMMENT has already consumed the literal remainder; its words are never lists.
func parseHistorySelections(args []string) (base, bands, modes []string, errText string) {
	pos := 0
	for pos < len(args) && !historySelectionToken(args[pos]) {
		pos++
	}
	base = args[:pos]
	for pos < len(args) {
		domain := strings.ToUpper(args[pos])
		start := pos + 1
		pos = start
		for pos < len(args) && !historySelectionToken(args[pos]) {
			pos++
		}
		if (domain == "BAND" && bands != nil) || (domain == "MODE" && modes != nil) {
			return nil, nil, nil, fmt.Sprintf("Invalid %s selection: category may appear only once.\n", domain)
		}
		values, text := parseHistorySelectionValues(domain, args[start:pos])
		if text != "" {
			return nil, nil, nil, text
		}
		if domain == "BAND" {
			bands = values
		} else {
			modes = values
		}
	}
	return base, bands, modes, ""
}

func parseHistorySelectionValues(domain string, args []string) ([]string, string) {
	var values []string
	for _, arg := range args {
		for _, value := range strings.Split(arg, ",") {
			if value == "" {
				continue // Same comma/space separators as PASS BAND/MODE.
			}
			var normalized string
			var valid bool
			if domain == "BAND" {
				normalized = spot.NormalizeBand(value)
				valid = spot.IsValidBand(normalized)
			} else {
				normalized = spot.CanonicalModeForFilter(value)
				valid = filter.IsSupportedMode(normalized)
			}
			if !valid || strings.EqualFold(value, "ALL") || strings.EqualFold(value, "NONE") {
				return nil, fmt.Sprintf("Invalid %s selection: unsupported value %q.\n", domain, value)
			}
			if !slices.Contains(values, normalized) {
				// A raw lowercase band token can borrow the whole input string.
				// Detach it, and never grow beyond the finite supported vocabulary.
				values = append(values, strings.Clone(normalized))
			}
		}
	}
	if len(values) == 0 {
		return nil, fmt.Sprintf("Invalid %s selection: supply at least one supported value.\n", domain)
	}
	return values, ""
}

// Explicit clauses also constrain self-spots. Saved filters keep their existing
// self exception and are evaluated by the caller after these mandatory gates.
func (query HistoryQuery) matchesBandMode(s *spot.Spot) bool {
	if len(query.bands) != 0 {
		band := s.BandNorm
		if band == "" {
			band = s.Band
		}
		if !slices.Contains(query.bands, spot.NormalizeBand(band)) {
			return false
		}
	}
	if len(query.modes) != 0 {
		mode := s.ModeNorm
		if mode == "" {
			mode = spot.CanonicalModeForFilter(s.Mode)
		}
		if !slices.Contains(query.modes, mode) {
			return false
		}
	}
	return true
}

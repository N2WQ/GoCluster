// File role: Resolves canonical CTY entity labels without callsign processing.
package cty

import (
	"errors"
	"slices"

	"dxcluster/strutil"
)

// ErrUnknownCanonicalPrefix means no positive ADIF association exists.
var ErrUnknownCanonicalPrefix = errors.New("unknown DXCC canonical prefix")

// ErrConflictingCanonicalPrefix means a label identifies multiple ADIF entities.
var ErrConflictingCanonicalPrefix = errors.New("conflicting DXCC canonical prefix")

// DXCCIndex is a read-only, request-owned interpretation of one CTY snapshot.
// Its maps and label bytes are bounded by that database's records. It is not
// retained across commands, so refresh needs no cache invalidation or locks.
type DXCCIndex struct {
	canonical map[string]int
	prefixes  map[int][]string
}

// NewDXCCIndex groups canonical Prefix fields, not callsign lookup keys.
// Conflicting labels are marked rather than choosing a map-iteration winner.
func NewDXCCIndex(db *CTYDatabase) *DXCCIndex {
	index := &DXCCIndex{canonical: make(map[string]int), prefixes: make(map[int][]string)}
	if db == nil {
		return index
	}
	groups := make(map[int]map[string]struct{})
	for _, info := range db.Data {
		prefix := strutil.NormalizeUpper(info.Prefix)
		if prefix == "" || info.ADIF <= 0 {
			continue
		}
		if previous, exists := index.canonical[prefix]; exists && previous != info.ADIF {
			index.canonical[prefix] = 0
		} else if !exists {
			index.canonical[prefix] = info.ADIF
		}
		if groups[info.ADIF] == nil {
			groups[info.ADIF] = make(map[string]struct{})
		}
		groups[info.ADIF][prefix] = struct{}{}
	}
	for adif, group := range groups {
		prefixes := make([]string, 0, len(group))
		for prefix := range group {
			// Wait until every record has been examined: a later association
			// can make an earlier label ambiguous. Keep valid alternatives so
			// human readbacks retain this entity's unambiguous identity.
			if index.canonical[prefix] != adif {
				continue
			}
			prefixes = append(prefixes, prefix)
		}
		slices.Sort(prefixes)
		index.prefixes[adif] = prefixes
	}
	return index
}

// ResolveCanonical resolves an exact trimmed, uppercased canonical label.
// Slash segments and portable suffixes retain their entity-label meaning.
func (index *DXCCIndex) ResolveCanonical(input string) (int, error) {
	if index == nil {
		return 0, ErrUnknownCanonicalPrefix
	}
	adif, exists := index.canonical[strutil.NormalizeUpper(input)]
	if !exists {
		return 0, ErrUnknownCanonicalPrefix
	}
	if adif == 0 {
		return 0, ErrConflictingCanonicalPrefix
	}
	return adif, nil
}

// Prefixes returns borrowed sorted unambiguous labels. Callers must not modify
// the slice. If no labels remain, human readbacks use their stored-number fallback.
func (index *DXCCIndex) Prefixes(adif int) []string {
	if index == nil {
		return nil
	}
	return index.prefixes[adif]
}

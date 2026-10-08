// File role: Bounded literal comment rules shared by delivery and archive queries.
package filter

import "fmt"

const (
	// MaxCommentPhrases bounds each owned allow or reject list, including duplicates.
	MaxCommentPhrases = 32
	// MaxCommentPhraseBytes bounds a single printable ASCII phrase.
	MaxCommentPhraseBytes       = 64
	commentConfigurationVersion = 4
)

// ValidCommentPhrase validates exact values without trimming or changing case.
func ValidCommentPhrase(phrase string) bool {
	if len(phrase) == 0 || len(phrase) > MaxCommentPhraseBytes {
		return false
	}
	nonSpace := false
	for i := range len(phrase) {
		if phrase[i] < ' ' || phrase[i] > '~' {
			return false
		}
		nonSpace = nonSpace || phrase[i] != ' '
	}
	return nonSpace
}

func foldCommentByte(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

// MatchCommentPhrase performs literal ASCII case-insensitive substring matching.
// The 64-byte admission cap fits Shift-And's state in one uint64: bit i records
// a matching prefix of length i+1 ending at the current byte. Repeated prefixes
// cannot restart scans. Work is O(len(comment)+len(phrase)+128), with a fixed
// 1 KiB stack table, no heap/cache state, and no rewriting of the stored text.
func MatchCommentPhrase(comment, phrase string) bool {
	if !ValidCommentPhrase(phrase) || len(phrase) > len(comment) {
		return false
	}
	var masks [128]uint64
	for i := range len(phrase) {
		masks[foldCommentByte(phrase[i])] |= uint64(1) << i
	}
	terminal := uint64(1) << (len(phrase) - 1)
	var state uint64
	for i := range len(comment) {
		ch := foldCommentByte(comment[i])
		if ch >= 128 {
			// An ASCII phrase cannot span a non-ASCII byte. Do not skip it or
			// apply Unicode folding, which would change literal semantics.
			state = 0
			continue
		}
		state = ((state << 1) | 1) & masks[ch]
		if state&terminal != 0 {
			return true
		}
	}
	return false
}

// ValidateCommentRules rejects invalid owned lists before cloning or publication.
// Every exact entry counts, including duplicates and cross-list overlaps.
func (c Configuration) ValidateCommentRules() error {
	for _, list := range []struct {
		name    string
		phrases []string
	}{{"comments", c.Filters.Comments}, {"block_comments", c.Filters.BlockComments}} {
		if len(list.phrases) > MaxCommentPhrases {
			return fmt.Errorf("filters.%s exceeds %d phrases", list.name, MaxCommentPhrases)
		}
		for _, phrase := range list.phrases {
			if !ValidCommentPhrase(phrase) {
				return fmt.Errorf("filters.%s contains an invalid comment phrase", list.name)
			}
		}
	}
	return nil
}

// passesComment checks rejects first even when exact YAML retained overlapping
// lists. No derived matcher state outlives the filter-owned phrase lists.
func (f *Filter) passesComment(comment string) bool {
	for _, phrase := range f.BlockComments {
		if MatchCommentPhrase(comment, phrase) {
			return false
		}
	}
	if len(f.Comments) == 0 {
		return true
	}
	for _, phrase := range f.Comments {
		if MatchCommentPhrase(comment, phrase) {
			return true
		}
	}
	return false
}

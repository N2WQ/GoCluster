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

// MatchCommentPhrase performs literal ASCII case-insensitive substring matching
// without allocating or rewriting the stored comment. Invalid needles fail.
func MatchCommentPhrase(comment, phrase string) bool {
	if !ValidCommentPhrase(phrase) || len(phrase) > len(comment) {
		return false
	}
	for start := 0; start <= len(comment)-len(phrase); start++ {
		matched := true
		for i := range len(phrase) {
			if foldCommentByte(comment[start+i]) != foldCommentByte(phrase[i]) {
				matched = false
				break
			}
		}
		if matched {
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

// File role: Bounded literal comment rules shared by delivery and archive queries.
package filter

// MaxCommentPhraseBytes bounds a single printable ASCII phrase.
const MaxCommentPhraseBytes = 64

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

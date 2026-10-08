// File role: Broaden only an explicitly introduced COMMENT argument to printable
// ASCII. The ordinary command/login whitelist, byte ceiling and editing remain
// owned by the input reader; backspacing a prefix immediately restores that list.
package telnet

func nextCommentInputWord(line []byte, start int) (word []byte, end int) {
	for start < len(line) && line[start] == ' ' {
		start++
	}
	end = start
	for end < len(line) && line[end] != ' ' {
		end++
	}
	return line[start:end], end
}

// The COMMENT keyword must be followed by a space before arbitrary punctuation
// is accepted. Inspect borrowed words, without retaining slices or allocating.
func commentArgumentsStarted(line []byte) bool {
	word, pos := nextCommentInputWord(line, 0)
	switch {
	case asciiWordEqual(word, "PASS"), asciiWordEqual(word, "REJECT"):
		word, pos = nextCommentInputWord(line, pos)
		return asciiWordEqual(word, "COMMENT") && pos < len(line)
	case asciiWordEqual(word, "REMOVE"):
		word, pos = nextCommentInputWord(line, pos)
		if !asciiWordEqual(word, "PASS") && !asciiWordEqual(word, "REJECT") {
			return false
		}
		word, pos = nextCommentInputWord(line, pos)
		return asciiWordEqual(word, "COMMENT") && pos < len(line)
	case asciiWordEqual(word, "SHOW"), asciiWordEqual(word, "SH"):
		word, pos = nextCommentInputWord(line, pos)
		if !asciiWordEqual(word, "DX") && !asciiWordEqual(word, "MYDX") {
			return false
		}
	case asciiWordEqual(word, "SHOW/DX"), asciiWordEqual(word, "SH/DX"):
	default:
		return false
	}
	// History accepts at most a selector and count before the phrase marker.
	for range 3 {
		word, pos = nextCommentInputWord(line, pos)
		if asciiWordEqual(word, "COMMENT") {
			return pos < len(line)
		}
		if len(word) == 0 || asciiWordEqual(word, "NEXT") {
			return false
		}
	}
	return false
}

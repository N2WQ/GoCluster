// File role: Broaden an explicitly introduced COMMENT argument to printable
// ASCII and admit '+' only within a manual DX comment. The ordinary command/login
// whitelist, byte ceiling and editing remain owned by the input reader;
// backspacing a prefix immediately restores that list.
package telnet

import "dxcluster/commands"

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

// dxCommentArgumentsStarted recognizes the comment boundary in both DX argument
// orders. Argument validity belongs to command dispatch. Borrowing the edited
// buffer avoids retained permission after an erase, and allocates no per-byte
// state while reading the line.
func dxCommentArgumentsStarted(line []byte) bool {
	word, pos := nextCommentInputWord(line, 0)
	if !asciiWordEqual(word, "DX") {
		return false
	}
	for range 2 {
		word, pos = nextCommentInputWord(line, pos)
		if len(word) == 0 || pos == len(line) {
			return false
		}
	}
	return true
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
	// The parser owns the same keyword/comma boundary rule, so a configured
	// COMMENT mode alias cannot prematurely enable literal punctuation here.
	return commands.HistoryCommentClauseStarted(line[pos:])
}

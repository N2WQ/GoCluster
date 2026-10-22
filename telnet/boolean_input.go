package telnet

// booleanRuleExpressionStart borrows only header words. A trailing space after
// the slot is required before expression punctuation is admitted. The command
// parser owns slot validation, so malformed slot values still get useful errors.
func booleanRuleExpressionStart(line []byte) (int, bool) {
	word, pos := nextCommentInputWord(line, 0)
	if !asciiWordEqual(word, "PASS") && !asciiWordEqual(word, "REJECT") {
		return 0, false
	}
	word, pos = nextCommentInputWord(line, pos)
	if !asciiWordEqual(word, "RULE") {
		return 0, false
	}
	word, pos = nextCommentInputWord(line, pos)
	if len(word) == 0 || pos == len(line) {
		return 0, false
	}
	for pos < len(line) && line[pos] == ' ' {
		pos++
	}
	return pos, true
}

func booleanInputNeedsHint(line []byte, b byte) bool {
	switch b {
	case '=', '<', '>', '(', ')', '[', ']', '"':
		word, _ := nextCommentInputWord(line, 0)
		return asciiWordEqual(word, "PASS") || asciiWordEqual(word, "REJECT")
	default:
		return false
	}
}

// Rule syntax shares framing, editing, recovery and machine-header ownership
// with login and ordinary command input.
func (c *Client) readBooleanCommandLine(maxLen int) (string, error) {
	return c.readInputLineWithRuleSyntax(maxLen, "command", true, true, true, true, true, true)
}

// File role: Identifies the rule expression in the edited Telnet line so the
// reader can preserve literal bytes. Staged GO edits own the rule 0 guard.
package telnet

// booleanRuleExpressionStart reads the edited input buffer without allocating.
// After PASS RULE and a space, the next byte may already be expression syntax.
// A numeric-looking token still owns the slot position and must be validated
// by the command parser. REJECT always waits for a slot token and a space.
func booleanRuleExpressionStart(line []byte) (start int, omittedSlot bool, ok bool) {
	word, pos := nextCommentInputWord(line, 0)
	pass := asciiWordEqual(word, "PASS")
	if !pass && !asciiWordEqual(word, "REJECT") {
		return 0, false, false
	}
	word, pos = nextCommentInputWord(line, pos)
	if !asciiWordEqual(word, "RULE") || pos == len(line) {
		return 0, false, false
	}
	for pos < len(line) && line[pos] == ' ' {
		pos++
	}
	if pass && (pos == len(line) || !booleanRuleSlotLead(line[pos])) {
		return pos, true, true
	}
	word, pos = nextCommentInputWord(line, pos)
	if len(word) == 0 || pos == len(line) {
		return 0, false, false
	}
	for pos < len(line) && line[pos] == ' ' {
		pos++
	}
	return pos, false, true
}

func booleanRuleSlotLead(b byte) bool {
	return b >= '0' && b <= '9' || b == '+' || b == '-'
}

func booleanInputHint(line []byte, b byte, dialect DialectName, tooLong bool) string {
	word, _ := nextCommentInputWord(line, 0)
	reject := asciiWordEqual(word, "REJECT")
	if reject && dialect != DialectCC {
		return booleanRejectRuleUsage
	}
	if tooLong {
		return ""
	}
	switch b {
	case '=', '<', '>', '(', ')', '[', ']', '"':
		if reject || asciiWordEqual(word, "PASS") {
			return booleanRuleSetUsage
		}
	}
	return ""
}

// Rule syntax shares framing, editing, recovery and machine-header ownership
// with login and ordinary command input.
func (c *Client) readBooleanCommandLine(maxLen int) (string, error) {
	return c.readInputLineWithRuleSyntax(maxLen, "command", true, true, true, true, true, true)
}

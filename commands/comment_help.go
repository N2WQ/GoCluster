// Historical COMMENT mutation topics explain the numbered-rule replacement.
// History queries retain their separate literal remainder grammar.
package commands

func installCommentHelp(catalog *helpCatalog) {
	for _, topic := range []string{"PASS COMMENT", "REJECT COMMENT", "REMOVE PASS COMMENT", "REMOVE REJECT COMMENT", "RESET FILTER COMMENT", "SHOW FILTER COMMENT"} {
		catalog.entries[topic] = helpEntry{
			summary: topic + " - Replaced by numbered Boolean rules.",
			lines: helpEntryLines(topic+" - Replaced by numbered Boolean rules.", []string{"HELP RULE"}, nil, []string{
				"This category command is refused, not translated.",
				`Use COMMENT="CQ DX" inside a numbered PASS RULE or REJECT RULE expression.`,
				"COMMENT is an ASCII case-insensitive literal substring, not a regex.",
				`Double a quote inside a quoted value: COMMENT="CQ ""DX"""`,
				"Replace or clear the numbered slot to edit or remove the complete rule.",
				"Use SHOW FILTER RULE to read saved and effective expressions.",
			}),
		}
	}
	for _, topic := range []string{"SHOW DX", "SHOW/DX", "SHOW MYDX"} {
		entry, ok := catalog.entries[topic]
		if !ok {
			continue
		}
		entry.lines = appendNotes(entry.lines, []string{
			"Append BAND <list> and/or MODE <list> after the optional selector/count.",
			"Lists require commas between values; spaces around commas are allowed.",
			"OR within lists, AND between selections; for example BAND 20,40 MODE CW,FT8.",
			"BAND and MODE may appear in either order, at most once each.",
			"Bands accept 20 or 20m; modes use existing names/aliases, including UNKNOWN.",
			"Aliases named BAND/MODE are values first or after a comma; otherwise clauses.",
			"BAND/MODE ALL and NONE are invalid; omit a category to leave it unrestricted.",
			"Explicit BAND/MODE selections are required even for self-spots.",
			"Put COMMENT <phrase> last; it consumes the remaining literal text.",
			"The literal phrase is required even for self-spots; saved filters still apply.",
			"Uses stored comments, printable ASCII, case-insensitive matching, 1-64 bytes.",
			"Interior spaces and punctuation are literal; NEXT retains all selections.",
			"Searching does not change saved comment rules or other preferences.",
		})
		catalog.entries[topic] = entry
	}
}

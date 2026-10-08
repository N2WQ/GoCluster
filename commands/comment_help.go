// COMMENT help documents literal phrase ownership separately from list syntax.
// Execution belongs to telnet; history queries share the filter phrase matcher.
package commands

func installCommentHelp(catalog *helpCatalog) {
	add := func(topic, summary string, usage, notes []string) {
		catalog.entries[topic] = helpEntry{summary: topic + " - " + summary, lines: helpEntryLines(topic+" - "+summary, usage, nil, notes)}
		catalog.order = append(catalog.order, topic)
	}
	for _, verb := range []string{"PASS", "REJECT"} {
		add(verb+" COMMENT", "Add a literal comment phrase.", []string{verb + " COMMENT <phrase>"}, []string{
			"Matches stored spot comments using case-insensitive literal substrings.",
			"Everything after COMMENT is one phrase; interior spaces are preserved.",
			"Printable ASCII including punctuation; 1-64 bytes after edge trimming.",
			"Commas, quotes, * and ? are literal; ALL and NONE are literal phrases.",
			"Limit: 32 PASS and 32 REJECT phrases; repeats accumulate distinct phrases.",
			"Any PASS phrase qualifies; any REJECT phrase blocks. Other filters apply.",
			"An active PASS list rejects empty or nonmatching comments.",
			"The same phrase moves from the opposite list; rejected edits change neither.",
			"Rules persist across reconnects and presets; changes invalidate history NEXT.",
		})
		add("REMOVE "+verb+" COMMENT", "Remove one comment phrase.", []string{"REMOVE " + verb + " COMMENT <phrase>"}, []string{
			"Removes every case-equivalent entry in the selected list.",
			"A missing phrase leaves that list unchanged; the opposite list is retained.",
		})
	}
	add("RESET FILTER COMMENT", "Clear comment selections.", []string{"RESET FILTER COMMENT [PASS|REJECT]"}, []string{
		"With PASS or REJECT, clears only that list. Without either, clears both.",
		"RESET FILTER and PASS NOFILTER also clear all comment rules.",
	})
	add("SHOW FILTER COMMENT", "Display saved comment phrases.", []string{"SHOW FILTER COMMENT"}, []string{
		"Shows PASS and REJECT phrases; punctuation and internal spaces are literal.",
		"Human readbacks use escaped exact values and bounded 78-byte lines.",
	})
	for _, topic := range []string{"SHOW DX", "SHOW/DX", "SHOW MYDX"} {
		entry, ok := catalog.entries[topic]
		if !ok {
			continue
		}
		entry.lines = appendNotes(entry.lines, []string{
			"Append COMMENT <phrase> after the optional selector and count (either order).",
			"The literal phrase is required even for self-spots; saved filters still apply.",
			"Uses stored comments, printable ASCII, case-insensitive matching, 1-64 bytes.",
			"Interior spaces and punctuation are literal; NEXT retains the original phrase.",
			"Searching does not change saved comment rules or other preferences.",
		})
		catalog.entries[topic] = entry
	}
}

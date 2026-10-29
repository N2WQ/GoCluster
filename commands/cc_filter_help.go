// CC retains separate control syntax, not category-to-rule translation.
package commands

func ccFilterHelpLines(verb string) []string {
	summary := "Enable separate controls"
	if verb == "UNSET/FILTER" {
		summary = "Disable separate controls"
	}
	usage := []string{
		verb + " <switch> [ALL]",
	}
	lines := helpEntryLines(verb+" - "+summary+" (CC dialect).", usage, nil, []string{
		"NEARBY belongs in numbered Boolean rules; old toggle commands are refused.",
		"Switches accept no value or ALL; other values are refused. PC93 aliases ANNOUNCE.",
		"Category lists, MINSNR, DXBM and NOFILTER are refused, not translated.",
		"Use PASS RULE or REJECT RULE in either dialect; see HELP RULE.",
		"RESET FILTER clears personal rules, preserving separate controls.",
		"UNSET/FILTER does not accept inline /ON or /OFF.",
	})
	lines = appendListSection(lines, "Switches:", []string{"BEACON", "WWV", "WCY", "ANNOUNCE", "SELF", "TOXIC"})
	if verb == "SET/FILTER" {
		lines = appendNotes(lines, []string{
			"SET/FILTER <switch>/ON or /OFF enables or disables a listed switch.",
			"Flags take no extra arguments. NEARBY is a rule predicate.",
			"Example: SET/FILTER BEACON/OFF",
		})
	}
	return lines
}

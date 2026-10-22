// HELP owns only presentation; telnet owns rule admission and mutation.
package commands

func installBooleanRuleHelp(catalog *helpCatalog) {
	lines := []string{"RULE - Numbered Boolean personal filters.", "Usage: HELP RULE"}
	for _, text := range BooleanRuleHelpLines() {
		if len(text)+2 <= helpMaxWidth {
			// In particular, do not collapse spaces inside quoted COMMENT values.
			lines = append(lines, "  "+text)
		} else {
			lines = append(lines, wrapTextLines(text, "  ")...)
		}
	}
	catalog.entries["RULE"] = helpEntry{summary: "RULE - Numbered Boolean personal filters.", lines: lines}
	for _, verb := range []string{"PASS", "REJECT"} {
		summary := "Allow matching spots or enable separate controls."
		if verb == "REJECT" {
			summary = "Veto matching spots or disable separate controls."
		}
		notes := []string{
			"Rule syntax is identical in GO and CC. Setting a slot replaces its action and full expression.",
			"PASS rules are ORed; a matching REJECT vetoes them. Slot IDs are not priorities.",
			"See HELP RULE for expressions, supported fields and values, defaults and limits.",
			"Controls remain separate from expressions. PC93 aliases ANNOUNCE.",
			"Controls accept no value or ALL; other values are refused. NEARBY takes ON or OFF.",
			"Old category mutation syntax is refused, not translated; use HELP RULE.",
		}
		catalog.entries[verb] = helpEntry{
			summary: verb + " - " + summary,
			lines: helpEntryLines(verb+" - "+summary, []string{
				verb + " RULE <0..9> <expression>",
				verb + " <BEACON|WWV|WCY|ANNOUNCE|SELF|TOXIC> [ALL]",
				verb + " NEARBY ON|OFF",
			}, nil, notes),
		}
		topic := verb + " RULE"
		catalog.entries[topic] = helpEntry{
			summary: topic + " - Replace a numbered personal rule.",
			lines: helpEntryLines(topic+" - Replace a numbered personal rule.",
				[]string{topic + " <0..9> <expression>"}, nil, notes[:3]),
		}
	}
	catalog.entries["RESET FILTER"] = helpEntry{
		summary: "RESET FILTER - Reset filters without restoring defaults.",
		lines: helpEntryLines("RESET FILTER - Reset filters without restoring defaults.",
			[]string{"RESET FILTER", "RESET FILTER RULE <0..9|ALL>"}, nil, []string{
				"RESET FILTER clears all personal rules and turns NEARBY OFF.",
				"RESET FILTER RULE clears only the selected slots; NEARBY is unchanged.",
				"Neither resets separate controls or profile settings; neither reinstalls default slot 0.",
				"No rules means no personal restriction. See HELP RULE.",
			}),
	}
	catalog.entries["RESET FILTER RULE"] = helpEntry{
		summary: "RESET FILTER RULE - Clear numbered personal rules.",
		lines: helpEntryLines("RESET FILTER RULE - Clear numbered personal rules.",
			[]string{"RESET FILTER RULE <0..9|ALL>"}, nil, []string{
				"Clears one slot, or all slots with ALL. NEARBY, separate controls and profile settings are unchanged.",
				"Does not restore default slot 0. See HELP RULE.",
			}),
	}
	catalog.order = append(catalog.order, "PASS RULE", "REJECT RULE", "RESET FILTER RULE", "RULE")
}

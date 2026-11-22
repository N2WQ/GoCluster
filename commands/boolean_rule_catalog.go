// HELP owns only presentation; telnet owns rule admission and mutation.
package commands

import "slices"

func installBooleanRuleHelp(catalog *helpCatalog, dialect string) {
	installDeliverySwitchHelp(catalog, dialect)
	if dialect == "cc" {
		installNativeCCHelpBoundary(catalog)
		return
	}
	lines := []string{
		"PASS and REJECT rules",
		"",
		"Start with SHOW FILTER to see your rules.",
		"Rule numbers run from 0 to 9. Using a number again replaces that rule.",
		"Numbers do not set priority.",
		"",
		"How rules work together",
		"  A matching REJECT rule blocks a spot, even if a PASS rule matches.",
		"  If you have PASS rules, a spot must match at least one of them.",
		"  With no PASS rules, only matching REJECT rules block spots.",
		"  Other delivery settings still apply. SELF can let spots of your call",
		"  bypass your rules. Other delivery checks still apply.",
		"",
		"Examples (each command replaces the rule at that number)",
		"  PASS RULE 0 MODE=CW,FT8",
		"    Allow CW or FT8 under rule 0. Other PASS rules still apply.",
		"  REJECT RULE 9 BAND=20M AND MODE=FT8",
		"    Block FT8 on 20m, even if a PASS rule allows it.",
		"",
		"To narrow a PASS rule, edit it and add a condition with AND.",
		"To limit rule 0 to human reports: PASS RULE 0 MODE=CW,FT8 AND SOURCE=HUMAN",
		"To remove rule 9: RESET FILTER RULE 9",
		"Removing a rule does not bring back the rule it replaced.",
		"",
		"For WWV, WCY, announcements, or beacons, use the delivery switches.",
		"See HELP PASS or HELP REJECT.",
		"",
	}
	for _, text := range BooleanRuleHelpLines() {
		if text == "" {
			lines = append(lines, "")
			continue
		}
		if len(text)+2 <= helpMaxWidth {
			// In particular, do not collapse spaces inside quoted COMMENT values.
			lines = append(lines, "  "+text)
		} else {
			lines = append(lines, wrapTextLines(text, "  ")...)
		}
	}
	catalog.entries["RULE"] = helpEntry{summary: "RULE - Numbered Boolean personal filters.", lines: lines}
	catalog.entries["PASS RULE"] = helpEntry{
		summary: "PASS RULE - Replace a numbered personal rule.",
		lines: []string{
			"PASS RULE <0-9> <expression>",
			"Example: PASS RULE 0 MODE=CW,FT8",
			"This replaces rule 0. A comma means either mode matches.",
			"Adding a PASS rule can let in more spots. Edit a rule to narrow it.",
			"A matching REJECT rule still blocks the spot. See HELP RULE.",
		},
	}
	catalog.entries["REJECT RULE"] = helpEntry{
		summary: "REJECT RULE - Replace a numbered personal rule.",
		lines: []string{
			"REJECT RULE <0-9> <expression>",
			"Example: REJECT RULE 9 BAND=20M AND MODE=FT8",
			"This replaces rule 9. AND means both conditions must match.",
			"A matching REJECT rule blocks a spot even if a PASS rule allows it.",
			"To remove this rule, type RESET FILTER RULE 9. See HELP RULE.",
		},
	}

	catalog.entries["RESET FILTER"] = helpEntry{
		summary: "RESET FILTER - Reset filters without restoring defaults.",
		lines: helpEntryLines("RESET FILTER - Reset filters without restoring defaults.",
			[]string{"RESET FILTER", "RESET FILTER RULE <0..9|ALL>"}, nil, []string{
				"RESET FILTER clears all personal rules.",
				"RESET FILTER RULE clears only the selected slots.",
				"Neither resets separate controls or profile settings; neither reinstalls default slot 0.",
				"No rules means no personal restriction. See HELP RULE.",
			}),
	}
	catalog.entries["RESET FILTER RULE"] = helpEntry{
		summary: "RESET FILTER RULE - Clear numbered personal rules.",
		lines: helpEntryLines("RESET FILTER RULE - Clear numbered personal rules.",
			[]string{"RESET FILTER RULE <0..9|ALL>"}, nil, []string{
				"Clears one slot, or all slots with ALL. Separate controls and profile settings are unchanged.",
				"Does not restore default slot 0. See HELP RULE.",
			}),
	}
	catalog.order = append(catalog.order, "PASS RULE", "REJECT RULE", "RESET FILTER RULE", "RULE")
}

func installDeliverySwitchHelp(catalog *helpCatalog, dialect string) {
	for _, verb := range []string{"PASS", "REJECT"} {
		lines := []string{
			"Spot rules (GO only)",
			"  PASS RULE 0 MODE=CW       Allow CW under rule 0.",
			"  REJECT RULE 9 MODE=FT8    Block FT8 even if a PASS rule allows it.",
			"  See HELP RULE for more examples.",
			"",
			"Delivery switches (GO and CC)",
			"  Switch     Turn on          Turn off",
			"  WWV        PASS WWV         REJECT WWV",
			"  WCY        PASS WCY         REJECT WCY",
			"  ANNOUNCE   PASS ANNOUNCE    REJECT ANNOUNCE",
			"  BEACON     PASS BEACON      REJECT BEACON",
			"",
			"WWV, WCY, and announcements are live messages, not spots.",
			"Numbered rules do not filter those messages.",
			"Beacon spots still go through your spot filters when BEACON is on.",
			"Changing a switch does not change any numbered rule.",
			"",
		}
		usage := []string{verb + " <BEACON|WWV|WCY|ANNOUNCE|SELF> [ALL]"}
		notes := []string{
			"SELF separately controls live spots of your own call.",
			"PC93 aliases ANNOUNCE. Switches accept no value or ALL.",
		}
		if dialect == "cc" {
			notes = append(notes, "Numbered PASS and REJECT rules are for GO. In CC, see HELP SET/FILTER.")
		} else {
			usage = append([]string{verb + " RULE <0..9> <expression>"}, usage...)
			notes = append(notes, "Old category mutation syntax is refused, not translated; use HELP RULE.")
		}
		summary := verb + " - Change separate delivery switches."
		if dialect != "cc" {
			summary = "PASS - Allow matching spots or enable separate controls."
			if verb == "REJECT" {
				summary = "REJECT - Veto matching spots or disable separate controls."
			}
		}
		details := helpEntryLines(summary, usage, nil, notes)
		catalog.entries[verb] = helpEntry{
			summary: summary,
			lines:   append(slices.Grow(lines, len(details)), details...),
		}
	}
}

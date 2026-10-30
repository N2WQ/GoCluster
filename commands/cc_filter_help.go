// Native CC HELP describes implemented commands and their qualified extensions.
package commands

import (
	"dxcluster/filter"
	"dxcluster/spot"
)

func ccFilterHelpLines(verb string) []string {
	usage := []string{verb + " <switch> [ALL]"}
	notes := []string{
		"Switches accept no value or ALL. PC93 aliases ANNOUNCE.",
		"SELF controls spots of your own call in GoCluster, not other users' self-spots.",
		"OWN, BOB, DX/NODX, TALK, WX and announcement/weather geography are unavailable.",
	}
	if verb == "SET/FILTER" {
		usage = append(usage,
			"SET/FILTER <DOC|DOS|DXCTY|DXSTATE>/<PASS|REJECT> <list>",
			"SET/FILTER <DOC|DOS|DXCTY|DXSTATE>/OFF",
			"SET/FILTER <country-list>/PASS or /REJECT",
			"SET/FILTER DXBM/PASS <band-or-slot-list>",
			"SET/FILTER DXBM/REJECT <band-or-slot-list>", "SET/FILTER DXBM/OFF")
		notes = append(notes,
			"DOC/DOS select spotter country/state; DXCTY/DXSTATE select the DX country/state.",
			"Lists accept commas or spaces. Countries use exact canonical CTY labels or positive ADIF integers; states/provinces use the existing US/Canadian codes.",
			"Each geography edit replaces that category's PASS/REJECT list. Global edits clear its band exceptions. Other categories remain unchanged.",
			"Values in one list use OR; enabled categories use AND. Missing/invalid metadata fails PASS and survives REJECT.",
			"OFF disables only that category. SET/NOFILTER restores current operator mode/source defaults and clears geography/DXBM, preserving six switches.",
			"DXBM names frequency slots, regardless of decoded mode; decoded-mode selection also applies. First PASS after OFF starts a whitelist; first REJECT starts a blacklist. Later edits keep other slots.",
			"The higher slot owns shared endpoints; each band's final upper endpoint is included.",
			"SET/FILTER <switch>/ON or /OFF changes a listed switch, without extra arguments.",
			"Per-band geography is a GoCluster schema-seven YAML extension. Its terminal grammar and CC User integration are unverified.",
			"The configured command-line limit still applies. Use YAML for long replacing lists; do not split them across replacing commands.",
			"Examples: SET/FILTER DOC/PASS K,VE; SET/FILTER DXBM/PASS 20-CW,40; SET/FILTER BEACON/OFF.")
	} else {
		notes = append(notes,
			"UNSET/FILTER does not accept inline /ON or /OFF or geography lists.",
			"Exact UNSET/<mode>, UNSET/SKIMMER and UNSET/<switch> shortcuts disable their selection and accept no arguments.",
			"Use SET/FILTER category/PASS, /REJECT or /OFF for native categories.")
	}
	lines := helpEntryLines(verb+" - Change native CC selections or switches.", usage, nil, notes)
	lines = appendListSection(lines, "Switches:", []string{"BEACON", "WWV", "WCY", "ANNOUNCE", "SELF", "TOXIC"})
	if verb == "SET/FILTER" {
		lines = appendListSection(lines, "Native bands:", filter.CCSupportedBands())
		nativeSlots := filter.CCSupportedSlots()
		slots := make([]string, 0, len(nativeSlots))
		for _, slot := range nativeSlots {
			slots = append(slots, slot.Name)
		}
		lines = appendListSection(lines, "Native slots:", slots)
		lines = appendListSection(lines, "State/province codes:", spot.StateCodes())
	}
	return lines
}

func nativeCCShortcutHelpLines(topic string) []string {
	notes := []string{"No additional arguments are accepted. Use SHOW/FILTER to inspect the result."}
	switch topic {
	case "SET/NOFILTER":
		notes = append(notes, "Restores current operator mode/source defaults and clears geography, band exceptions and DXBM. Keeps six switches, inactive GO bank and shared preferences.")
	case "SET/SKIMMER", "SET/NOSKIMMER", "UNSET/SKIMMER":
		notes = append(notes, "Changes only the SKIMMER source selection; modes stay unchanged and no feed is activated.")
	default:
		notes = append(notes,
			"Only modes with an enabled operator cc_shortcut flag are accepted; configured aliases normalize to their canonical mode.",
			"The mode selection applies to HUMAN and SKIMMER. Source selection is unchanged.",
			"SET/<MODE> enables; SET/NO<MODE> or UNSET/<MODE> disables. Empty mode/source selections admit no reports.")
	}
	return helpEntryLines(topic+" - Change a native CC selection.", []string{topic}, nil, notes)
}

func installNativeCCHelpBoundary(catalog *helpCatalog) {
	for _, name := range []string{"ANN", "ANNOUNCE", "BEACON", "WWV", "WCY", "SELF", "TOXIC"} {
		for _, prefix := range []string{"SET/", "SET/NO", "UNSET/"} {
			topic := prefix + name
			if _, exists := catalog.entries[topic]; exists {
				continue
			}
			catalog.entries[topic] = helpEntry{summary: topic + " - Change a separate switch.", lines: helpEntryLines(topic+" - Change a separate switch.", []string{topic}, nil, []string{"SET enables; SET/NO and UNSET disable the named switch.", "No additional arguments are accepted. Use SHOW/FILTER to inspect the result.", "SELF means spots of your own call in GoCluster."})}
		}
	}
	for _, topic := range []string{"RULE", "PASS RULE", "REJECT RULE", "RESET FILTER", "RESET FILTER RULE", "NEARBY", "PASS COMMENT", "REJECT COMMENT", "REMOVE PASS COMMENT", "REMOVE REJECT COMMENT", "RESET FILTER COMMENT", "SHOW FILTER COMMENT", "PASS MINSNR", "REJECT MINSNR"} {
		catalog.entries[topic] = helpEntry{summary: topic + " - GO dialect only.", lines: helpEntryLines(topic+" - GO dialect only.", []string{"DIALECT GO"}, nil, []string{
			"CC uses native mode/source, geography and DXBM selections; Boolean mutations are refused here.",
			"Switching banks preserves the other bank. Use HELP SET/FILTER or HELP FILTERS for native CC commands.",
		})}
	}
	for _, verb := range []string{"PASS", "REJECT"} {
		catalog.entries[verb] = helpEntry{summary: verb + " - Change separate delivery switches.", lines: helpEntryLines(verb+" - Change separate delivery switches.", []string{verb + " <BEACON|WWV|WCY|ANNOUNCE|SELF|TOXIC> [ALL]"}, nil, []string{
			"PASS enables; REJECT disables. PC93 aliases ANNOUNCE. Switches accept no value or ALL.",
			"SELF controls spots of your own call. Native geography uses SET/FILTER category/PASS or /REJECT.",
			"Boolean rule edits require GO; HELP SET/FILTER describes native CC selection.",
		})}
	}
	for _, topic := range []string{"SHOW/FILTER", "SH/FILTER"} {
		aliases := []string{"SHOW FILTER", "SH/FILTER"}
		if topic == "SH/FILTER" {
			aliases = []string{"SHOW/FILTER", "SHOW FILTER"}
		}
		catalog.entries[topic] = helpEntry{summary: topic + " - Inspect native CC state.", lines: helpEntryLines(topic+" - Inspect native CC state.", []string{topic + " [DOC|DOS|DXCTY|DXSTATE|DXBM|MODE|SOURCE|FULL]"}, aliases, []string{
			"Shows exact native selections and six controls. Band INHERIT differs from explicit OFF; DXBM OFF differs from active allow-all.",
			"Readbacks pause live spots through delivery and reading. RESUME ends the pause; missed spots are not replayed.",
			"GET YAML CONFIG SCHEMA 7 provides a complete both-bank backup. Schema 6 does not.",
		})}
	}
}

func nativeCCFilterReferenceLines() []string {
	return append([]string{"Native CC filter reference:", "  HELP SET/FILTER describes geography, frequency slots and six switches.", "  HELP SET/NOFILTER describes current operator defaults.", "  Boolean edits require DIALECT GO; switching preserves the other bank.", ""}, ccFilterHelpLines("SET/FILTER")...)
}

func nativeCCHelpOverviewLines() []string {
	lines := []string{"GoCluster help - CC dialect"}
	lines = helpSection(lines, "Getting started",
		helpRow{"SHOW/DX 10", "Show 10 recent spots matching your filters."},
		helpRow{"SHOW/FILTER", "Inspect active native CC selections."},
		helpRow{"SHOW SETTINGS", "Inspect preferences and session settings."},
		helpRow{"SET GRID FN31", "Set your grid square."}, helpRow{"BYE", "Disconnect."})
	lines = append(lines, "", "Type HELP followed by a command for details:", "  HELP SET/FILTER", "  HELP SET/NOFILTER", "  HELP FILTERS")
	lines = helpSection(lines, "Reading and posting spots",
		helpRow{"SHOW/DX | SH/DX", "Search recent spots matching your filters."},
		helpRow{"SHOW MYDX", "Same history with explicit query selections."}, helpRow{"DX", "Post a spot."},
		helpRow{"SHOW DXCC", "Look up callsign or country metadata."}, helpRow{"WHOSPOTSME", "Show recent spotter countries for your call."})
	lines = helpSection(lines, "Space weather bulletins",
		helpRow{"SHOW WWV [FULL]", "Show WWV bulletins; FULL selects retained history."},
		helpRow{"SHOW WCY [FULL]", "Show WCY bulletins; FULL selects retained history."})
	lines = helpSection(lines, "Examples",
		helpRow{"SHOW/DX 10 BAND 20", "Search for 10 matching spots on 20m."},
		helpRow{"SHOW/DX 10 MODE CW", "Search for 10 matching CW spots."},
		helpRow{"SHOW/DX 10 COMMENT CQ", "Search stored comments for literal CQ."},
		helpRow{"DX 14025.0 K1ABC CQ", "Post a DX spot."})
	lines = helpSection(lines, "Changing filters",
		helpRow{"SET/FILTER", "Replace geography or edit frequency slots."},
		helpRow{"SET/<MODE>", "Enable an operator-configured mode shortcut."},
		helpRow{"SET/NO<MODE>", "Disable an operator-configured mode shortcut."},
		helpRow{"UNSET/<MODE>", "Disable the same configured mode."},
		helpRow{"SET/SKIMMER", "Enable the SKIMMER source selection."},
		helpRow{"SET/NOSKIMMER", "Disable the SKIMMER source selection."},
		helpRow{"UNSET/SKIMMER", "Disable the SKIMMER source selection."},
		helpRow{"SET/NOFILTER", "Restore operator defaults; keep switches."},
		helpRow{"SHOW/FILTER FULL", "Show native selections, controls and DXBM basis."},
		helpRow{"SH/FILTER", "Short form of SHOW/FILTER."})
	lines = append(lines, "", "What you can filter:",
		"  DOC/DOS select spotter country/state; DXCTY/DXSTATE select the DX.",
		"  Mode and source selections both apply. Geography categories use AND.",
		"  Values within one list use OR. Missing data fails PASS and survives REJECT.",
		"  OFF disables only its category. DXBM selects frequency, not decoded mode.",
		"", "A complete recipe:", "  SET/NOFILTER", "  SET/FILTER DOC/PASS K,VE", "  SET/FILTER DXBM/PASS 20-CW,40", "  SHOW/FILTER",
		"", "Filter rules to remember:",
		"  Geography edits replace their list and clear that category's band exceptions.",
		"  DXBM edits are incremental; first PASS after OFF starts a whitelist.",
		"  First REJECT starts a blacklist. Active allow-all remains active.",
		"  Mode shortcuts require the operator's cc_shortcut flag.",
		"  Per-band geography uses native schema-seven YAML; terminal syntax and",
		"  CC User integration for per-band geography remain unverified.",
		"  Long lists need YAML; splitting replacing commands loses earlier values.",
		"  Boolean mutations require DIALECT GO, which preserves the CC bank.",
		"", "Comment rules:", "  SHOW/DX COMMENT is a history search, not a saved CC category.")
	lines = helpSection(lines, "Separate switches",
		helpRow{"PASS | REJECT", "Enable or disable the six separate switches."},
		helpRow{"UNSET/FILTER", "Disable a named switch; see HELP."},
		helpRow{"SET/ANN | SET/NOANN", "Allow or block announcements."},
		helpRow{"SET/BEACON | SET/NOBEACON", "Allow or block beacon spots."},
		helpRow{"SET/WWV | SET/NOWWV", "Allow or block WWV bulletins."},
		helpRow{"SET/WCY | SET/NOWCY", "Allow or block WCY bulletins."},
		helpRow{"SET/SELF | SET/NOSELF", "Allow or block spots of your own call."})
	lines = append(lines, "  TOXIC is a GoCluster switch for already classified toxic spots.",
		"  OWN, BOB, DX/NODX, TALK, WX and announcement/weather geography are absent.",
		"", "Nearby filtering:",
		"  NEARBY belongs to the GO expression bank; CC adds no proximity gate.")
	lines = append(lines, preferencesOverviewLines()...)
	lines = helpSection(lines, "Saving and loading presets",
		helpRow{"SAVE PRESET <name>", "Save the active bank and shared preferences."},
		helpRow{"LIST PRESET", "List names shared by your numeric SSIDs."},
		helpRow{"LOAD PRESET <name>", "Apply its tagged bank and announce the selection."},
		helpRow{"DELETE PRESET <name>", "Remove the named preset."})
	lines = helpSection(lines, "Pausing live spots",
		helpRow{"PAUSE", "Pause live spots for 30 seconds."},
		helpRow{"PAUSE 60", "Pause for 60 seconds (maximum 300)."},
		helpRow{"SHOW HOLD", "Show the remaining pause and suppressed spots."},
		helpRow{"RESUME", "Resume live spots immediately."})
	lines = append(lines, "  Filter/settings readbacks pause live spots; missed spots are not replayed.")
	lines = helpSection(lines, "Diagnostics", helpRow{"SHOW BUILD", "Show version/build information."},
		helpRow{"SHOW OWN", "Show login and baseline own-call identity."}, helpRow{"SET DIAG", "Choose diagnostic spot comments."})
	lines = helpSection(lines, "More help", helpRow{"HELP FILTERS", "Native CC rules and support boundaries."},
		helpRow{"HELP SYMBOLS", "Confidence and path symbols."}, helpRow{"HELP <command>", "Detailed command help."})
	lines = helpSection(lines, "Other commands", helpRow{"HELP", "Show this overview."},
		helpRow{"DIALECT", "Inspect or switch independent GO/CC banks."}, helpRow{"BYE", "Disconnect (also QUIT or EXIT)."})
	lines = append(lines, "", "Syntax used in detailed help:", "  <value> is required; [value] is optional. A|B means choose one.")
	var wrapped []string
	for _, line := range lines {
		if len(line) > helpMaxWidth {
			wrapped = append(wrapped, wrapTextLines(line, "  ")...)
		} else {
			wrapped = append(wrapped, line)
		}
	}
	return wrapped
}

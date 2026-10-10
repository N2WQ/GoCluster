// File role: Describes existing CC filter grammar by its actual category owner.
// Rendering is bounded by the finite vocabulary and normal HELP wrapping. This
// catalog adds no executable alias; telnet owns admission and persistence.
package commands

func ccFilterListTypes() []string {
	var names []string
	for _, name := range filterListTypes() {
		if name != "COMMENT" && name != "MINSNR" {
			names = append(names, name)
		}
	}
	return names
}

func ccFilterHelpLines(verb string) []string {
	summary, clear := "Allow filter matches", "ALL"
	if verb == "UNSET/FILTER" {
		summary, clear = "Block filter matches", "NONE"
	}
	usage := []string{
		verb + " <list-type> <values>",
		verb + " MINSNR <modes|ALL> <integer dB|" + clear + ">",
		verb + " NEARBY ON|OFF",
		verb + " <switch> [ALL]",
		verb + " DXBM <band-codes>",
	}
	if verb == "SET/FILTER" {
		usage = append(usage, "SET/FILTER NOFILTER [ALL]",
			"SET/FILTER DXBM PASS|REJECT <band-codes>",
			"SET/FILTER DXBM/PASS <band-codes>",
			"SET/FILTER DXBM/REJECT <band-codes>")
	}
	lines := helpEntryLines(verb+" - "+summary+" (CC dialect).", usage, nil, []string{
		"Ordinary lists use the existing PASS/REJECT rules; MODE preserves unlisted modes.",
		"List values accept commas or spaces. SOURCE takes one HUMAN, SKIMMER or ALL.",
		"MINSNR numeric values set the same inclusive minimum with either verb.",
		"Clear MINSNR with " + verb + " MINSNR <modes|ALL> " + clear + ".",
		"NEARBY follows its explicit ON/OFF value with either verb; ON needs usable GRID cells.",
		"Switches accept no value or ALL; other values are refused. PC93 aliases ANNOUNCE.",
		"COMMENT uses PASS/REJECT COMMENT <phrase>; see HELP PASS COMMENT.",
		"SET/FILTER COMMENT and UNSET/FILTER COMMENT are unsupported.",
		"NOFILTER clears restrictions; RESET FILTER restores new-user defaults.",
		"UNSET/FILTER does not accept inline /ON or /OFF, or NOFILTER.",
		"DXBM takes numeric band codes, not ALL. It maps codes to BAND; modes are ignored.",
		"DXBM bands: 160, 80, 40, 30, 20, 17, 15, 12, 10, 6, 2 (1 if enabled).",
		"DXDXCC/DEDXCC accept canonical CTY prefixes or positive ADIF numbers.",
		"DXSTATE/DESTATE accept US state and Canadian province/territory codes.",
		"Unknown state fails an explicit PASS list and passes a REJECT-only list.",
		"State and other geography filters are suspended and retained while NEARBY is enabled.",
	})
	lines = appendListSection(lines, "CC list types:", ccFilterListTypes())
	lines = appendListSection(lines, "Switches:", []string{"BEACON", "WWV", "WCY", "ANNOUNCE", "SELF", "TOXIC"})
	if verb == "SET/FILTER" {
		lines = appendNotes(lines, []string{
			"SET/FILTER <list-type>/ON or /OFF applies ALL to a listed ordinary category.",
			"SET/FILTER <switch>/ON or /OFF enables or disables a listed switch.",
			"SET/FILTER NOFILTER/ON clears restrictions; NOFILTER/OFF is unsupported.",
			"No inline flags for COMMENT, MINSNR, NEARBY or DXBM. Flags take no extra arguments.",
			"Example: SET/FILTER BAND/ON",
		})
	}
	return lines
}

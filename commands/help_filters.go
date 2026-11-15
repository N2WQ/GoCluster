// Filter guides teach the active dialect's existing commands. Recipes state
// replacement boundaries before mutations; detailed syntax stays in its catalog.
package commands

func filterGuideHelpLines(dialect string) []string {
	if dialect == "cc" {
		return []string{
			"Choose which spots you receive:",
			"  SHOW/FILTER", "    Show your current filters.",
			"    This pauses live spots. Type RESUME when ready.",
			"", "How selections work:",
			"  Your band, mode, source and country selections all apply.",
			"  For example, selecting 20m does not turn on a disabled mode.",
			"  A country or state list replaces that category's old choices.",
			"  Band edits can keep earlier band choices.",
			"", "Other settings can still hide spots.",
			"With SELF on, spots of your own call can bypass your filters.",
			"", "What would you like to do?",
			"  HELP FILTER EXAMPLES   Try a complete filter setup.",
			"  HELP PRESETS           Save and switch between setups.",
			"  HELP SET/FILTER        See all filter options.",
			"  HELP SET/NOFILTER      Restore this cluster's starting selections.",
		}
	}
	return []string{
		"Choose which spots you receive:",
		"  SHOW FILTER", "    Show your current filters.",
		"    This pauses live spots. Type RESUME when ready.",
		"", "How rules work:",
		"  PASS rules choose spots you want.",
		"  A spot must match at least one PASS rule, if you have any.",
		"  Any matching REJECT rule blocks it.",
		"  With no PASS rules, only REJECT rules restrict your selection.",
		"  Adding another PASS rule can allow more spots.",
		"  To narrow a selection, change the existing rule.",
		"", "Other settings can still hide spots.",
		"With SELF on, spots of your own call can bypass your rules.",
		"", "What would you like to do?",
		"  HELP FILTER EXAMPLES   Try a complete filter setup.",
		"  HELP PRESETS           Save and switch between setups.",
		"  HELP RULE              Write or change individual rules.",
		"  HELP PASS              Change separate delivery settings.",
	}
}

func filterExampleHelpLines(dialect string) []string {
	var lines []string
	show := "SHOW FILTER"
	if dialect == "cc" {
		lines, show = ccFilterExampleHelpLines(), "SHOW/FILTER"
	} else {
		lines = goFilterExampleHelpLines()
	}
	return append(lines,
		"", show+" pauses live spots while you read.",
		"RESUME restarts them. Missed spots are not replayed.",
		"Other filters and delivery settings still apply.",
		"With SELF on, spots of your own call can bypass these filters.",
		"", "Keep a setup you like:", "  HELP PRESETS")
}

func goFilterExampleHelpLines() []string {
	return []string{
		"Human-reported CW and FT8 spots on 20m and 40m:",
		"  Removes all your GO rules. Your other settings stay unchanged.",
		"  To keep your current setup, see HELP PRESETS before changing it.",
		"  RESET FILTER RULE ALL",
		"  PASS RULE 0 BAND=20M,40M AND MODE=CW,FT8 AND SOURCE=HUMAN",
		"  SHOW FILTER", "  RESUME",
		"", "Skimmer CW spots on 20m and 40m:",
		"  Removes all your GO rules. Your other settings stay unchanged.",
		"  RESET FILTER RULE ALL",
		"  PASS RULE 0 BAND=20M,40M AND MODE=CW AND SOURCE=SKIMMER",
		"  SHOW FILTER", "  RESUME",
		"", "Allow any band, mode and source except FT8:",
		"  Removes all your GO rules. Your other settings stay unchanged.",
		"  RESET FILTER RULE ALL", "  REJECT RULE 0 MODE=FT8",
		"  SHOW FILTER", "  RESUME",
		"", "Remove all GO rule restrictions:",
		"  This clears your rules. It does not restore the starting rule.",
		"  Your other settings stay unchanged.",
		"  RESET FILTER", "  SHOW FILTER", "  RESUME",
	}
}

func ccFilterExampleHelpLines() []string {
	return []string{
		"Select the 20m and 40m frequency ranges:",
		"  Replaces all band and frequency-range choices.",
		"  Keeps your mode, source and country selections.",
		"  To keep your current setup, see HELP PRESETS before changing it.",
		"  SET/FILTER DXBM/OFF", "  SET/FILTER DXBM/PASS 20,40",
		"  SHOW/FILTER", "  RESUME",
		"", "Select reports from K or VE spotters:",
		"  Replaces your spotter-country list, including choices",
		"  for individual bands. Other filter categories stay unchanged.",
		"  SET/FILTER DOC/PASS K,VE", "  SHOW/FILTER", "  RESUME",
		"", "Hide FT8 spots:",
		"  Turns off FT8. Keeps your other selections.",
		"  Requires the FT8 command to be enabled on this cluster.",
		"  SET/NOFT8", "  SHOW/FILTER", "  RESUME",
		"", "Restore this cluster's starting selections:",
		"  Restores the operator's starting modes and report sources.",
		"  Clears country, state and band restrictions.",
		"  Keeps your beacon, own-call and other delivery settings.",
		"  This does not necessarily allow every spot.",
		"  SET/NOFILTER", "  SHOW/FILTER", "  RESUME",
	}
}

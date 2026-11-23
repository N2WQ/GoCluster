// Filter guides teach the active dialect's existing commands. Recipes state
// replacement boundaries before mutations; detailed syntax stays in its catalog.
package commands

func filterGuideHelpLines(dialect string) []string {
	if dialect == "cc" {
		return []string{
			"SHOW/FILTER shows your active filters.",
			"Band, mode, source, country, and state filters all apply together.",
			"For example, selecting 20m doesn't turn on a disabled mode.",
			"Replacing a country or state list removes its previous choices.",
			"Other delivery settings still apply. SELF can let your own spots bypass",
			"filters.",
			"HELP SET/FILTER lists the options. HELP FILTER EXAMPLES shows complete setups.",
		}
	}
	return []string{
		"SHOW FILTER shows your active rules. Use RESUME if live spots pause.",
		"Coming from AR-Cluster? Try this GO shortcut for human reports on 20m.",
		"  PASS RULE BAND=20M AND SOURCE=HUMAN",
		"This replaces all of rule 0. Other rules and delivery settings still apply.",
		"AR-Cluster's SET DX FILTER replaces the whole DX filter.",
		"Use PASS RULE 0 with your expression to replace a REJECT in rule 0.",
		"A matching REJECT blocks the spot. Include a number in every REJECT RULE.",
		"If you have PASS rules, a spot must match at least one of them.",
		"HELP RULE explains the syntax. HELP FILTER EXAMPLES shows complete setups.",
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
		"Coming from AR-Cluster? Try this GO shortcut for human reports on 20m.",
		"AR-Cluster command   SET DX FILTER BAND=20 AND NOT SKIMMER",
		"The GoCluster command sets rule 0 and replaces any PASS already there.",
		"This is enough when rule 0 is your only rule, as on a new GO account.",
		"If rule 1 allows FT8 on any band, it can still allow FT8 outside 20m.",
		"If rule 0 is a REJECT, include 0 in the PASS command to replace it.",
		"To keep your PASS and block 20m, use REJECT RULE 9 BAND=20M if 9 is free.",
		"  PASS RULE BAND=20M AND SOURCE=HUMAN", "  SHOW FILTER", "  RESUME",
		"",
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
		"", "CW spots on any band:",
		"  This recipe clears the existing GO rules before adding one.",
		"  RESET FILTER RULE ALL",
		"  PASS RULE 0 BAND=ALL AND MODE=CW",
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

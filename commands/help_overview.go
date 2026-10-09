// HELP presentation is compiled into the binary. The overview teaches common
// tasks; detailed catalogs still own command usage and aliases. Rendering is
// request-local, bounded by fixed rows and the existing supported-value lists.
package commands

import (
	"fmt"
	"strings"

	"dxcluster/filter"
	"dxcluster/spot"
)

type helpRow struct{ command, description string }

// helpRowLines aligns descriptions and wraps without breaking command syntax.
func helpRowLines(rows ...helpRow) []string {
	var lines []string
	width := 23
	for _, row := range rows {
		width = max(width, len(row.command)+1)
	}
	for _, row := range rows {
		prefix := fmt.Sprintf("  %-*s", width, row.command)
		line := prefix + row.description
		for len(line) > helpMaxWidth {
			cut := strings.LastIndexByte(line[:helpMaxWidth+1], ' ')
			lines = append(lines, strings.TrimRight(line[:cut], " "))
			line = strings.Repeat(" ", len(prefix)) + strings.TrimSpace(line[cut:])
		}
		lines = append(lines, strings.TrimRight(line, " "))
	}
	return lines
}

func helpSection(lines []string, title string, rows ...helpRow) []string {
	lines = append(lines, "", title+":")
	return append(lines, helpRowLines(rows...)...)
}

// filterVerb selects actual CC list syntax; COMMENT, MINSNR and NEARBY keep
// their shared PASS/REJECT spellings in either dialect.
func filterVerb(dialect, verb, category string) string {
	if dialect == "cc" && category != "COMMENT" && category != "MINSNR" && category != "NEARBY" {
		if verb == "PASS" {
			return "SET/FILTER"
		}
		return "UNSET/FILTER"
	}
	return verb
}

func filterCategoryRows() []helpRow {
	return []helpRow{
		{"BAND", "Radio band, such as 20m or 40m."},
		{"MODE", "Operating mode, such as CW, FT8 or USB."},
		{"SOURCE", "HUMAN or SKIMMER reports."},
		{"EVENT", "POTA, SOTA, IOTA, WWFF or LLOTA tags."},
		{"COMMENT", "A phrase anywhere in the spot comment."},
		{"DXCALL", "Spotted callsign or pattern, such as K1ABC or W1*."},
		{"DECALL", "Spotter callsign or pattern."},
		{"DXCONT", "Spotted station's continent."},
		{"DECONT", "Spotter's continent."},
		{"DXZONE", "Spotted station's CQ zone (1-40)."},
		{"DEZONE", "Spotter's CQ zone (1-40)."},
		{"DXDXCC", "Spotted station's country prefix or ADIF number."},
		{"DEDXCC", "Spotter's country prefix or ADIF number."},
		{"DXSTATE", "Spotted station's US state or Canadian province code."},
		{"DESTATE", "Spotter's US state or Canadian province code."},
		{"DXGRID2", "Spotted station's two-character grid, such as FN."},
		{"DEGRID2", "Spotter's two-character grid."},
		{"CONFIDENCE", "Callsign confidence symbols: ?, S, P, V, C, B."},
		{"PATH", "HIGH, MEDIUM, LOW, UNLIKELY, CLOSED or INSUFFICIENT."},
		{"MINSNR", "Minimum signal report in dB, selected by mode."},
	}
}

func filterExampleRows(dialect string) []helpRow {
	rows := []helpRow{
		{"PASS BAND 20,40", "Add 20m and 40m to your band selections."},
		{"REJECT BAND 80", "Block 80m spots."},
		{"PASS MODE CW,FT8", "Enable CW and FT8; other modes stay unchanged."},
		{"REJECT MODE FT8", "Disable FT8."},
		{"PASS SOURCE HUMAN", "Allow human reports."},
		{"REJECT SOURCE SKIMMER", "Block automated skimmer reports."},
		{"REJECT EVENT POTA", "Block POTA-tagged spots."},
		{"PASS COMMENT CQ", "Require a comment containing CQ."},
		{"REJECT COMMENT QRT", "Block comments containing QRT."},
		{"REJECT DXCALL W1*", "Block spotted calls beginning with W1."},
		{"REJECT DECALL W1ABC", "Block reports from spotter W1ABC."},
		{"PASS DXCONT EU", "Add Europe to your DX continent selections."},
		{"PASS DECONT NA", "Add North America to your spotter selections."},
		{"PASS DXZONE 14,15", "Add DX CQ zones 14 and 15."},
		{"PASS DEZONE 5", "Add spotters in CQ zone 5."},
		{"PASS DXDXCC DL", "Add Germany to your DX country selections."},
		{"PASS DEDXCC K", "Add the United States to your spotter countries."},
		{"PASS DXSTATE CT,MA", "Add DX stations in Connecticut and Massachusetts."},
		{"PASS DESTATE ON", "Add spotters with Canadian province code ON."},
		{"PASS DXGRID2 JO", "Add DX stations in grid field JO."},
		{"PASS DEGRID2 FN", "Add spotters in grid field FN."},
		{"REJECT CONFIDENCE ?", "Block spots marked with confidence symbol ?."},
		{"REJECT PATH LOW", "Block spots classified as LOW path reliability."},
		{"PASS MINSNR CW 10", "Set a minimum CW signal report of 10 dB."},
	}
	for i := range rows {
		parts := strings.SplitN(rows[i].command, " ", 3)
		rows[i].command = filterVerb(dialect, parts[0], parts[1]) + " " + parts[1] + " " + parts[2]
	}
	return rows
}

func filterRecipeLines(dialect string) []string {
	lines := []string{"", "A complete recipe: only 20m/40m and only CW/FT8:"}
	for _, row := range []struct{ verb, category, value string }{
		{"REJECT", "BAND", "ALL"}, {"PASS", "BAND", "20,40"},
		{"REJECT", "MODE", "ALL"}, {"PASS", "MODE", "CW,FT8"},
	} {
		lines = append(lines, "  "+filterVerb(dialect, row.verb, row.category)+" "+row.category+" "+row.value)
	}
	show := "SHOW FILTER"
	if dialect == "cc" {
		show = "SHOW/FILTER"
	}
	return append(lines, "  "+show, "", "  This changes BAND and MODE only. Other filters still apply.", "  Own-call spots have special exemptions; see HELP SHOW MYDX.")
}

func filterBasicsLines(dialect string) []string {
	passBand := filterVerb(dialect, "PASS", "BAND") + " BAND ALL"
	rejectEvent := filterVerb(dialect, "REJECT", "EVENT") + " EVENT ALL"
	return []string{
		"", "Filter rules to remember:",
		"  Allowed selections do not generally replace your existing selections.",
		"  Allowing moves named items to allowed; blocking moves them to blocked.",
		"  Different filter categories must all match, except own-call exemptions.",
		"  MODE changes only the modes you name.",
		"  " + passBand + " allows every band; other filters still apply.",
		"  " + rejectEvent + " blocks tagged events, not untagged spots.",
		"  COMMENT matches literal text without regard to letter case.",
		"  COMMENT treats commas, quotes, * and ? as literal characters.",
		"  COMMENT also treats ALL and NONE as literal phrases.",
		"  Any allowed COMMENT phrase can match; a blocked phrase wins.",
		"  Numeric PASS and REJECT MINSNR commands set the same minimum.",
		"  MINSNR exempts human reports and reports without SNR.",
		"  RESET FILTER restores cluster defaults, which may restrict spots.",
	}
}

func helpOverviewLines(dialect string) []string {
	show, short, readback, allow, block := "SHOW DX", "SH DX", "SHOW FILTER", "PASS", "REJECT"
	if dialect == "cc" {
		show, short, readback, allow, block = "SHOW/DX", "SH/DX", "SHOW/FILTER", "SET/FILTER", "UNSET/FILTER"
	}
	lines := []string{"GoCluster help - " + strings.ToUpper(dialect) + " dialect"}
	lines = helpSection(lines, "Getting started",
		helpRow{show + " 10", "Show 10 recent spots matching your filters."},
		helpRow{readback, "See your current filters."},
		helpRow{"SHOW SETTINGS", "See your preferences and session settings."},
		helpRow{"SET GRID FN31", "Set your Maidenhead grid square."},
		helpRow{"BYE", "Disconnect."})
	lines = append(lines, "", "Type HELP followed by a command for details:", "  HELP "+show, "  HELP "+allow, "  HELP "+block, "  HELP PASS COMMENT")
	lines = helpSection(lines, "Reading and posting spots",
		helpRow{show, "Show recent spots matching your filters."}, helpRow{"SHOW MYDX", "Same as " + show + "."},
		helpRow{short, "Short form of " + show + "."}, helpRow{"DX", "Post a spot."},
		helpRow{"SHOW DXCC", "Look up a callsign or country prefix."}, helpRow{"WHOSPOTSME", "Show recent spotter countries for your call."})
	lines = helpSection(lines, "Examples",
		helpRow{show + " 10", "Show the latest 10 matching spots."}, helpRow{show + " K1ABC 10", "Search for spots of K1ABC."},
		helpRow{show + " 10 BAND 20", "Search for 10 matching spots on 20m."}, helpRow{"DX 14025.0 K1ABC CQ", "Post K1ABC on 14025.0 kHz with comment CQ."})
	lines = helpSection(lines, "Changing filters",
		helpRow{allow, "Allow selections."}, helpRow{block, "Block selections."}, helpRow{readback, "Show your current filters."},
		helpRow{readback + " FULL", "Show complete filter selections."}, helpRow{readback + " MODE", "Show one filter category."},
		helpRow{"RESET FILTER", "Restore the cluster's default filters."})
	lines = append(lines, "", "What you can filter:", "  DX means the station being spotted; DE means the spotter.", "")
	lines = append(lines, helpRowLines(filterCategoryRows()...)...)
	lines = append(lines, "", allow+" and "+block+" examples:", "  Each example below is a separate change, not a combined recipe.", "")
	lines = append(lines, helpRowLines(filterExampleRows(dialect)...)...)
	lines = append(lines, filterRecipeLines(dialect)...)
	lines = append(lines, filterBasicsLines(dialect)...)
	lines = append(lines, commentOverviewLines()...)
	lines = append(lines, featureOverviewLines(dialect)...)
	lines = helpSection(lines, "Nearby filtering",
		helpRow{"SET GRID FN31", "Set your location first."}, helpRow{"PASS NEARBY ON", "Enable nearby filtering."}, helpRow{"PASS NEARBY OFF", "Disable nearby filtering."})
	lines = append(lines, "", "  NEARBY suspends location filters and retains their rules.", "  Type HELP PASS NEARBY for details.")
	lines = append(lines, preferencesOverviewLines()...)
	lines = helpSection(lines, "Saving and loading presets",
		helpRow{"SAVE PRESET <name>", "Save your filters and preferences under a name."}, helpRow{"LIST PRESET", "List your saved presets."},
		helpRow{"LOAD PRESET <name>", "Apply a preset and save this login's defaults."}, helpRow{"DELETE PRESET <name>", "Delete a saved preset."})
	lines = append(lines, "", "Examples:", "  SAVE PRESET CONTEST", "  LOAD PRESET CONTEST")
	lines = helpSection(lines, "Pausing live spots",
		helpRow{"PAUSE", "Pause live spots for 30 seconds."}, helpRow{"PAUSE 60", "Pause for 60 seconds (maximum 300)."},
		helpRow{"SHOW HOLD", "Show time remaining and spots suppressed."}, helpRow{"RESUME", "Resume live spots immediately."})
	lines = append(lines, "", "  Filter and settings readbacks temporarily pause live spots.", "  Type RESUME when ready, or wait for the pause to expire.", "  Spots missed during a pause are not replayed.")
	lines = helpSection(lines, "Diagnostics", helpRow{"SHOW BUILD", "Show server version and build information."},
		helpRow{"SHOW OWN", "Show your login call and own-call identity."}, helpRow{"SET DIAG", "Select diagnostic information in spot comments."})
	lines = helpSection(lines, "More help", helpRow{"HELP " + allow, "Complete allow rules and examples."}, helpRow{"HELP " + block, "Complete block rules and examples."},
		helpRow{"HELP FILTERS", "Filter reference and supported values."}, helpRow{"HELP SYMBOLS", "Confidence and path reliability symbols."}, helpRow{"HELP <command>", "Detailed help for a command."})
	lines = helpSection(lines, "Other commands", helpRow{"HELP", "Show this help."}, helpRow{"BYE", "Disconnect (also QUIT or EXIT)."})
	return append(lines, "", "Syntax used in detailed help:", "  <value> means required; [value] means optional.", "  A|B means choose one. Do not type the brackets.")
}

func commentOverviewLines() []string {
	lines := helpSection(nil, "Comment rules", helpRow{"PASS COMMENT", "Add an allowed comment phrase."},
		helpRow{"REJECT COMMENT", "Add a blocked comment phrase."}, helpRow{"REMOVE PASS COMMENT", "Remove an allowed phrase."},
		helpRow{"REMOVE REJECT COMMENT", "Remove a blocked phrase."}, helpRow{"RESET FILTER COMMENT", "Clear all comment rules."}, helpRow{"SHOW FILTER COMMENT", "Show your saved phrases."})
	return append(lines, "", "Examples:", "  PASS COMMENT CQ DX", "  REJECT COMMENT QRT", "  REMOVE PASS COMMENT CQ DX", "  RESET FILTER COMMENT REJECT")
}

func featureOverviewLines(dialect string) []string {
	if dialect == "cc" {
		return helpSection(nil, "CC shortcuts",
			helpRow{"SH/FILTER", "Short form of SHOW/FILTER."}, helpRow{"SET/ANN | SET/NOANN", "Allow or block announcements."},
			helpRow{"SET/BEACON | SET/NOBEACON", "Allow or block beacon spots."}, helpRow{"SET/WWV | SET/NOWWV", "Allow or block WWV bulletins."},
			helpRow{"SET/WCY | SET/NOWCY", "Allow or block WCY bulletins."}, helpRow{"SET/SELF | SET/NOSELF", "Allow or block your own spots."},
			helpRow{"SET/SKIMMER", "Allow automated skimmer reports."}, helpRow{"SET/NOSKIMMER", "Block automated skimmer reports."},
			helpRow{"SET/<MODE>", "Enable a mode; for example SET/FT8."}, helpRow{"SET/NO<MODE>", "Disable a mode; for example SET/NOFT8."},
			helpRow{"SET/NOFILTER", "Clear filter restrictions."}, helpRow{"REJECT TOXIC", "Hide human spots already classified as toxic."},
			helpRow{"PASS TOXIC", "Allow human spots already classified as toxic."})
	}
	lines := make([]string, 0, 16)
	lines = append(lines, "", "Feature switches:", "  Use PASS to enable or REJECT to disable:")
	lines = append(lines, helpRowLines(helpRow{"BEACON", "Beacon spots."}, helpRow{"WWV", "WWV bulletins."}, helpRow{"WCY", "WCY bulletins."},
		helpRow{"ANNOUNCE", "Announcements."}, helpRow{"SELF", "Your own spots."}, helpRow{"TOXIC", "Human spots already classified as toxic."})...)
	return helpSection(lines, "Examples", helpRow{"REJECT BEACON", "Hide beacon spots."}, helpRow{"PASS ANNOUNCE", "Allow announcements."}, helpRow{"REJECT TOXIC", "Hide spots already classified as toxic."})
}

func preferencesOverviewLines() []string {
	lines := helpSection(nil, "Preferences and propagation", helpRow{"SHOW SETTINGS", "Show preferences and session settings."},
		helpRow{"SET GRID", "Set your Maidenhead grid square."}, helpRow{"SET NOISE", "Set your receiving noise environment."},
		helpRow{"SHOW PROP", "Show an outlook to a callsign, prefix or grid."}, helpRow{"SET PATHSAMPLES", "Set the minimum observations for path estimates."},
		helpRow{"SHOW DEDUPE", "Show duplicate-spot suppression settings."}, helpRow{"SET DEDUPE", "Choose FAST, MED or SLOW duplicate suppression."},
		helpRow{"SET SOLAR", "Receive solar summaries or turn them off."}, helpRow{"DIALECT", "Show or switch between GO and CC command styles."})
	lines = append(lines, "", "Examples:", "  SET GRID FN31", "  SET NOISE SUBURBAN", "  SHOW PROP DL 20 CW", "  SET DEDUPE FAST")
	return append(lines, helpRowLines(helpRow{"SET SOLAR 30", "Receive solar summaries every 30 minutes."}, helpRow{"SET SOLAR OFF", "Stop solar summaries."})...)
}

func filterReferenceHelpLines(dialect string) []string {
	lines := []string{"Filter reference:", "  DX means the station being spotted; DE means the spotter.", ""}
	lines = append(lines, helpRowLines(filterCategoryRows()...)...)
	lines = append(lines, filterHelpLines(dialect)...)
	return append(lines, supportedFilterValuesLines()...)
}

func supportedFilterValuesLines() []string {
	lines := make([]string, 0, 32)
	for _, section := range []struct {
		title  string
		values []string
	}{
		{"List types:", filterListTypes()}, {"Supported modes:", filter.SupportedModes()},
		{"Supported events:", filter.SupportedEvents()}, {"Supported bands:", spot.SupportedBandNames()},
		{"Supported sources:", filter.SupportedSources}, {"Continents:", []string{"AF", "AN", "AS", "EU", "NA", "OC", "SA"}},
		{"State/province mailing-address codes:", spot.StateCodes()},
	} {
		lines = append(lines, "", section.title)
		lines = append(lines, wrapListLines(section.values)...)
	}
	return lines
}

// installConcreteFilterHelp adds teaching material without replacing the
// existing usage, aliases or exception notes. CC PASS/REJECT help describes
// their supported list equivalents rather than advertising GO-only execution.
func installConcreteFilterHelp(catalog *helpCatalog, dialect string) {
	for _, verb := range []string{"PASS", "REJECT"} {
		key := verb
		if dialect == "cc" {
			key = filterVerb(dialect, verb, "BAND")
		}
		entry := catalog.entries[key]
		entry.lines = append(entry.lines, "", "Categories:")
		entry.lines = append(entry.lines, helpRowLines(filterCategoryRows()...)...)
		entry.lines = append(entry.lines, "", "Examples (separate changes; other filters still apply):")
		entry.lines = append(entry.lines, helpRowLines(detailedFilterExampleRows(dialect, verb)...)...)
		entry.lines = append(entry.lines, filterRecipeLines(dialect)...)
		entry.lines = append(entry.lines, filterBasicsLines(dialect)...)
		entry.lines = appendNotes(entry.lines, []string{
			"Most lists accept commas or spaces. SOURCE takes one value; COMMENT consumes the remaining text as one phrase.",
			"CQ zones use integers 1-40; grid fields use two letters (for example FN or JO).",
			"DXCC values use canonical CTY prefixes or positive ADIF integers, not arbitrary callsigns; lookup depends on the server CTY database.",
			"CONFIDENCE accepts ?, S, P, V, C, B; PATH accepts HIGH, MEDIUM, LOW, UNLIKELY, CLOSED, INSUFFICIENT.",
			"Callsign patterns: exact call, W1* (starts with W1), or *ABC (ends with ABC). Only a leading or trailing * is supported; ? is not a wildcard.",
			"Geography selections are suspended by NEARBY; turn it OFF before editing them.",
			"Type HELP FILTERS for supported values, full rules and feature switches.",
			"Type HELP SYMBOLS for confidence and path symbol meanings.",
		})
		entry.lines = append(entry.lines, supportedFilterValuesLines()...)
		catalog.entries[key] = entry
		if dialect == "cc" {
			catalog.entries[verb] = entry
		}
	}
}

func detailedFilterExampleRows(dialect, verb string) []helpRow {
	rows := []helpRow{
		{"BAND 20,40", "20m and 40m bands."}, {"MODE CW,FT8", "CW and FT8 modes; other modes stay unchanged."},
		{"SOURCE HUMAN", "Human reports."}, {"EVENT POTA", "POTA-tagged spots; untagged spots are unaffected."},
		{"COMMENT CQ DX", "Comments containing the literal phrase CQ DX."},
		{"DXCALL W1*", "Spotted calls beginning with W1."}, {"DECALL W1ABC", "Reports from spotter W1ABC."},
		{"DXCONT EU", "DX stations in Europe."}, {"DECONT NA", "Spotters in North America."},
		{"DXZONE 14,15", "DX CQ zones 14 and 15."}, {"DEZONE 5", "Spotters in CQ zone 5."},
		{"DXDXCC DL", "DX stations in Germany."}, {"DEDXCC K", "Spotters in the United States."},
		{"DXSTATE CT,MA", "DX mailing-address codes CT and MA."}, {"DESTATE ON", "Spotter mailing-address code ON."},
		{"DXGRID2 JO", "DX stations in grid field JO."}, {"DEGRID2 FN", "Spotters in grid field FN."},
		{"CONFIDENCE ?", "Spots marked with confidence symbol ?."}, {"PATH LOW", "Spots classified as LOW path reliability."},
		{"MINSNR CW 10", "Set the inclusive CW minimum to 10 dB (either verb)."},
	}
	for i := range rows {
		category, _, _ := strings.Cut(rows[i].command, " ")
		rows[i].command = filterVerb(dialect, verb, category) + " " + rows[i].command
	}
	return rows
}

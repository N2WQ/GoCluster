// HELP presentation is compiled into the binary. The overview teaches common
// tasks; detailed catalogs still own command usage and aliases. Rendering is
// request-local, bounded by fixed rows and the existing supported-value lists.
package commands

import (
	"fmt"
	"strings"
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

func filterRecipeLines(dialect string) []string {
	show := "SHOW FILTER"
	if dialect == "cc" {
		show = "SHOW/FILTER"
	}
	return []string{
		"", "A complete recipe: human-reported CW/FT8 on 20m/40m:",
		"  RESET FILTER RULE ALL",
		"  PASS RULE 0 BAND=20M,40M AND MODE=CW,FT8 AND SOURCE=HUMAN",
		"  " + show,
		"", "  This replaces all personal rules. Separate controls are unchanged.",
		"  SELF ON can bypass personal rules; see HELP SHOW MYDX.",
	}
}

func filterBasicsLines() []string {
	return []string{
		"", "Filter rules to remember:",
		"  Slots 0..9 identify rules, not priorities. Each edit replaces a full rule.",
		"  Any PASS can qualify; any matching REJECT vetoes it.",
		"  No active PASS means unrestricted except for matching REJECT rules.",
		"  No rules means no personal restriction.",
		"  New users start with an ordinary editable default PASS rule in slot 0.",
		"  PASS without SOURCE allows humans and skimmers.",
		"  Add AND SOURCE=HUMAN to select human reports only.",
		"  To narrow it, replace/remove slot 0; adding another PASS broadens it.",
		"  RESET FILTER clears rules, preserving separate controls/preferences.",
		"  Reset does not reinstall default slot 0. See HELP RULE for full details.",
	}
}

func helpOverviewLines(dialect string) []string {
	show, short, readback := "SHOW DX", "SH DX", "SHOW FILTER"
	if dialect == "cc" {
		show, short, readback = "SHOW/DX", "SH/DX", "SHOW/FILTER"
	}
	lines := []string{"GoCluster help - " + strings.ToUpper(dialect) + " dialect"}
	lines = helpSection(lines, "Getting started",
		helpRow{show + " 10", "Show 10 recent spots matching your filters."},
		helpRow{readback, "See your current filters."},
		helpRow{"SHOW SETTINGS", "See your preferences and session settings."},
		helpRow{"SET GRID FN31", "Set your Maidenhead grid square."},
		helpRow{"BYE", "Disconnect."})
	lines = append(lines, "", "Type HELP followed by a command for details:", "  HELP "+show, "  HELP RULE", "  HELP PASS", "  HELP REJECT")
	lines = helpSection(lines, "Reading and posting spots",
		helpRow{show, "Show recent spots matching your filters."}, helpRow{"SHOW MYDX", "Same as " + show + "."},
		helpRow{short, "Short form of " + show + "."}, helpRow{"DX", "Post a spot."},
		helpRow{"SHOW DXCC", "Look up a callsign or country prefix."}, helpRow{"WHOSPOTSME", "Show recent spotter countries for your call."})
	lines = helpSection(lines, "Space weather bulletins",
		helpRow{"SHOW WWV", "Show the latest five WWV bulletins."}, helpRow{"SHOW WCY", "Show the latest five WCY bulletins."},
		helpRow{"SHOW WWV FULL", "Show retained WWV bulletins from the last 24 hours."},
		helpRow{"SHOW WCY FULL", "Show retained WCY bulletins from the last 24 hours."})
	lines = helpSection(lines, "Examples",
		helpRow{show + " 10", "Show the latest 10 matching spots."}, helpRow{show + " K1ABC 10", "Search for spots of K1ABC."},
		helpRow{show + " 10 BAND 20", "Search for 10 matching spots on 20m."},
		helpRow{show + " 10 MODE CW", "Search for 10 matching CW spots."},
		helpRow{show + " 10 COMMENT CQ", "Search stored comments for literal CQ."},
		helpRow{"DX 14025.0 K1ABC CQ", "Post K1ABC on 14025.0 kHz with comment CQ."})
	lines = helpSection(lines, "Changing filters",
		helpRow{"PASS RULE", "Replace one slot with an allowing expression."},
		helpRow{"REJECT RULE", "Replace one slot with a vetoing expression."},
		helpRow{readback, "See your current settings and rules."},
		helpRow{readback + " RULE", "Same as " + readback + "."},
		helpRow{readback + " FULL", "Show rules, controls and program counts."},
		helpRow{"RESET FILTER RULE", "Clear one slot or ALL; preserve settings."},
		helpRow{"RESET FILTER", "Clear personal rules; preserve settings."})
	lines = append(lines, "", "What you can filter:",
		"  Combine band, mode, source, geography, calls, comments and more.",
		"  DX means the station being spotted; DE means the spotter.",
		"  HELP RULE lists every field, supported value and expression limit.",
		"", "Examples (separate full-rule replacements):",
		"  PASS RULE 0 MODE=CW,FT8 AND SOURCE=HUMAN",
		"  REJECT RULE 9 SNR<-15 OR COMMENT=\"test beacon\"")
	lines = append(lines, filterRecipeLines(dialect)...)
	lines = append(lines, filterBasicsLines()...)
	lines = append(lines, commentOverviewLines()...)
	lines = append(lines, featureOverviewLines(dialect)...)
	lines = helpSection(lines, "Nearby filtering",
		helpRow{"SET GRID FN31", "Set your location."},
		helpRow{"PASS RULE 0 NEARBY", "Replace rule 0 with nearby selection."})
	lines = append(lines, "", "  NEARBY combines with other conditions using NOT, AND and OR.",
		"  It does not change geography conditions or duplicate suppression.",
		"  Type HELP NEARBY or HELP RULE for details.")
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
	lines = helpSection(lines, "More help", helpRow{"HELP RULE", "Canonical Boolean grammar, fields and values."},
		helpRow{"HELP PASS", "Rule updates and separate enable controls."},
		helpRow{"HELP REJECT", "Rule updates and separate disable controls."},
		helpRow{"HELP FILTERS", "Navigate to rules and separate controls."}, helpRow{"HELP SYMBOLS", "Confidence and path reliability symbols."}, helpRow{"HELP <command>", "Detailed help for a command."})
	lines = helpSection(lines, "Other commands", helpRow{"HELP", "Show this help."}, helpRow{"BYE", "Disconnect (also QUIT or EXIT)."})
	return append(lines, "", "Syntax used in detailed help:", "  <value> means required; [value] means optional.", "  A|B means choose one. Do not type the brackets.")
}

func commentOverviewLines() []string {
	return []string{
		"", "Comment rules:", `  PASS RULE 0 COMMENT="CQ DX"`,
		`  REJECT RULE 9 COMMENT="QRT"`,
		"  ASCII case-insensitive literal substrings, not regular expressions.",
		`  Inside a quoted value, double a literal quote: COMMENT="CQ ""DX"""`,
		"  Edit/clear the numbered slot to replace/remove the whole rule.",
		"  SHOW DX COMMENT remains a history query, not a saved rule edit.",
	}
}

func featureOverviewLines(dialect string) []string {
	if dialect == "cc" {
		return helpSection(nil, "CC shortcuts",
			helpRow{"SH/FILTER", "Short form of SHOW/FILTER."}, helpRow{"SET/ANN | SET/NOANN", "Allow or block announcements."},
			helpRow{"SET/BEACON | SET/NOBEACON", "Allow or block beacon spots."}, helpRow{"SET/WWV | SET/NOWWV", "Allow or block WWV bulletins."},
			helpRow{"SET/WCY | SET/NOWCY", "Allow or block WCY bulletins."}, helpRow{"SET/SELF | SET/NOSELF", "Allow or block your own spots."},
			helpRow{"SET/FILTER", "Separate switches only; see HELP."},
			helpRow{"UNSET/FILTER", "Separate switches only; see HELP."},
			helpRow{"REJECT TOXIC", "Hide human spots already classified as toxic."},
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
		helpRow{"SHOW PROP", "Show an outlook to a callsign, prefix or grid."}, helpRow{"SET PATHSAMPLES", "Set the minimum recent report units for paths."},
		helpRow{"SHOW DEDUPE", "Show duplicate-spot suppression settings."}, helpRow{"SET DEDUPE", "Choose FAST, MED or SLOW duplicate suppression."},
		helpRow{"SET SOLAR", "Set the cadence for active solar summaries."}, helpRow{"DIALECT", "Show or switch between GO and CC command styles."})
	lines = append(lines, "", "Examples:", "  SET GRID FN31", "  SET NOISE SUBURBAN", "  SHOW PROP DL 20 CW", "  SET DEDUPE FAST")
	return append(lines, helpRowLines(helpRow{"SET SOLAR 30", "Receive active solar summaries every 30 minutes."}, helpRow{"SET SOLAR OFF", "Stop solar summaries."})...)
}

func filterReferenceHelpLines(dialect string) []string {
	lines := []string{"Filter reference:", "  HELP RULE is the canonical Boolean grammar, field and value reference."}
	lines = append(lines, filterBasicsLines()...)
	lines = append(lines, filterRecipeLines(dialect)...)
	lines = append(lines, featureOverviewLines(dialect)...)
	return append(lines, "", "  HELP SYMBOLS explains confidence and path reliability symbols.",
		"  Old category edits and CC filter/mode shortcuts are not translated.")
}

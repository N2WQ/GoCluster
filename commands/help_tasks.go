// Task HELP pages teach existing commands without executing them. All content
// and indexes are request-owned and bounded by the finite compiled HELP catalog.
package commands

import (
	"fmt"
	"slices"
	"strings"
)

func helpTaskCommands(dialect string) (show, readback string) {
	if dialect == "cc" {
		return "SHOW/DX", "SHOW/FILTER"
	}
	return "SHOW DX", "SHOW FILTER"
}

func installTaskHelp(catalog *helpCatalog, dialect string) {
	show, readback := helpTaskCommands(dialect)
	pages := []struct {
		topic   string
		summary string
		lines   []string
	}{
		{"SPOTS", "Find spots or report a station.", []string{
			"Find spots or report a station:",
			"  " + show + " 10", "  Show up to 10 recent spots that match your filters.",
			"", "Find a call, band or mode:",
			"  " + show + " K1ABC 10", "  " + show + " 10 BAND 20", "  " + show + " 10 MODE CW",
			"  These searches do not change your saved filters.",
			"", "Report a station:", "  DX 14025.0 K1ABC CQ",
			"  This posts a real spot. Use a station you heard.",
			"  Enter the frequency in kHz.",
			"  A queued spot still needs processing before delivery.",
			"", "More help:", "  HELP " + show, "  HELP BANDS", "  HELP DX", "  HELP SHOW DXCC", "  HELP WHOSPOTSME",
		}},
		{"SETTINGS", "Change your location and settings.", []string{
			"Change your location and settings:",
			"  SHOW SETTINGS", "  Show your saved settings and session details.",
			"", "Set your location:", "  SET GRID FN31", "  Replace FN31 with your own grid square.",
			"  This saves your location.",
			"", "Choose other settings:",
			"  HELP SET NOISE", "  Choose the noise level at your station.",
			"  HELP SET DEDUPE", "  Choose how often repeated spots appear.",
			"  HELP SET CWSPEED", "  Show or hide CW speed in reports.",
			"  HELP SET SOLAR", "  Choose when to receive solar summaries.",
			"  HELP SET PATHSAMPLES", "  Set the minimum recent reports for path ratings.",
			"  HELP SHOW PROP", "  View the path outlook to a station or place.",
			"  HELP DIALECT", "  View or change your command style.",
			"", "Reading settings pauses live spots. Use RESUME when ready.",
		}},
		{"FILTER EXAMPLES", "Try a complete filter setup.", filterExampleHelpLines(dialect)},
		{"FILTERS", "Learn how to change which spots you receive.", filterGuideHelpLines(dialect)},
		{"PRESETS", "Save or load your settings.", presetGuideHelpLines()},
		{"TROUBLE", "Find help when something goes wrong.", []string{
			"Live spots stopped? Type SHOW HOLD to check. Type RESUME to restart.",
			"Missed spots are not replayed.",
			"Too few or too many spots? Check " + readback + " and SHOW DEDUPE.",
			"No path ratings? Check your grid in SHOW SETTINGS. See HELP SET GRID.",
			"Command rejected? Type HELP before the command you tried.",
			"Reporting a problem? Include the exact error and the output of SHOW BUILD.",
		}},
	}
	for _, page := range pages {
		catalog.entries[page.topic] = helpEntry{summary: page.topic + " - " + page.summary, lines: page.lines}
	}
}

func presetGuideHelpLines() []string {
	return []string{
		"A preset saves your active GO or CC filters and shared settings.",
		"  LIST PRESET        List your saved presets.",
		"When LIST PRESET pauses live spots, type RESUME to restart.",
		"  SAVE PRESET DAY    Save your current setup. Replaces DAY if it exists.",
		"  LOAD PRESET DAY    Apply DAY and save it for future logins.",
		"  DELETE PRESET DAY  Remove DAY without changing your active settings.",
		"Loading a preset switches to its dialect and replaces that dialect's filters",
		"and your shared settings. The other dialect's filters stay unchanged.",
		"Your callsign's numeric SSIDs share the same presets.",
	}
}

func archiveHelpExamples(show string) []string {
	return []string{
		"Example: " + show + " 10 - Show up to 10 recent spots that match your filters.",
		fmt.Sprintf("  %-29sSearch for a callsign.", show+" K1ABC 10"),
		fmt.Sprintf("  %-29sSearch for CW spots on 20m.", show+" 10 BAND 20 MODE CW"),
		fmt.Sprintf("  %-29sFind spots with CQ in the comment.", show+" 10 COMMENT CQ"),
		"These searches don't change your saved filters.",
		"",
	}
}

// commandIndexHelpLines uses every installed entry, including narrow topics and
// machine commands omitted from the landing page. Sorting makes output stable.
// Aliases remain in command-specific help; this index creates no executable aliases.
func commandIndexHelpLines(catalog helpCatalog) []string {
	special := map[string]string{
		"COMMANDS": "List all commands and help topics.",
		"SYMBOLS":  "Explain symbols on spot lines.",
	}
	topics := make([]string, 0, len(catalog.entries)+len(special))
	for topic := range catalog.entries {
		topics = append(topics, topic)
	}
	for topic := range special {
		topics = append(topics, topic)
	}
	slices.Sort(topics)
	lines := []string{
		"All commands and help topics",
		"Type HELP before any name below for details. Example: HELP SHOW SETTINGS.",
		"Some commands are only available in GO or CC.",
		"",
	}
	lines = slices.Grow(lines, len(topics))
	for _, topic := range topics {
		description := special[topic]
		if description == "" {
			description = strings.TrimPrefix(catalog.entries[topic].summary, topic+" - ")
		}
		lines = append(lines, helpRowLines(helpRow{topic, description})...)
	}
	return lines
}

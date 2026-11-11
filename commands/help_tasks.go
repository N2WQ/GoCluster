// Task HELP pages teach existing commands without executing them. All content
// and indexes are request-owned and bounded by the finite compiled HELP catalog.
package commands

import (
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
			"  " + show + " 10", "  Show 10 recent spots that match your filters.",
			"", "Find a call, band or mode:",
			"  " + show + " K1ABC 10", "  " + show + " 10 BAND 20", "  " + show + " 10 MODE CW",
			"  These searches do not change your saved filters.",
			"", "Report a station:", "  DX 14025.0 K1ABC CQ",
			"  This posts a real spot. Use a station you heard.",
			"  Enter the frequency in kHz.",
			"  A queued spot still needs processing before delivery.",
			"", "More help:", "  HELP " + show, "  HELP DX", "  HELP SHOW DXCC", "  HELP WHOSPOTSME",
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
		{"PRESETS", "Save or load your settings.", []string{
			"Save or load your settings:",
			"  SAVE PRESET CONTEST", "  Save your current filters and settings as CONTEST.",
			"  Saving with an existing name replaces that saved setup.",
			"", "List saved setups:", "  LIST PRESET",
			"", "Load a saved setup:", "  LOAD PRESET CONTEST",
			"  Loading replaces your filters and shared settings with the saved setup.",
			"  It selects the saved GO or CC style and saves this login's defaults.",
			"  Your filters for the other style stay unchanged.",
			"", "Delete a saved setup:", "  DELETE PRESET CONTEST",
			"  This removes the saved setup. Your current settings stay unchanged.",
			"", "More help:", "  HELP SAVE PRESET", "  HELP LOAD PRESET", "  HELP DELETE PRESET",
		}},
		{"TROUBLE", "Find help when something goes wrong.", []string{
			"Find help when something goes wrong:",
			"  SHOW HOLD", "  Check whether live spots are paused.",
			"  RESUME", "  End the pause when you are ready.",
			"  Spots missed during a pause are not replayed.",
			"", "Too few or too many spots:", "  " + readback,
			"  Inspect your filters before changing them.",
			"  Showing these settings pauses live spots. Use RESUME when ready.",
			"  HELP FILTERS", "  Check which spots your filters allow or block.",
			"  HELP SHOW DEDUPE", "  Check how repeated spots are hidden.",
			"", "Missing path ratings:", "  SHOW SETTINGS",
			"  Check your grid square and settings.",
			"  Showing these settings pauses live spots. Use RESUME when ready.",
			"  HELP SET GRID", "  HELP SYMBOLS",
			"", "Command rejected:", "  HELP " + show,
			"  Type HELP before the command you tried.",
			"  HELP DIALECT", "  Check which command style you use.",
			"", "Server details:", "  SHOW BUILD",
			"  Keep the exact error and server version when asking for help.",
		}},
	}
	for _, page := range pages {
		catalog.entries[page.topic] = helpEntry{summary: page.topic + " - " + page.summary, lines: page.lines}
	}
}

// commandIndexHelpLines uses every installed entry, including narrow topics and
// machine commands omitted from the landing page. Sorting makes output stable.
// Aliases remain in command-specific help; this index creates no executable aliases.
func commandIndexHelpLines(catalog helpCatalog) []string {
	topics := make([]string, 0, len(catalog.entries))
	for topic := range catalog.entries {
		topics = append(topics, topic)
	}
	slices.Sort(topics)
	lines := make([]string, 0, 11+2*len(topics))
	lines = append(lines, "All commands and help topics:",
		"  Type HELP before a name below for details.",
		"  Some topics explain commands available only in the other command style.",
		"", "Help topics:",
		"  COMMANDS - See all commands and help topics.",
		"  FILTERS - Choose which spots you receive.",
		"  SYMBOLS - Read call confidence and path reliability.",
		"", "Commands and guides:")
	for _, topic := range topics {
		description := strings.TrimPrefix(catalog.entries[topic].summary, topic+" - ")
		lines = append(lines, helpRowLines(helpRow{topic, description})...)
	}
	return lines
}

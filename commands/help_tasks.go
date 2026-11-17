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
		{"PRESETS", "Save or load your settings.", presetGuideHelpLines()},
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

func presetGuideHelpLines() []string {
	return []string{
		"Save your current setup:",
		"  Saving an existing name replaces that saved preset.",
		"  SAVE PRESET DAY",
		"", "Create another setup:",
		"  Use HELP FILTER EXAMPLES to choose different filters.",
		"  Then save that setup with another name:",
		"  SAVE PRESET NIGHT",
		"", "See your saved setups:", "  LIST PRESET",
		"", "Switch between setups:",
		"  Loading replaces the saved style's filters and your shared",
		"  settings. It also switches to that GO or CC command style.",
		"  Your filters for the other style stay unchanged.",
		"  The loaded settings are saved for future logins using",
		"  your current callsign and SSID.",
		"", "  Load the setup you want:",
		"  LOAD PRESET DAY", "    Use your DAY setup.",
		"  LOAD PRESET NIGHT", "    Use your NIGHT setup.",
		"", "Delete a saved setup:",
		"  This deletes the preset. Your current settings stay unchanged.",
		"  DELETE PRESET NIGHT",
		"", "A preset saves your current GO or CC filters and settings,",
		"including your grid square and duplicate-spot settings.",
		"Presets are shared across your callsign's numeric SSIDs.",
		"For example, K1ABC-1 and K1ABC-2 can load the same presets.",
		"", "More help:",
		"  HELP SAVE PRESET", "  HELP LOAD PRESET", "  HELP DELETE PRESET",
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

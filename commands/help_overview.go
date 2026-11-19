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

func helpOverviewLines(dialect string) []string {
	show, readback := helpTaskCommands(dialect)
	return []string{
		"Getting help",
		"  HELP COMMANDS    List all commands.",
		"  HELP <command>   Get help with one command.",
		"  HELP SYMBOLS     Explain symbols on spot lines.",
		"  HELP TROUBLE     Get help when something isn't working.",
		"", "Showing archive spots",
		fmt.Sprintf("  %-17sShow up to 10 recent spots that match your filters.", show+" 10"),
		fmt.Sprintf("  %-17sLearn to search by call, band, mode, or comment.", "HELP "+show),
		"", "Managing filters",
		fmt.Sprintf("  %-17sSee your active filters.", readback),
		"  HELP FILTERS     Learn how to change which spots you receive.",
		"  SHOW SETTINGS    See your settings.",
		"", "Managing presets",
		"  LIST PRESET      List your saved presets.",
		"  SAVE PRESET DAY  Save your current setup as DAY.",
		"  LOAD PRESET DAY  Switch to your saved DAY setup.",
		"  HELP PRESETS     Learn how to manage saved setups.",
	}
}

func symbolHelpLines(path PathGlyphHelpConfig) []string {
	lines := []string{
		"Call confidence",
		"  ?  One reporter; no added support for the call.",
		"  S  One reporter; stored or recent reports support the call.",
		"  P  Several reporters support the call.",
		"  V  Several reporters provide stronger support.",
		"  C  The cluster corrected the call.",
		"  B  A correction failed checks; the original call remains.",
	}
	lines = append(lines, briefPathHelpLines(path)...)
	lines = append(lines, "")
	lines = append(lines, confidenceHelpLines()...)
	return append(lines, pathGlyphHelpLines(path)...)
}

func briefPathHelpLines(cfg PathGlyphHelpConfig) []string {
	if !pathHelpAvailable(cfg) {
		return nil
	}
	lines := []string{"", "Path reliability"}
	// Wrap configured symbols rather than assuming the shipped ASCII mapping.
	row := "  " + cfg.High + " Favorable   " + cfg.Medium + " Workable   " + cfg.Low + " Weak   " + cfg.Unlikely + " Poor   " + cfg.Closed + " Closed"
	if len(row) <= helpMaxWidth {
		lines = append(lines, row)
	} else {
		lines = append(lines, wrapTextLines(row, "  ")...)
	}
	if strings.TrimSpace(cfg.Insufficient) == "" {
		return append(lines, "  A blank means no path rating is shown.")
	}
	return append(lines, wrapTextLines("  "+quoteHelpGlyph(cfg.Insufficient)+" means not enough recent reports.", "  ")...)
}

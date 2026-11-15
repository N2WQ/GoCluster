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

// helpOverviewLines renders the reviewed landing page. Symbols come from the same
// startup snapshot as HELP SYMBOLS; no configuration or model state is changed.
func helpOverviewLines(dialect string, path PathGlyphHelpConfig) []string {
	show, readback := helpTaskCommands(dialect)
	lines := []string{"Try these commands:"}
	lines = append(lines,
		"  "+show+" 10"+strings.Repeat(" ", 17-len(show+" 10"))+"Show 10 recent spots that match your filters.",
		"  "+readback+strings.Repeat(" ", 17-len(readback))+"Show your current filters.",
		"  SHOW SETTINGS    Show your settings.",
		"", "Call confidence:",
		"  ?  One reporter; no added support for the call.",
		"  S  One reporter; stored or recent reports support the call.",
		"  P  Several reporters support the call.",
		"  V  Several reporters provide stronger support.",
		"  C  The cluster corrected the call.",
		"  B  A correction failed checks; the original call remains.")
	lines = append(lines, briefPathHelpLines(path)...)
	return append(lines,
		"", "What would you like to do?",
		"  HELP SPOTS       Find spots or report a station.",
		"  HELP FILTERS     Choose which spots you receive.",
		"  HELP SETTINGS    Change your location and settings.",
		"  HELP PRESETS     Save or load your settings.",
		"  HELP SYMBOLS     Learn more about call confidence and path reliability.",
		"  HELP TROUBLE     Find help when something goes wrong.",
		"  HELP COMMANDS    See all commands.",
		"", "For help with one command, type HELP before its name:",
		"  HELP "+show)
}

func briefPathHelpLines(cfg PathGlyphHelpConfig) []string {
	if !pathHelpAvailable(cfg) {
		return nil
	}
	lines := []string{"", "Path reliability:"}
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

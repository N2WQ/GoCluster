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

func filterReferenceHelpLines(dialect string) []string {
	if dialect == "cc" {
		return nativeCCFilterReferenceLines()
	}
	lines := []string{"Filter reference:", "  HELP RULE is the canonical Boolean grammar, field and value reference."}
	lines = append(lines, filterBasicsLines()...)
	lines = append(lines, filterRecipeLines(dialect)...)
	lines = append(lines, featureOverviewLines(dialect)...)
	return append(lines, "", "  HELP SYMBOLS explains confidence and path reliability symbols.",
		"  Old category edits and CC filter/mode shortcuts are not translated.")
}

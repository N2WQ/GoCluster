// Band HELP discovers the shared physical and meta catalogs without changing
// selections. Numeric limits remain owned by spot, not this presentation layer.
package commands

import (
	"fmt"

	"dxcluster/spot"
)

func installBandHelp(catalog *helpCatalog) {
	catalog.entries["BANDS"] = helpEntry{
		summary: "BANDS - Choose individual bands or fixed groups of bands.",
		lines:   bandHelpLines(),
	}
	for _, topic := range []string{"SHOW DX", "SHOW MYDX", "SHOW/DX", "SH/DX"} {
		if entry, ok := catalog.entries[topic]; ok {
			entry.lines = append(entry.lines, "", "See HELP BANDS for individual bands and fixed band groups.")
			catalog.entries[topic] = entry
		}
	}
}

func bandHelpLines() []string {
	lines := []string{"BANDS - Choose individual bands or fixed groups of bands.", ""}
	lines = appendListSection(lines, "Individual bands:", spot.SupportedBandNames())
	lines = append(lines, "", "Meta bands:")
	for _, group := range spot.SupportedMetaBands() {
		label := fmt.Sprintf("  %-10s", group.Name)
		lines = append(lines, wrapLabelList(label, group.Members, helpMaxWidth)...)
	}
	lines = append(lines,
		"", "A meta name selects its member bands.",
		"Names ignore letter case.",
		"Mix names and bands in GO/history: WARC,6m.",
		"For native CC, use its individual selectors: WARC,6.",
		"Overlapping selections do not add anything twice.",
		"", "HF includes 160m and 60m.",
		"HFCONTEST selects bands, not contest activity.",
		"VHFUP selects supported bands, not every frequency above HF.")
	if segments, ok := spot.BandFrequencySegments("4m"); ok {
		for _, segment := range segments {
			lines = append(lines, fmt.Sprintf("4m covers %.3f-%.3f MHz.", segment.Min/1000, segment.Max/1000))
		}
	}
	return append(lines,
		"", "GO example:",
		"  This replaces rule 0, including its previous conditions.",
		"  Keep any other conditions you still want.",
		"  PASS RULE 0 BAND=WARC AND MODE=CW",
		"  Other PASS rules can allow spots on other bands.",
		"", "CC example - replace the DXBM selection with WARC:",
		"  SET/FILTER DXBM/OFF", "  SET/FILTER DXBM/PASS WARC",
		"  Mode, source and geography selections still apply.",
		"", "Search history without changing your filters:",
		"  GO: SHOW DX 20 BAND WARC", "  CC: SHOW/DX 20 BAND WARC",
		"", "More help:", "  HELP FILTERS", "  HELP RULE", "  HELP SET/FILTER")
}

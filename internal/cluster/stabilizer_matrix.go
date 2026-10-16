package cluster

import (
	"fmt"
	"math"
	"strings"

	"dxcluster/stats"
)

// formatStabilizerMatrix projects a coherent tracker snapshot into the console
// stats cycle. Empty cells mean no observations, rather than zero-turn paths.
// Headless final-glyph diagnostics retain their separate historical definition.
func formatStabilizerMatrix(tracker *stats.Tracker) []string {
	matrix := tracker.StabilizerTransitionStats()
	lines := make([]string, 0, 5)
	lines = append(lines,
		"Stabilizer: average turns per call (entered → ended)",
		fmt.Sprintf("%-20s%6s%6s%6s%6s%6s%6s", "Entered ↓ / Ended →", "?", "S", "P", "V", "C", "B"),
	)
	for row, glyph := range []string{"?", "S", "P"} {
		var b strings.Builder
		fmt.Fprintf(&b, "%-20s", glyph)
		for _, cell := range matrix[row] {
			value := "—"
			if cell.Samples > 0 {
				value = fmt.Sprintf("%.1f", math.Round(cell.AverageTurns*10)/10)
			}
			fmt.Fprintf(&b, "%6s", value)
		}
		lines = append(lines, b.String())
	}
	return lines
}

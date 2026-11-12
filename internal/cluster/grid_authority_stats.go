// File role: Formats bounded grid-work diagnostics for headless and cache panes.
// Related decision: docs/decisions/ADR-0345-grid-active-work-authority.md.
package cluster

import "fmt"

// Four fixed rows fit the 56-column interior of a 60-column cache pane even at
// MaxUint64. Gauges describe owned coordination, never total process memory.
func formatGridAuthorityLines(metrics *gridMetrics) []string {
	labels := []string{"active keys", "copied bytes", "pending reports", "superseded"}
	values := []uint64{0, 0, 0, 0}
	if metrics != nil {
		values[0] = metrics.activeKeys.Load()
		values[1] = metrics.copiedTextBytes.Load()
		values[2] = metrics.pendingReports.Load()
		values[3] = metrics.supersededCandidates.Load()
	}
	lines := make([]string, len(labels))
	for i, label := range labels {
		if metrics == nil {
			lines[i] = "Grid work: " + label + "=--"
		} else {
			lines[i] = fmt.Sprintf("Grid work: %s=%d", label, values[i])
		}
	}
	return lines
}

//go:build dashboard_integration

package ui

import (
	"strings"

	"dxcluster/config"

	"github.com/gdamore/tcell/v2"
)

// IntegrationDashboard uses an injected simulation screen only in the
// explicitly tagged dashboard integration test build.
func IntegrationDashboard(cfg config.UIConfig, screen tcell.Screen) *DashboardV2 {
	return newDashboardV2(cfg, true, screen)
}

// IntegrationDashboardScreen reads screen cells on the application's event
// loop, so tview cannot redraw concurrently while the test inspects them.
func IntegrationDashboardScreen(d *DashboardV2, screen tcell.SimulationScreen) string {
	var text string
	d.app.QueueUpdate(func() {
		cells, width, _ := screen.GetContents()
		var b strings.Builder
		for i, cell := range cells {
			if i > 0 && i%width == 0 {
				b.WriteByte('\n')
			}
			if len(cell.Runes) == 0 {
				b.WriteByte(' ')
			} else {
				b.WriteString(string(cell.Runes))
			}
		}
		text = b.String()
	})
	return text
}

// IntegrationDashboardEvents observes the actual dashboard's protected rows.
func IntegrationDashboardEvents(d *DashboardV2) ([]string, uint64) {
	v := d.eventsStream.view
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.allRowsLocked(), v.truncated
}

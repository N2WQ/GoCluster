package ui

import (
	"strconv"
	"strings"
	"testing"

	"github.com/rivo/tview"
)

func TestGridAuthorityCachePane(t *testing.T) {
	rows := []string{"Grid work: active keys=18446744073709551615", "Grid work: copied bytes=18446744073709551615", "Grid work: pending reports=18446744073709551615", "Grid work: superseded=18446744073709551615"}
	for _, width := range []int{60, 120} {
		t.Run(strconv.Itoa(width), func(t *testing.T) {
			pane := newBoxedTextView("Caches & Data Freshness")
			pane.SetText(strings.Join(rows, "\n"))
			pane.SetRect(0, 0, width, 13)
			screen := newSimulationScreen(t, width, 13)
			pane.Draw(screen)
			screen.Show()
			text := simulationScreenText(screen)
			for _, row := range rows {
				if !strings.Contains(text, row) {
					t.Fatalf("cache diagnostic clipped at width %d: %q", width, row)
				}
			}
			if overviewCachesDefaultHeight != 13 {
				t.Fatal("four new placeholder rows lack default pane space")
			}
			for _, label := range []string{"active keys", "copied bytes", "pending reports", "superseded"} {
				if !strings.Contains(tview.Escape(placeholderCaches), label+"=--") {
					t.Fatalf("placeholder missing %s", label)
				}
			}
		})
	}
}

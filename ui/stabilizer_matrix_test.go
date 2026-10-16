package ui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func TestStabilizerMatrixRefreshAndPaneLayout(t *testing.T) {
	d := &DashboardV2{
		overviewPipeline: newBoxedTextView("Pipeline Quality"),
		pipelineQuality:  newBoxedTextView("Pipeline Quality"),
		eventsPipeline:   newBoxedTextView("Pipeline Quality"),
		scheduler:        newFrameScheduler(nil, 60, 50*time.Millisecond, nil),
	}
	d.snapshotFrameFn = func() {}
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	defer screen.Fini()
	screen.SetSize(60, 11)
	for _, average := range []string{"2.3", "8.4"} {
		lines := []string{
			"Cluster: test", "MEMORY / GC", "Heap: ok", "INGEST RATES (per min)", "RBN: ok",
			"PIPELINE QUALITY", "Primary Dedupe: ok", "Corrections: ok", "Flood: ok", "",
			"Stabilizer: average turns per call (entered → ended)",
			fmt.Sprintf("%-20s%6s%6s%6s%6s%6s%6s", "Entered ↓ / Ended →", "?", "S", "P", "V", "C", "B"),
			fmt.Sprintf("%-20s%6s%6s%6s%6s%6s%6s", "?", "—", "—", "—", "—", "—", "—"),
			fmt.Sprintf("%-20s%6s%6s%6s%6s%6s%6s", "S", "—", "—", "—", average, "—", "—"),
			fmt.Sprintf("%-20s%6s%6s%6s%6s%6s%6s", "P", "—", "—", "—", "—", "—", "—"),
			"CACHES & DATA FRESHNESS", "Grid: ok", "PATH PREDICTIONS", "Path: ok", "NETWORK", "Telnet: ok",
		}
		d.SetSnapshot(Snapshot{GeneratedAt: time.Now(), OverviewLines: lines})
		snap := d.overviewSnapshot()
		if snap.PipelineHeight != 11 || strings.Contains(snap.Pipeline, "Stabilizer Glyph") {
			t.Fatalf("wrong matrix pane size/content: %+v", snap)
		}
		d.updateOverviewSnapshot(snap)
		d.updatePipelineSnapshot(snap)
		d.updateEventsOverviewSnapshot(snap)
		for _, pane := range []*tview.TextView{d.overviewPipeline, d.pipelineQuality, d.eventsPipeline} {
			if got := pane.GetText(true); !strings.Contains(got, "Stabilizer: average turns per call") || !strings.Contains(got, average) {
				t.Fatalf("pane did not refresh matrix: %q", got)
			}
			pane.SetRect(0, 0, 60, 11)
			screen.Clear()
			pane.Draw(screen)
			screen.Show()
			var rendered strings.Builder
			for y := 0; y < 11; y++ {
				for x := 0; x < 60; x++ {
					ch, _, _, _ := screen.GetContent(x, y)
					rendered.WriteRune(ch)
				}
				rendered.WriteByte('\n')
			}
			if text := rendered.String(); !strings.Contains(text, average) || !strings.Contains(text, "entered → ended") || strings.Count(text, "—") != 17 {
				t.Fatalf("matrix clipped in 60x11 pane:\n%s", text)
			}
		}
	}
	if isRemovedOverviewPipelineLine("Stabilizer: average turns per call (entered → ended)") || !isRemovedOverviewPipelineLine("Stabilizer: held=1") {
		t.Fatal("matrix exception reintroduced the retired diagnostic row")
	}
}

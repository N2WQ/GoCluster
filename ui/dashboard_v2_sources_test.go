package ui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func TestDashboardSnapshotBoundsIngestSourcesHeight(t *testing.T) {
	tests := []struct {
		name       string
		sourceRows int
		wantHeight int
	}{
		{name: "minimum", sourceRows: 1, wantHeight: overviewSourcesMinHeight},
		{name: "default", sourceRows: 3, wantHeight: overviewSourcesDefaultHeight},
		{name: "maximum", sourceRows: 8, wantHeight: overviewSourcesMaxHeight},
		{name: "maximum with sixty-four rows", sourceRows: 64, wantHeight: overviewSourcesMaxHeight},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sources := ingestSourceRows(tt.sourceRows, "connected")
			snap := buildDashboardSnapshot(time.Time{}, overviewLinesWithSources(sources))

			if got := snap.SourcesHeight; got != tt.wantHeight {
				t.Fatalf("expected source pane height %d, got %d", tt.wantHeight, got)
			}
			for _, row := range sources {
				if !strings.Contains(snap.Sources, row) {
					t.Fatalf("expected complete source content to include %q", row)
				}
			}
		})
	}
}

func TestOverviewIngestSourcesScrollToLastRowAndPreserveFocusOnRefresh(t *testing.T) {
	sources := newBoxedTextView("Ingest Sources")
	sources.SetScrollable(true)
	d := &DashboardV2{
		overviewSources:       sources,
		overviewSourcesHeight: overviewSourcesDefaultHeight,
	}
	d.overviewRoot = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(sources, overviewSourcesDefaultHeight, 0, false)
	d.overviewGroup = newFocusGroup(newFocusBox(sources, "Ingest Sources", true))
	d.app = tview.NewApplication()

	rows := ingestSourceRows(64, "connected")
	d.updateOverviewSnapshot(buildDashboardSnapshot(time.Time{}, overviewLinesWithSources(rows)))
	d.overviewGroup.set(d.app, 0)
	if got := d.app.GetFocus(); got != sources || !sources.HasFocus() {
		t.Fatalf("expected ingest sources to own focus, got %T", got)
	}

	screen := newSimulationScreen(t, 64, overviewSourcesMaxHeight)
	d.overviewRoot.SetRect(0, 0, 64, overviewSourcesMaxHeight)
	d.overviewRoot.Draw(screen)
	if handled := d.overviewGroup.handleScroll(d.app, tcell.NewEventKey(tcell.KeyEnd, 0, tcell.ModNone)); !handled {
		t.Fatal("expected focused ingest sources to handle End")
	}
	d.overviewRoot.Draw(screen)
	screen.Show()
	if got := simulationScreenText(screen); !strings.Contains(got, rows[len(rows)-1]) {
		t.Fatalf("expected last source row to be scroll-discoverable, screen=%q", got)
	}
	rowBefore, colBefore := sources.GetScrollOffset()
	if rowBefore <= 0 {
		t.Fatalf("expected a nonzero scroll offset after End, got %d", rowBefore)
	}

	refreshed := ingestSourceRows(64, "retrying")
	d.updateOverviewSnapshot(buildDashboardSnapshot(time.Time{}, overviewLinesWithSources(refreshed)))
	rowAfter, colAfter := sources.GetScrollOffset()
	if rowAfter != rowBefore || colAfter != colBefore {
		t.Fatalf("expected refresh to preserve scroll offset (%d,%d), got (%d,%d)", rowBefore, colBefore, rowAfter, colAfter)
	}
	if got := d.app.GetFocus(); got != sources || !sources.HasFocus() {
		t.Fatalf("expected refresh to preserve ingest source focus, got %T", got)
	}
	d.overviewRoot.Draw(screen)
	screen.Show()
	if got := simulationScreenText(screen); !strings.Contains(got, refreshed[len(refreshed)-1]) {
		t.Fatalf("expected refreshed last source row to remain visible, screen=%q", got)
	}
}

func TestOverviewIngestSourcesHeightCapPreservesSurroundingSections(t *testing.T) {
	d := newOverviewLayoutForTest()
	d.updateOverviewSnapshot(buildDashboardSnapshot(time.Time{}, overviewLinesWithSources(ingestSourceRows(64, "connected"))))

	screen := newSimulationScreen(t, 120, 70)
	d.overviewRoot.SetRect(0, 0, 120, 70)
	d.overviewRoot.Draw(screen)

	if _, _, _, got := d.overviewSources.GetRect(); got != overviewSourcesMaxHeight {
		t.Fatalf("expected source pane height %d, got %d", overviewSourcesMaxHeight, got)
	}
	for name, pane := range map[string]*tview.TextView{
		"header":   d.overviewHdr,
		"memory":   d.overviewMem,
		"ingest":   d.overviewIngest,
		"pipeline": d.overviewPipeline,
		"caches":   d.overviewCaches,
		"path":     d.overviewPath,
		"network":  d.overviewNetwork,
	} {
		if _, _, _, height := pane.GetRect(); height <= 0 {
			t.Fatalf("expected surrounding %s pane to retain visible layout space", name)
		}
	}
}

func overviewLinesWithSources(sources []string) []string {
	lines := []string{
		"Cluster: test  Version: 1  Uptime: 00:01",
		"MEMORY / GC",
		"Heap: 1 MiB",
		"INGEST RATES (per min)",
		"RBN: 1",
		"PIPELINE QUALITY",
		"Primary: ok",
		"CACHES & DATA FRESHNESS",
		"Grid: ok",
		"PATH PREDICTIONS",
		"Path: ok",
		"INGEST SOURCES",
	}
	lines = append(lines, sources...)
	return append(lines, "NETWORK", "Telnet: ok")
}

func ingestSourceRows(count int, state string) []string {
	rows := make([]string, count)
	for i := range rows {
		rows[i] = fmt.Sprintf("HUMAN/upstream-%02d: %s", i+1, state)
	}
	return rows
}

func newOverviewLayoutForTest() *DashboardV2 {
	d := &DashboardV2{
		overviewHdr:            newBoxedTextView("Overview"),
		overviewMem:            newBoxedTextView("Memory / GC"),
		overviewIngest:         newBoxedTextView("Ingest Rates (per min)"),
		overviewPipeline:       newBoxedTextView("Pipeline Quality"),
		overviewCaches:         newBoxedTextView("Caches & Data Freshness"),
		overviewPath:           newBoxedTextView("Path Predictions"),
		overviewSources:        newBoxedTextView("Ingest Sources"),
		overviewNetwork:        newBoxedTextView("Network"),
		overviewPipelineHeight: overviewPipelineDefaultHeight,
		overviewCachesHeight:   overviewCachesDefaultHeight,
		overviewPathHeight:     overviewPathMinHeight,
		overviewSourcesHeight:  overviewSourcesDefaultHeight,
	}
	d.overviewSources.SetScrollable(true)
	d.overviewRoot = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(d.overviewHdr, 3, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.overviewMem, 3, 0, false).
		AddItem(newSpacer(), 1, 0, false)
	addOverviewTopSections(d.overviewRoot, d.overviewIngest)
	d.overviewRoot.
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.overviewPipeline, overviewPipelineDefaultHeight, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.overviewCaches, overviewCachesDefaultHeight, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.overviewPath, overviewPathMinHeight, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.overviewSources, overviewSourcesDefaultHeight, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.overviewNetwork, 0, 1, false)
	return d
}

func newSimulationScreen(t *testing.T, width, height int) tcell.SimulationScreen {
	t.Helper()
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatalf("initialize simulation screen: %v", err)
	}
	screen.SetSize(width, height)
	t.Cleanup(screen.Fini)
	return screen
}

func simulationScreenText(screen tcell.SimulationScreen) string {
	cells, width, height := screen.GetContents()
	var text strings.Builder
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			cell := cells[y*width+x]
			if len(cell.Runes) == 0 {
				text.WriteByte(' ')
				continue
			}
			text.WriteRune(cell.Runes[0])
		}
		text.WriteByte('\n')
	}
	return text.String()
}

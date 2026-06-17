package ui

import (
	"strconv"
	"testing"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func BenchmarkVirtualLogViewDraw(b *testing.B) {
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		b.Fatalf("init simulation screen: %v", err)
	}
	defer screen.Fini()

	view := newVirtualLogView("Events", 256, false)
	view.SetRect(0, 0, 120, 20)
	for i := 0; i < 256; i++ {
		view.Append("seed line " + strconv.Itoa(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view.Append("event " + strconv.Itoa(i))
		view.Draw(screen)
	}
}

func BenchmarkVirtualLogViewDrawDynamicColors(b *testing.B) {
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		b.Fatalf("init simulation screen: %v", err)
	}
	defer screen.Fini()

	view := newVirtualLogView("Validation", 256, true)
	view.SetRect(0, 0, 120, 20)
	for i := 0; i < 256; i++ {
		view.Append("[yellow]seed[-] line " + strconv.Itoa(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view.Append("[red]event[-] " + strconv.Itoa(i))
		view.Draw(screen)
	}
}

func BenchmarkVirtualLogViewAppendLargeMessages(b *testing.B) {
	view := newVirtualLogView("Events", 256, false)
	line := "event " + strconv.Itoa(b.N) + " " + string(make([]byte, 4096))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view.Append(line)
	}
}

func BenchmarkDashboardV2SetSnapshotOverview(b *testing.B) {
	d := benchmarkDashboardV2()
	lines := benchmarkOverviewLines()
	d.activePage.Store("overview")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lines[0] = "Cluster: bench  Version: " + strconv.Itoa(i) + "  Uptime: 00:01"
		d.SetSnapshot(Snapshot{GeneratedAt: time.Unix(int64(i), 0), OverviewLines: lines})
		d.scheduler.flush()
	}
}

func BenchmarkDashboardV2UpdateOverviewBoxes(b *testing.B) {
	d := benchmarkDashboardV2()
	lines := benchmarkOverviewLines()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lines[0] = "Cluster: bench  Version: " + strconv.Itoa(i) + "  Uptime: 00:01"
		d.updateOverviewBoxes(lines)
	}
}

func BenchmarkFrameSchedulerFlush(b *testing.B) {
	f := newFrameScheduler(nil, 60, 50*time.Millisecond, nil)
	ids := []string{"snapshot", "network", "validation", "unlicensed", "corrected", "harmonics", "events"}
	callbacks := make([]func(), len(ids))
	for i := range callbacks {
		callbacks[i] = func() {}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j, id := range ids {
			f.Schedule(id, callbacks[j])
		}
		f.flush()
	}
}

func benchmarkDashboardV2() *DashboardV2 {
	overviewHdr := newBoxedTextView("Overview")
	overviewMem := newBoxedTextView("Memory / GC")
	overviewIngest := newBoxedTextView("Ingest Rates (per min)")
	overviewPipeline := newBoxedTextView("Pipeline Quality")
	overviewCaches := newBoxedTextView("Caches & Data Freshness")
	overviewPath := newBoxedTextView("Path Predictions")
	overviewSources := newBoxedTextView("Ingest Sources")
	overviewNetwork := newBoxedTextView("Network")
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(overviewHdr, 3, 0, false).
		AddItem(overviewMem, 3, 0, false).
		AddItem(overviewIngest, 6, 0, false).
		AddItem(overviewPipeline, overviewPipelineDefaultHeight, 0, false).
		AddItem(overviewCaches, overviewCachesDefaultHeight, 0, false).
		AddItem(overviewPath, overviewPathMinHeight, 0, false).
		AddItem(overviewSources, overviewSourcesDefaultHeight, 0, false).
		AddItem(overviewNetwork, 0, 1, false)
	d := &DashboardV2{
		scheduler:              newFrameScheduler(nil, 60, 50*time.Millisecond, nil),
		overviewRoot:           root,
		overviewHdr:            overviewHdr,
		overviewMem:            overviewMem,
		overviewIngest:         overviewIngest,
		overviewPipeline:       overviewPipeline,
		overviewCaches:         overviewCaches,
		overviewPath:           overviewPath,
		overviewSources:        overviewSources,
		overviewNetwork:        overviewNetwork,
		overviewPipelineHeight: overviewPipelineDefaultHeight,
		overviewCachesHeight:   overviewCachesDefaultHeight,
		overviewPathHeight:     overviewPathMinHeight,
		overviewSourcesHeight:  overviewSourcesDefaultHeight,
	}
	d.snapshotFrameFn = d.renderSnapshot
	return d
}

func benchmarkOverviewLines() []string {
	return []string{
		"Cluster: bench  Version: 1  Uptime: 00:01",
		"MEMORY / GC",
		"Heap: 32 MiB  Sys: 128 MiB  GC p99 (interval): 2ms  Last GC: 1m ago  Goroutines: 64",
		"INGEST RATES (per min)",
		"RBN: 1,000 | CW 200 | RTTY 50 | FT8 600 | FT4 100 | FT2 50",
		"PSK: 900 | CW 100 | RTTY 40 | FT8 500 | FT4 120 | FT2 30 | MSK 10 | PSK 90",
		"P92: 25",
		"Path: 100 (U) / 5 (S) / 2 (N) / 3 (G) / 1 (H) / 4 (B) / 6 (M)",
		"PIPELINE QUALITY",
		"Primary Dedupe: 5.0% | Secondary: F95 M92 S90",
		"Corrections: 1,234 | Unlicensed: 12 | Harmonics: 3 | Reputation: 4",
		"Flood: 2 (O) / 1 (S) / 0 (D) / 0 (X)",
		"",
		"Stabilizer Glyph: avg turns ? 1.00 | S 2.00 | P 3.00",
		"CACHES & DATA FRESHNESS",
		"Grid cache: [100%] 326,629 | Meta: [99%] 5,479",
		"Mode cache: DX hit 98.5% | Digital 100/128 | Mix E1 I2 RC3 RV4 RM5 RU6",
		"",
		"Custom SCP: 120 (R) / 80 (S)",
		"160m: 1 80m: 2 40m: 3 20m: 4 15m: 5",
		"",
		"CTY: 2026-06-04  FCC: 2026-06-04  Skew: 2026-06-04",
		"PATH PREDICTIONS",
		"H3 path pairs: 100 (L2) / 200 (L1)",
		"",
		"160m: 1 / 2   80m: 3 / 4   40m: 5 / 6   30m: 7 / 8",
		"20m: 9 / 10   17m: 11 / 12  15m: 13 / 14  12m: 15 / 16",
		"10m: 17 / 18  6m: 19 / 20",
		"INGEST SOURCES",
		"Ingest: 4 / 5 connected",
		"RBN RBN-FT PSKReporter DXSummit",
		"NETWORK",
		"Telnet: 20 clients   Drops: Q0 C1 W2   Prelogin: A0 G0 R0 C0 T0",
		"K1ABC N0CALL W1AW VE3XYZ JA1NUT",
	}
}

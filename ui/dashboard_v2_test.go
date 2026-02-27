package ui

import (
	"strings"
	"testing"
	"time"

	"github.com/rivo/tview"
)

func TestStreamPanelOverflow(t *testing.T) {
	panel := newStreamPanel("Corrected", 2, true)

	panel.Append("alpha")
	panel.Append("bravo")
	panel.Append("charlie")

	text := panel.SnapshotText()
	if !strings.Contains(text, "... +1 more") {
		t.Fatalf("expected overflow marker, got %q", text)
	}
}

func TestUpdateEventsOverviewBoxesMatchesOverviewSummary(t *testing.T) {
	lines := []string{
		"Cluster: test  Version: 1  Uptime: 00:01",
		"MEMORY / GC",
		"Heap: 1 MiB",
		"INGEST RATES (per min)",
		"RBN: 1",
		"PSK: 2",
		"P92: 3",
		"Path: 4",
		"PIPELINE QUALITY",
		"Primary: ok",
		"Corrections: ok",
		"Resolver: ok",
		"Resolver Pressur: ok",
		"Stabilizer: ok",
		"Stabilizer Reason: ok",
		"Temporal: ok",
		"CACHES & DATA FRESHNESS",
		"Grid: ok",
		"PATH PREDICTIONS",
		"160m: ok",
		"NETWORK",
		"Telnet: ok",
	}
	d := &DashboardV2{
		overviewHdr:      newBoxedTextView("Overview"),
		overviewMem:      newBoxedTextView("Memory / GC"),
		overviewIngest:   newBoxedTextView("Ingest Rates (per min)"),
		overviewPipeline: newBoxedTextView("Pipeline Quality"),
		eventsHdr:        newBoxedTextView("Overview"),
		eventsMem:        newBoxedTextView("Memory / GC"),
		eventsIngest:     newBoxedTextView("Ingest Rates (per min)"),
		eventsPipeline:   newBoxedTextView("Pipeline Quality"),
	}

	d.updateOverviewBoxes(lines)
	d.updateEventsOverviewBoxes(lines)

	if got, want := d.eventsHdr.GetText(true), d.overviewHdr.GetText(true); got != want {
		t.Fatalf("events header mismatch: got %q want %q", got, want)
	}
	if got, want := d.eventsMem.GetText(true), d.overviewMem.GetText(true); got != want {
		t.Fatalf("events memory mismatch: got %q want %q", got, want)
	}
	if got, want := d.eventsIngest.GetText(true), d.overviewIngest.GetText(true); got != want {
		t.Fatalf("events ingest mismatch: got %q want %q", got, want)
	}
	if got, want := d.eventsPipeline.GetText(true), d.overviewPipeline.GetText(true); got != want {
		t.Fatalf("events pipeline mismatch: got %q want %q", got, want)
	}
	if got := d.overviewPipeline.GetText(true); !strings.Contains(got, "Temporal: ok") {
		t.Fatalf("expected pipeline section to include temporal line, got %q", got)
	}
}

func TestOverviewPathPaneGrowsToFitBandBuckets(t *testing.T) {
	lines := []string{
		"Cluster: test  Version: 1  Uptime: 00:01",
		"MEMORY / GC",
		"Heap: 1 MiB",
		"INGEST RATES (per min)",
		"RBN: 1",
		"PSK: 2",
		"P92: 3",
		"Path: 4",
		"PIPELINE QUALITY",
		"Primary: ok",
		"Corrections: ok",
		"Resolver: ok",
		"Resolver Pressur: ok",
		"Stabilizer: ok",
		"Stabilizer Reason: ok",
		"Temporal: ok",
		"CACHES & DATA FRESHNESS",
		"Grid: ok",
		"PATH PREDICTIONS",
		"Path pairs: 100 / 200",
		"",
		"160m: 1 / 2   80m: 3 / 4   40m: 5 / 6   30m: 7 / 8",
		"20m: 9 / 10   17m: 11 / 12 15m: 13 / 14 12m: 15 / 16",
		"10m: 17 / 18  6m: 19 / 20",
		"NETWORK",
		"Telnet: ok",
	}

	path := newBoxedTextView("Path Predictions")
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(path, overviewPathMinHeight, 0, false)

	d := &DashboardV2{
		overviewRoot:       root,
		overviewPath:       path,
		overviewPathHeight: overviewPathMinHeight,
	}

	d.updateOverviewBoxes(lines)

	got := d.overviewPath.GetText(true)
	for _, want := range []string{
		"Path pairs: 100 / 200",
		"160m: 1 / 2",
		"20m: 9 / 10",
		"10m: 17 / 18",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected path details to include %q, got %q", want, got)
		}
	}
	if d.overviewPathHeight <= overviewPathMinHeight {
		t.Fatalf("expected path pane to grow beyond min height, got %d", d.overviewPathHeight)
	}
}

func TestOverviewCachesPaneResizesToContentHeight(t *testing.T) {
	lines := []string{
		"Cluster: test  Version: 1  Uptime: 00:01",
		"MEMORY / GC",
		"Heap: 1 MiB",
		"INGEST RATES (per min)",
		"RBN: 1",
		"PSK: 2",
		"P92: 3",
		"Path: 4",
		"PIPELINE QUALITY",
		"Primary: ok",
		"Corrections: ok",
		"Resolver: ok",
		"Resolver Pressur: ok",
		"Stabilizer: ok",
		"Stabilizer Reason: ok",
		"Temporal: ok",
		"CACHES & DATA FRESHNESS",
		"Grid: 1",
		"Meta: 2",
		"Known: 3",
		"",
		"CTY: now",
		"PATH PREDICTIONS",
		"Path pairs: 100 / 200",
		"NETWORK",
		"Telnet: ok",
	}

	caches := newBoxedTextView("Caches & Data Freshness")
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(caches, overviewCachesDefaultHeight, 0, false)

	d := &DashboardV2{
		overviewRoot:         root,
		overviewCaches:       caches,
		overviewCachesHeight: overviewCachesDefaultHeight,
	}

	d.updateOverviewBoxes(lines)

	got := d.overviewCaches.GetText(true)
	for _, want := range []string{"Grid: 1", "Meta: 2", "Known: 3", "CTY: now"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected caches details to include %q, got %q", want, got)
		}
	}
	if want := 7; d.overviewCachesHeight != want {
		t.Fatalf("expected caches pane height %d, got %d", want, d.overviewCachesHeight)
	}
}

func TestEventsPanelOnlyReceivesSystemStream(t *testing.T) {
	d := &DashboardV2{
		eventsStream:      newStreamPanel("Events", 10, false),
		ingestValidation:  newStreamPanel("Validation", 10, true),
		pipelineCorrected: newStreamPanel("Corrected", 10, true),
		ingestUnlicensed:  newStreamPanel("Unlicensed", 10, true),
		pipelineHarmonics: newStreamPanel("Harmonics", 10, true),
		scheduler:         newFrameScheduler(tview.NewApplication(), 60, 50*time.Millisecond, nil),
	}

	d.AppendDropped("CTY drop: A")
	d.AppendCall("corr B")
	d.AppendUnlicensed("unl C")
	d.AppendHarmonic("harm D")
	d.AppendReputation("rep E")
	d.AppendSystem("sys F")

	got := d.eventsStream.SnapshotText()
	if !strings.Contains(got, "sys F") {
		t.Fatalf("expected system line in events panel, got %q", got)
	}
	for _, forbidden := range []string{"CTY drop: A", "corr B", "unl C", "harm D", "rep E"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("unexpected non-system line in events panel: %q (panel=%q)", forbidden, got)
		}
	}
}

func TestRenderSnapshotUpdatesOnlyActivePage(t *testing.T) {
	lines := []string{
		"Cluster: test  Version: 1  Uptime: 00:01",
		"MEMORY / GC",
		"Heap: 1 MiB",
		"INGEST RATES (per min)",
		"RBN: 1",
		"PSK: 2",
		"P92: 3",
		"Path: 4",
		"PIPELINE QUALITY",
		"Primary: ok",
		"Corrections: ok",
		"Resolver: ok",
		"Resolver Pressur: ok",
		"Stabilizer: ok",
		"Stabilizer Reason: ok",
		"Temporal: ok",
		"CACHES & DATA FRESHNESS",
		"Grid: ok",
		"PATH PREDICTIONS",
		"160m: ok",
		"NETWORK",
		"Telnet: ok",
	}
	d := &DashboardV2{
		overviewHdr:      newBoxedTextView("Overview"),
		overviewIngest:   newBoxedTextView("Ingest Rates (per min)"),
		overviewPipeline: newBoxedTextView("Pipeline Quality"),
		overviewMem:      newBoxedTextView("Memory / GC"),
		overviewCaches:   newBoxedTextView("Caches & Data Freshness"),
		overviewPath:     newBoxedTextView("Path Predictions"),
		overviewNetwork:  newBoxedTextView("Network"),
		ingestHdr:        newBoxedTextView("Overview"),
		ingestIngest:     newBoxedTextView("Ingest Rates (per min)"),
	}
	d.snapshot.Store(cloneSnapshot(Snapshot{OverviewLines: lines}))
	d.activePage.Store("ingest")
	setBoxText(d.overviewHdr, "stale overview")

	d.renderSnapshot()

	if got := d.ingestHdr.GetText(true); !strings.Contains(got, "Cluster: test") {
		t.Fatalf("expected ingest page to refresh, got %q", got)
	}
	if got := d.overviewHdr.GetText(true); strings.Contains(got, "Cluster: test") {
		t.Fatalf("expected hidden overview page to remain cold, got %q", got)
	}
}

func TestAppendStreamBuffersWhileHiddenButDoesNotSchedule(t *testing.T) {
	d := &DashboardV2{
		eventsStream: newStreamPanel("Events", 10, false),
		scheduler:    newFrameScheduler(nil, 60, 50*time.Millisecond, nil),
	}
	d.eventsFrameFn = func() {}
	d.activePage.Store("pipeline")

	d.AppendSystem("sys hidden")

	if got := d.eventsStream.SnapshotText(); !strings.Contains(got, "sys hidden") {
		t.Fatalf("expected hidden page stream to buffer entries, got %q", got)
	}
	if batch := d.scheduler.collectBatch(); len(batch) != 0 {
		t.Fatalf("expected no scheduled draw for hidden page, got %d callbacks", len(batch))
	}

	d.activePage.Store("events")
	d.AppendSystem("sys visible")
	if batch := d.scheduler.collectBatch(); len(batch) != 1 {
		t.Fatalf("expected one scheduled draw for active page, got %d callbacks", len(batch))
	}
}

func TestNetworkUpdateDeferredUntilOverviewActive(t *testing.T) {
	d := &DashboardV2{
		overviewNetwork: newBoxedTextView("Network"),
		scheduler:       newFrameScheduler(nil, 60, 50*time.Millisecond, nil),
	}
	d.networkFrameFn = d.renderOverviewNetwork
	d.activePage.Store("pipeline")

	d.UpdateNetworkStatus("Telnet: 2 clients", []string{"A", "B"})
	if batch := d.scheduler.collectBatch(); len(batch) != 0 {
		t.Fatalf("expected no network draw while overview hidden, got %d callbacks", len(batch))
	}
	d.refreshVisiblePage("overview")
	if got := d.overviewNetwork.GetText(true); !strings.Contains(got, "Telnet: 2 clients") {
		t.Fatalf("expected deferred network state on overview activation, got %q", got)
	}
}

func TestEventsStreamDynamicColorsDisabled(t *testing.T) {
	panel := newStreamPanel("Events", 10, false)

	panel.Append("[yellow]tag[-] literal")

	got := panel.SnapshotText()
	if !strings.Contains(got, "[yellow]tag[-] literal") {
		t.Fatalf("expected literal bracket text in events stream, got %q", got)
	}
}

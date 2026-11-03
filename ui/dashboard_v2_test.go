package ui

import (
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/config"

	"github.com/rivo/tview"
)

func boundedSystemTestDashboard() *DashboardV2 {
	d := &DashboardV2{
		eventsStream: newStreamPanelWithOptions("Events", streamPanelOptions{
			MaxEvents: 32, MaxBytes: 1 << 20, MaxMessageBytes: 4096, EvictOnByteLimit: true,
		}, false),
		scheduler: newFrameScheduler(nil, 60, 50*time.Millisecond, nil),
	}
	d.activePage.Store("pipeline")
	return d
}

func TestBoundedSystemWriterConstruction(t *testing.T) {
	d := boundedSystemTestDashboard()
	discard := func(DashboardSystemDiscard) {}
	for _, limit := range []int{0, -1, dashboardV2MaxSystemRowBytes + 1} {
		if _, err := d.NewBoundedSystemWriter(limit, discard); err == nil {
			t.Fatalf("accepted invalid limit %d", limit)
		}
	}
	if _, err := d.NewBoundedSystemWriter(1, nil); err == nil {
		t.Fatal("accepted nil discard callback")
	}
	var nilDashboard *DashboardV2
	if _, err := nilDashboard.NewBoundedSystemWriter(1, discard); err == nil {
		t.Fatal("accepted nil dashboard")
	}
	for _, limit := range []int{1, dashboardV2MaxSystemRowBytes} {
		w, err := d.NewBoundedSystemWriter(limit, discard)
		if err != nil || cap(w.buf) != limit || len(w.buf) != 0 {
			t.Fatalf("limit %d: writer=%v err=%v", limit, w, err)
		}
		if err := w.Close(); err != nil || w.buf != nil {
			t.Fatalf("limit %d: close err=%v, buffer retained=%v", limit, err, w.buf != nil)
		}
	}
}

func TestBoundedSystemWriterSplitFramingAndOwnership(t *testing.T) {
	input := "alpha\r\r\n\nbeta\r\ngamma\npartial"
	for split := 0; split <= len(input); split++ {
		d := boundedSystemTestDashboard()
		var losses []DashboardSystemDiscard
		w, err := d.NewBoundedSystemWriter(64, func(kind DashboardSystemDiscard) { losses = append(losses, kind) })
		if err != nil {
			t.Fatal(err)
		}
		first := []byte(input[:split])
		if n, err := w.Write(first); err != nil || n != len(first) {
			t.Fatalf("split %d first write = %d, %v", split, n, err)
		}
		for i := range first {
			first[i] = 'X'
		}
		if n, err := w.Write([]byte(input[split:])); err != nil || n != len(input)-split {
			t.Fatalf("split %d second write = %d, %v", split, n, err)
		}
		if got, want := d.eventsStream.view.allRowsLocked(), []string{"alpha", "", "beta", "gamma"}; !slices.Equal(got, want) {
			t.Fatalf("split %d rows = %#v, want %#v", split, got, want)
		}
		if err := w.Close(); err != nil || !slices.Equal(losses, []DashboardSystemDiscard{DashboardSystemIncompleteRow}) {
			t.Fatalf("split %d close: losses=%v err=%v", split, losses, err)
		}
		if err := w.Close(); err != nil || len(losses) != 1 || w.buf != nil {
			t.Fatalf("split %d repeated close: losses=%v err=%v", split, losses, err)
		}
		if n, err := w.Write([]byte("late\n")); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("split %d closed write = %d, %v", split, n, err)
		}
	}
}

func TestBoundedSystemWriterOversizeAndRecovery(t *testing.T) {
	d := boundedSystemTestDashboard()
	var losses []DashboardSystemDiscard
	w, err := d.NewBoundedSystemWriter(4, func(kind DashboardSystemDiscard) { losses = append(losses, kind) })
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := w.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if len(w.buf) > 4 || cap(w.buf) != 4 {
			t.Fatalf("fragment %d: len=%d cap=%d", i, len(w.buf), cap(w.buf))
		}
	}
	if !slices.Equal(losses, []DashboardSystemDiscard{DashboardSystemOversizedRow}) {
		t.Fatalf("overflow losses = %v", losses)
	}
	_, _ = w.Write([]byte("\r\nok\n1234\n1234\r\nlast"))
	if got, want := d.eventsStream.view.allRowsLocked(), []string{"ok", "1234"}; !slices.Equal(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	wantLosses := []DashboardSystemDiscard{DashboardSystemOversizedRow, DashboardSystemOversizedRow, DashboardSystemIncompleteRow}
	if !slices.Equal(losses, wantLosses) {
		t.Fatalf("losses = %v, want %v", losses, wantLosses)
	}
}

func TestBoundedSystemWriterGapDoesNotSpliceOrDoubleCount(t *testing.T) {
	d := boundedSystemTestDashboard()
	var losses []DashboardSystemDiscard
	w, err := d.NewBoundedSystemWriter(4, func(kind DashboardSystemDiscard) { losses = append(losses, kind) })
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte("abc"))
	w.MarkGap() // A rejected "DEF" is counted by the caller.
	w.MarkGap()
	_, _ = w.Write([]byte("ghi\nok\n"))
	if got, want := d.eventsStream.view.allRowsLocked(), []string{"ok"}; !slices.Equal(got, want) {
		t.Fatalf("rows across gap = %#v, want %#v", got, want)
	}
	if !slices.Equal(losses, []DashboardSystemDiscard{DashboardSystemGapRowsUnknown}) {
		t.Fatalf("gap losses = %v", losses)
	}
	_, _ = w.Write([]byte("12345"))
	w.MarkGap() // The overflowing row was already counted as one known loss.
	_, _ = w.Write([]byte("tail\nend\n"))
	w.MarkGap()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	wantLosses := []DashboardSystemDiscard{DashboardSystemGapRowsUnknown, DashboardSystemOversizedRow, DashboardSystemGapRowsUnknown}
	if !slices.Equal(losses, wantLosses) {
		t.Fatalf("gap and overflow losses = %v, want %v", losses, wantLosses)
	}
	if got, want := d.eventsStream.view.allRowsLocked(), []string{"ok", "end"}; !slices.Equal(got, want) {
		t.Fatalf("recovered rows = %#v, want %#v", got, want)
	}
}

func TestBoundedSystemWriterUsesDashboardTruncation(t *testing.T) {
	d := boundedSystemTestDashboard()
	var losses []DashboardSystemDiscard
	w, err := d.NewBoundedSystemWriter(8192, func(kind DashboardSystemDiscard) { losses = append(losses, kind) })
	if err != nil {
		t.Fatal(err)
	}
	_, _ = w.Write([]byte(strings.Repeat("x", 4097) + "\n"))
	want := strings.Repeat("x", 4081) + "... [truncated]"
	if got := d.eventsStream.view.allRowsLocked(); !slices.Equal(got, []string{want}) {
		t.Fatal("dashboard did not retain its configured truncation")
	}
	if d.eventsStream.view.truncated != 1 || len(losses) != 0 {
		t.Fatalf("truncated=%d losses=%v", d.eventsStream.view.truncated, losses)
	}
	_ = w.Close()
}

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
		"Resolver Pressure: ok",
		"Stabilizer: ok",
		"Stabilizer Glyph: ok",
		"Temporal: ok",
		"FT Burst: ok",
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
	gotPipeline := d.overviewPipeline.GetText(true)
	if !strings.Contains(gotPipeline, "Stabilizer Glyph: ok") {
		t.Fatalf("expected pipeline section to keep stabilizer glyph line, got %q", gotPipeline)
	}
	for _, removed := range []string{"Resolver: ok", "Resolver Pressure: ok", "Stabilizer: ok", "Temporal: ok", "FT Burst: ok"} {
		if strings.Contains(gotPipeline, removed) {
			t.Fatalf("expected pipeline section to remove %q, got %q", removed, gotPipeline)
		}
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
		"Resolver Pressure: ok",
		"Stabilizer: ok",
		"Stabilizer Glyph: ok",
		"Temporal: ok",
		"CACHES & DATA FRESHNESS",
		"Grid: ok",
		"PATH PREDICTIONS",
		"VOACAP cache: 10 (C) / 2 (D) / 1 (I) / 0 (Q)",
		"H3 path pairs: 100 / 200",
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
		"VOACAP cache: 10 (C) / 2 (D) / 1 (I) / 0 (Q)",
		"H3 path pairs: 100 / 200",
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

func TestIngestPlaceholderIncludesFT2Counters(t *testing.T) {
	d := &DashboardV2{
		ingestHdr:        newBoxedTextView("Overview"),
		ingestIngest:     newBoxedTextView("Ingest Rates (per min)"),
		ingestValidation: newStreamPanel("Validation", 10, true),
		ingestUnlicensed: newStreamPanel("Unlicensed", 10, true),
	}

	d.seedIngestPlaceholders()

	got := d.ingestIngest.GetText(true)
	for _, want := range []string{"RBN: --", "FT8 --", "FT4 --", "FT2 --", "MSK --", "PSK --"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected ingest placeholder to include %q, got %q", want, got)
		}
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
		"Resolver Pressure: ok",
		"Stabilizer: ok",
		"Stabilizer Glyph: ok",
		"Temporal: ok",
		"CACHES & DATA FRESHNESS",
		"Grid: 1",
		"Meta: 2",
		"Known: 3",
		"",
		"CTY: now",
		"PATH PREDICTIONS",
		"VOACAP cache: n/a",
		"H3 path pairs: 100 / 200",
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
		"Resolver Pressure: ok",
		"Stabilizer: ok",
		"Stabilizer Glyph: ok",
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

func TestDashboardLifecycleWitnessesReadinessAndTermination(t *testing.T) {
	ready, runDone := make(chan struct{}), make(chan struct{})
	d := &DashboardV2{ready: ready, runDone: runDone}
	close(runDone)
	if d.WaitReadyStatus() {
		t.Fatal("application exit before first draw appeared ready")
	}

	ready, runDone = make(chan struct{}), make(chan struct{})
	schedulerDone := make(chan struct{})
	d = &DashboardV2{ready: ready, runDone: runDone, scheduler: &frameScheduler{done: schedulerDone}}
	d.wg.Add(1)
	close(ready)
	if !d.WaitReadyStatus() {
		t.Fatal("first draw was not recognized")
	}
	finished := make(chan struct{})
	go func() { d.WaitStopped(); close(finished) }()
	close(runDone)
	select {
	case <-finished:
		t.Fatal("application exit alone falsely proved scheduler retirement")
	default:
	}
	close(schedulerDone)
	select {
	case <-finished:
		t.Fatal("scheduler and app exit alone falsely proved dashboard worker retirement")
	default:
	}
	d.wg.Done()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("retirement was not observed after both workers returned")
	}
}

func TestDashboardDroppedAndReputationFilters(t *testing.T) {
	d := &DashboardV2{
		ingestValidation: newStreamPanel("Validation", 10, true),
		eventsStream:     newStreamPanel("Events", 10, false),
		scheduler:        newFrameScheduler(nil, 60, 50*time.Millisecond, nil),
	}
	d.AppendDropped("other drop must not appear")
	d.AppendDropped("CTY drop: accepted")
	d.AppendReputation("reputation must not appear")
	got := d.ingestValidation.SnapshotText()
	if !strings.Contains(got, "CTY drop: accepted") || strings.Contains(got, "other drop") {
		t.Fatalf("validation pane routing changed: %q", got)
	}
	if strings.Contains(d.eventsStream.SnapshotText(), "reputation must not appear") {
		t.Fatal("reputation no-op acquired a display route")
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

func TestStreamPanelOptionsFromConfigUsesByteAndMessageCaps(t *testing.T) {
	opts := streamPanelOptionsFromConfig(config.UIV2BufferConfig{
		MaxEvents:        7,
		MaxBytesMB:       2,
		MaxMessageBytes:  128,
		EvictOnByteLimit: true,
	}, 3)

	if opts.MaxEvents != 7 {
		t.Fatalf("expected max events 7, got %d", opts.MaxEvents)
	}
	if opts.MaxBytes != 2*1024*1024 {
		t.Fatalf("expected max bytes from MiB config, got %d", opts.MaxBytes)
	}
	if opts.MaxMessageBytes != 128 {
		t.Fatalf("expected max message bytes 128, got %d", opts.MaxMessageBytes)
	}
	if !opts.EvictOnByteLimit {
		t.Fatalf("expected evict-on-byte-limit to be preserved")
	}
}

func TestSetStatsAndSetSnapshotCoalesceToOneFrame(t *testing.T) {
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
		overviewSources:  newBoxedTextView("Ingest Sources"),
		overviewNetwork:  newBoxedTextView("Network"),
		scheduler:        newFrameScheduler(nil, 60, 50*time.Millisecond, nil),
	}
	d.snapshotFrameFn = func() {}

	d.SetStats([]string{"legacy stats"})
	d.SetSnapshot(Snapshot{OverviewLines: lines})

	if batch := d.scheduler.collectBatch(); len(batch) != 1 {
		t.Fatalf("expected SetStats and SetSnapshot to coalesce into one frame, got %d", len(batch))
	}
}

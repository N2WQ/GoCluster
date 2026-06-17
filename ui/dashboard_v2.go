package ui

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/config"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const (
	accentTag   = "[#7aa2f7]"
	accentReset = "[-]"
)

const (
	placeholderHeader = "[lightgray]Cluster[-]: --  [lightgray]Version[-]: --  [lightgray]Uptime[-]: --:--"
	placeholderMem    = "[lightgray]Heap[-]: --  [lightgray]Sys[-]: --  [lightgray]GC p99 (interval)[-]: --  [lightgray]Last GC[-]: --  [lightgray]Goroutines[-]: --"
	placeholderIngest = "[lightgray]RBN[-]: -- | [lightgray]CW[-] -- | [lightgray]RTTY[-] -- | [lightgray]FT8[-] -- | [lightgray]FT4[-] -- | [lightgray]FT2[-] --\n" +
		"[lightgray]PSK[-]: -- | [lightgray]CW[-] -- | [lightgray]RTTY[-] -- | [lightgray]FT8[-] -- | [lightgray]FT4[-] -- | [lightgray]FT2[-] -- | [lightgray]MSK[-] -- | [lightgray]PSK[-] --\n" +
		"[lightgray]P92[-]: --\n" +
		"[lightgray]Path[-]: -- (U) / -- (S) / -- (N) / -- (G) / -- (H) / -- (B) / -- (M)"
	placeholderPipeline = "[lightgray]Primary Dedupe[-]: -- | [lightgray]Secondary[-]: F-- M-- S--\n" +
		"[lightgray]Corrections[-]: -- | [lightgray]Unlicensed[-]: -- | [lightgray]Harmonics[-]: -- | [lightgray]Reputation[-]: --\n" +
		"\n" +
		"[lightgray]Stabilizer Glyph[-]: --"
	placeholderCaches = "[lightgray]Grid cache[-]:  [[white:white]   [black:white]326,629[-:-]   [-:-]░░░░] 98.5%  |  [lightgray]Meta[-]: [[white:white]  [black:white] 5,479[-:-]  [-:-]] 99.5%\n" +
		"\n" +
		"[lightgray]Custom SCP[-]: -- (R) / -- (S)\n" +
		"[lightgray]160m[-]: --  [lightgray]80m[-]: --  [lightgray]40m[-]: --  [lightgray]20m[-]: --\n" +
		"\n" +
		"[lightgray]CTY[-]: --  [lightgray]FCC[-]: --  [lightgray]Skew[-]: --  [lightgray]VOACAP SSN[-]: --"
	placeholderPath               = "[lightgray]H3 path pairs[-]: -- (L2) / -- (L1)\n[lightgray]160m[-]: -- / --   [lightgray]80m[-]: -- / --"
	placeholderIngestSources      = "[lightgray]Ingest[-]: -- / 4 connected\n\n(none)"
	placeholderNetwork            = "[lightgray]Telnet[-]: -- clients   [lightgray]Drops[-]: Q-- C-- W--"
	placeholderValidation         = "CTY drop: --"
	placeholderUnlicensed         = "Unlicensed drop: --"
	placeholderCorrected          = "Corrected: --"
	placeholderHarmonics          = "Harmonics: --"
	placeholderEvents             = "No events yet."
	streamPanelMaxLines           = 200
	overviewPipelineDefaultHeight = 9
	overviewPipelineMinHeight     = 4
	overviewCachesDefaultHeight   = 9
	overviewCachesMinHeight       = 3
	overviewPathMinHeight         = 3
	overviewSourcesDefaultHeight  = 5
	overviewSourcesMinHeight      = 3
)

var (
	uiBorderColor      = tcell.ColorGray
	uiTitleColor       = tcell.NewRGBColor(170, 180, 200)
	uiFocusBorderColor = tcell.NewRGBColor(122, 162, 247)
	uiFocusTitleColor  = tcell.NewRGBColor(122, 162, 247)
)

// DashboardV2 implements the page-based tview UI.
type DashboardV2 struct {
	app        *tview.Application
	pages      *tview.Pages
	scheduler  *frameScheduler
	activePage atomic.Value

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	ready chan struct{}

	snapshot   atomic.Pointer[dashboardSnapshot]
	statsMu    sync.Mutex
	statsLines []string
	networkMu  sync.RWMutex
	network    string

	overviewRoot      *tview.Flex
	overviewHdr       *tview.TextView
	overviewMem       *tview.TextView
	overviewIngest    *tview.TextView
	overviewPipeline  *tview.TextView
	overviewCaches    *tview.TextView
	overviewPath      *tview.TextView
	overviewSources   *tview.TextView
	overviewNetwork   *tview.TextView
	ingestRoot        *tview.Flex
	ingestHdr         *tview.TextView
	ingestIngest      *tview.TextView
	ingestValidation  *streamPanel
	ingestUnlicensed  *streamPanel
	pipelineRoot      *tview.Flex
	pipelineHdr       *tview.TextView
	pipelineQuality   *tview.TextView
	pipelineCorrected *streamPanel
	pipelineHarmonics *streamPanel
	eventsRoot        *tview.Flex
	eventsHdr         *tview.TextView
	eventsMem         *tview.TextView
	eventsIngest      *tview.TextView
	eventsPipeline    *tview.TextView
	eventsStream      *streamPanel

	overviewGroup focusGroup
	ingestGroup   focusGroup
	pipelineGroup focusGroup
	eventsGroup   focusGroup

	pageOrder []string
	pageIndex int
	helpShown bool
	metrics   *Metrics

	pagePresent map[string]bool

	snapshotFrameFn   func()
	validationFrameFn func()
	unlicensedFrameFn func()
	correctedFrameFn  func()
	harmonicsFrameFn  func()
	eventsFrameFn     func()
	networkFrameFn    func()

	overviewPipelineHeight int
	overviewCachesHeight   int
	overviewPathHeight     int
	overviewSourcesHeight  int
}

// NewDashboardV2 constructs the v2 dashboard if enabled.
func NewDashboardV2(cfg config.UIConfig, enable bool) *DashboardV2 {
	if !enable {
		return nil
	}

	//nolint:gosec // Stop calls the retained cancel function; the returned dashboard owns that lifecycle.
	ctx, cancel := context.WithCancel(context.Background())
	app := tview.NewApplication().EnableMouse(cfg.V2.EnableMouse)
	pages := tview.NewPages()
	ready := make(chan struct{})
	var once sync.Once
	app.SetBeforeDrawFunc(func(screen tcell.Screen) bool {
		once.Do(func() { close(ready) })
		return false
	})

	metrics := NewMetrics()
	d := &DashboardV2{
		app:                    app,
		pages:                  pages,
		ctx:                    ctx,
		cancel:                 cancel,
		ready:                  ready,
		pageOrder:              cfg.V2.Pages,
		metrics:                metrics,
		pagePresent:            make(map[string]bool),
		overviewPipelineHeight: overviewPipelineDefaultHeight,
		overviewCachesHeight:   overviewCachesDefaultHeight,
		overviewPathHeight:     overviewPathMinHeight,
		overviewSourcesHeight:  overviewSourcesDefaultHeight,
	}

	eventBufferOpts := streamPanelOptionsFromConfig(cfg.V2.EventBuffer, streamPanelMaxLines)
	debugBufferOpts := streamPanelOptionsFromConfig(cfg.V2.DebugBuffer, streamPanelMaxLines)

	d.overviewHdr = newBoxedTextView("Overview")
	d.overviewMem = newBoxedTextView("Memory / GC")
	d.overviewIngest = newBoxedTextView("Ingest Rates (per min)")
	d.overviewPipeline = newBoxedTextView("Pipeline Quality")
	d.overviewCaches = newBoxedTextView("Caches & Data Freshness")
	d.overviewPath = newBoxedTextView("Path Predictions")
	d.overviewSources = newBoxedTextView("Ingest Sources")
	d.overviewSources.SetScrollable(true)
	d.overviewNetwork = newBoxedTextView("Network")
	d.overviewNetwork.SetScrollable(true)
	d.seedOverviewPlaceholders()
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
	d.ingestHdr = newBoxedTextView("Overview")
	d.ingestIngest = newBoxedTextView("Ingest Rates (per min)")
	d.ingestValidation = newStreamPanelWithOptions("Validation", eventBufferOpts, true)
	d.ingestUnlicensed = newStreamPanelWithOptions("Unlicensed", eventBufferOpts, true)
	d.seedIngestPlaceholders()
	d.ingestRoot = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(d.ingestHdr, 3, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.ingestIngest, 6, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.ingestValidation.Primitive(), 28, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.ingestUnlicensed.Primitive(), 28, 0, false)

	d.pipelineHdr = newBoxedTextView("Overview")
	d.pipelineQuality = newBoxedTextView("Pipeline Quality")
	d.pipelineCorrected = newStreamPanelWithOptions("Corrected", eventBufferOpts, true)
	d.pipelineHarmonics = newStreamPanelWithOptions("Harmonics", eventBufferOpts, true)
	d.seedPipelinePlaceholders()
	d.pipelineRoot = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(d.pipelineHdr, 3, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.pipelineQuality, overviewPipelineDefaultHeight, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.pipelineCorrected.Primitive(), 28, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.pipelineHarmonics.Primitive(), 28, 0, false)

	d.eventsHdr = newBoxedTextView("Overview")
	d.eventsMem = newBoxedTextView("Memory / GC")
	d.eventsIngest = newBoxedTextView("Ingest Rates (per min)")
	d.eventsPipeline = newBoxedTextView("Pipeline Quality")
	d.eventsStream = newStreamPanelWithOptions("Events", debugBufferOpts, false)
	d.seedEventsPlaceholders()
	d.eventsRoot = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(d.eventsHdr, 3, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.eventsMem, 3, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.eventsIngest, 6, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.eventsPipeline, overviewPipelineDefaultHeight, 0, false).
		AddItem(newSpacer(), 1, 0, false).
		AddItem(d.eventsStream.Primitive(), 0, 1, false)

	d.overviewGroup = newFocusGroup(
		newFocusBox(d.overviewSources, "Ingest Sources", true),
		newFocusBox(d.overviewNetwork, "Network", true),
	)
	d.ingestGroup = newFocusGroup(d.ingestValidation, d.ingestUnlicensed)
	d.pipelineGroup = newFocusGroup(d.pipelineCorrected, d.pipelineHarmonics)
	d.eventsGroup = newFocusGroup(d.eventsStream)

	d.addPage("overview", d.overviewRoot)
	d.addPage("ingest", d.ingestRoot)
	d.addPage("pipeline", d.pipelineRoot)
	d.addPage("events", d.eventsRoot)

	help := buildHelpOverlay()
	d.addPage("help", help)

	d.snapshotFrameFn = d.renderSnapshot
	d.validationFrameFn = func() { d.ingestValidation.Render(d.app) }
	d.unlicensedFrameFn = func() { d.ingestUnlicensed.Render(d.app) }
	d.correctedFrameFn = func() { d.pipelineCorrected.Render(d.app) }
	d.harmonicsFrameFn = func() { d.pipelineHarmonics.Render(d.app) }
	d.eventsFrameFn = func() { d.eventsStream.Render(d.app) }
	d.networkFrameFn = d.renderOverviewNetwork

	d.scheduler = newFrameScheduler(app, cfg.V2.TargetFPS, 100*time.Millisecond, metrics.ObserveRender)
	d.scheduler.Start()

	d.installKeybindings()
	d.installRoot()

	go func() {
		if err := app.Run(); err != nil {
			log.Printf("UI: tview-v2 error: %v", err)
		}
	}()

	return d
}

func streamPanelOptionsFromConfig(buf config.UIV2BufferConfig, fallbackMaxEvents int) streamPanelOptions {
	maxEvents := buf.MaxEvents
	if maxEvents <= 0 {
		maxEvents = fallbackMaxEvents
	}
	if maxEvents <= 0 {
		maxEvents = 1
	}
	maxBytes := 0
	if buf.MaxBytesMB > 0 {
		maxBytes = buf.MaxBytesMB * 1024 * 1024
	}
	return streamPanelOptions{
		MaxEvents:        maxEvents,
		MaxBytes:         maxBytes,
		MaxMessageBytes:  buf.MaxMessageBytes,
		EvictOnByteLimit: buf.EvictOnByteLimit,
	}
}

func (d *DashboardV2) installRoot() {
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(d.pages, 0, 1, true).
		AddItem(buildFooter(), 1, 0, false)
	d.app.SetRoot(root, true)
	d.showFirstAvailablePage()
}

func (d *DashboardV2) installKeybindings() {
	d.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if d.helpShown {
			if event.Key() == tcell.KeyEsc || event.Rune() == 'h' || event.Rune() == '?' {
				d.toggleHelp(false)
				return nil
			}
		}

		if pageName, _ := d.pages.GetFrontPage(); pageName == "ingest" {
			if d.ingestGroup.handleScroll(d.app, event) {
				return nil
			}
		} else if pageName == "overview" {
			if d.overviewGroup.handleScroll(d.app, event) {
				return nil
			}
		} else if pageName == "pipeline" {
			if d.pipelineGroup.handleScroll(d.app, event) {
				return nil
			}
		} else if pageName == "events" {
			if d.eventsGroup.handleScroll(d.app, event) {
				return nil
			}
		}

		switch event.Key() {
		case tcell.KeyTab:
			if pageName, _ := d.pages.GetFrontPage(); pageName == "ingest" {
				d.ingestGroup.cycle(d.app, 1)
			} else if pageName == "overview" {
				d.overviewGroup.cycle(d.app, 1)
			} else if pageName == "pipeline" {
				d.pipelineGroup.cycle(d.app, 1)
			} else if pageName == "events" {
				d.eventsGroup.cycle(d.app, 1)
			} else {
				d.nextPage()
			}
			return nil
		case tcell.KeyBacktab:
			if pageName, _ := d.pages.GetFrontPage(); pageName == "ingest" {
				d.ingestGroup.cycle(d.app, -1)
			} else if pageName == "overview" {
				d.overviewGroup.cycle(d.app, -1)
			} else if pageName == "pipeline" {
				d.pipelineGroup.cycle(d.app, -1)
			} else if pageName == "events" {
				d.eventsGroup.cycle(d.app, -1)
			} else {
				d.prevPage()
			}
			return nil
		case tcell.KeyCtrlC:
			d.Stop()
			return nil
		}

		switch event.Rune() {
		case 'q', 'Q':
			d.Stop()
			return nil
		case 'h', '?':
			d.toggleHelp(!d.helpShown)
			return nil
		case 'o', 'O':
			d.showPage("overview")
			return nil
		case 'i', 'I':
			d.showPage("ingest")
			return nil
		case 'p', 'P':
			d.showPage("pipeline")
			return nil
		case 'e', 'E':
			d.showPage("events")
			return nil
		}

		return event
	})
}

func (d *DashboardV2) toggleHelp(show bool) {
	d.helpShown = show
	d.pages.ShowPage("help")
	d.pages.SendToFront("help")
	if !show {
		d.pages.HidePage("help")
	}
}

func (d *DashboardV2) showPage(name string) {
	if !d.pageEnabled(name) || !d.pageAvailable(name) {
		return
	}
	for i, page := range d.pageOrder {
		if page == name {
			d.pageIndex = i
			break
		}
	}
	d.pages.SwitchToPage(name)
	d.activePage.Store(name)
	d.refreshVisiblePage(name)
	if d.metrics != nil {
		d.metrics.PageSwitch()
	}
	switch name {
	case "overview":
		d.overviewGroup.set(d.app, 0)
	case "ingest":
		d.ingestGroup.set(d.app, 0)
	case "pipeline":
		d.pipelineGroup.set(d.app, 0)
	case "events":
		d.eventsGroup.set(d.app, 0)
	}
}

func (d *DashboardV2) showFirstAvailablePage() {
	if d == nil {
		return
	}
	if name, ok := d.firstAvailablePage(); ok {
		d.showPage(name)
	}
}

func (d *DashboardV2) firstAvailablePage() (string, bool) {
	if d == nil {
		return "", false
	}
	for _, name := range d.pageOrder {
		if d.pageAvailable(name) {
			return name, true
		}
	}
	return "", false
}

func (d *DashboardV2) nextPage() {
	if len(d.pageOrder) == 0 {
		return
	}
	d.cyclePage(1)
}

func (d *DashboardV2) prevPage() {
	if len(d.pageOrder) == 0 {
		return
	}
	d.cyclePage(-1)
}

func (d *DashboardV2) pageEnabled(name string) bool {
	for _, page := range d.pageOrder {
		if page == name {
			return true
		}
	}
	return false
}

func (d *DashboardV2) pageAvailable(name string) bool {
	if d == nil {
		return false
	}
	return d.pagePresent[name]
}

func (d *DashboardV2) addPage(name string, page tview.Primitive) {
	if d == nil || d.pages == nil || page == nil || name == "" {
		return
	}
	d.pages.AddPage(name, page, true, false)
	d.pagePresent[name] = true
}

func (d *DashboardV2) cyclePage(delta int) {
	if d == nil || len(d.pageOrder) == 0 {
		return
	}
	for i := 0; i < len(d.pageOrder); i++ {
		d.pageIndex += delta
		if d.pageIndex < 0 {
			d.pageIndex = len(d.pageOrder) - 1
		} else if d.pageIndex >= len(d.pageOrder) {
			d.pageIndex = 0
		}
		name := d.pageOrder[d.pageIndex]
		if d.pageAvailable(name) {
			d.showPage(name)
			return
		}
	}
}

func (d *DashboardV2) WaitReady() {
	if d == nil || d.ready == nil {
		return
	}
	<-d.ready
}

func (d *DashboardV2) Stop() {
	if d == nil {
		return
	}
	d.cancel()
	if d.scheduler != nil {
		d.scheduler.Stop()
	}
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		log.Printf("UI: dashboard stop timeout, some goroutines may leak")
	}
	if d.app != nil {
		d.app.Stop()
	}
}

func (d *DashboardV2) SetStats(lines []string) {
	if d == nil || d.scheduler == nil {
		return
	}
	d.statsMu.Lock()
	d.statsLines = appendFilteredOverviewLines(d.statsLines[:0], lines)
	d.statsMu.Unlock()
	d.scheduler.Schedule("snapshot", d.snapshotFrameFn)
}

func (d *DashboardV2) SetSnapshot(snapshot Snapshot) {
	if d == nil || d.scheduler == nil {
		return
	}
	d.snapshot.Store(cloneSnapshot(snapshot))
	d.scheduler.Schedule("snapshot", d.snapshotFrameFn)
}

func (d *DashboardV2) UpdateNetworkStatus(summaryLine string, clientLines []string) {
	if d == nil || d.scheduler == nil {
		return
	}
	lines := make([]string, 0, 1+len(clientLines))
	if summaryLine != "" {
		lines = append(lines, summaryLine)
	}
	lines = append(lines, clientLines...)
	d.storeNetworkLines(lines)
	if d.currentActivePage() == "overview" {
		d.scheduler.Schedule("network", d.networkFrameFn)
	}
}

func (d *DashboardV2) renderSnapshot() {
	active := d.currentActivePage()
	if active == "" {
		d.refreshVisiblePage("overview")
		d.refreshVisiblePage("ingest")
		d.refreshVisiblePage("pipeline")
		d.refreshVisiblePage("events")
		return
	}
	d.refreshVisiblePage(active)
}

type dashboardSnapshot struct {
	GeneratedAt    time.Time
	HasContent     bool
	Header         string
	Memory         string
	Ingest         string
	Pipeline       string
	Caches         string
	Path           string
	Sources        string
	Network        string
	PipelineHeight int
	CachesHeight   int
	PathHeight     int
	SourcesHeight  int
}

func cloneSnapshot(src Snapshot) *dashboardSnapshot {
	return buildDashboardSnapshot(src.GeneratedAt, src.OverviewLines)
}

func buildDashboardSnapshot(generatedAt time.Time, lines []string) *dashboardSnapshot {
	snap := &dashboardSnapshot{
		GeneratedAt: generatedAt,
		HasContent:  len(lines) > 0,
	}
	if len(lines) == 0 {
		return snap
	}
	snap.Header = lines[0]
	if len(lines) > 2 {
		snap.Memory = lines[2]
	}
	snap.Ingest = joinOverviewSection(lines, "INGEST RATES (per min)", "PIPELINE QUALITY")
	snap.Pipeline, snap.PipelineHeight = joinOverviewSectionWithHeight(lines, "PIPELINE QUALITY", "CACHES & DATA FRESHNESS", overviewPipelineMinHeight)

	cacheIdx := -1
	pathIdx := -1
	sourceIdx := -1
	networkIdx := -1
	for i, line := range lines {
		switch line {
		case "CACHES & DATA FRESHNESS":
			cacheIdx = i
		case "PATH PREDICTIONS":
			pathIdx = i
		case "INGEST SOURCES":
			sourceIdx = i
		case "NETWORK":
			networkIdx = i
		}
	}
	if cacheIdx >= 0 && pathIdx > cacheIdx+1 {
		snap.Caches, snap.CachesHeight = joinLinesWithHeight(lines[cacheIdx+1:pathIdx], overviewCachesMinHeight)
	}
	pathEnd := networkIdx
	if sourceIdx > pathIdx+1 && sourceIdx < pathEnd {
		pathEnd = sourceIdx
	}
	if pathIdx >= 0 && pathEnd > pathIdx+1 {
		snap.Path, snap.PathHeight = joinLinesWithHeight(lines[pathIdx+1:pathEnd], overviewPathMinHeight)
	}
	if sourceIdx >= 0 && networkIdx > sourceIdx+1 {
		snap.Sources, snap.SourcesHeight = joinLinesWithHeight(lines[sourceIdx+1:networkIdx], overviewSourcesMinHeight)
	}
	if networkIdx >= 0 && len(lines) > networkIdx+1 {
		snap.Network = strings.Join(lines[networkIdx+1:], "\n")
	}
	return snap
}

func joinOverviewSection(lines []string, startMarker, endMarker string) string {
	section := overviewSectionLines(lines, startMarker, endMarker)
	if len(section) == 0 {
		return ""
	}
	return strings.Join(section, "\n")
}

func joinOverviewSectionWithHeight(lines []string, startMarker, endMarker string, minHeight int) (string, int) {
	return joinLinesWithHeight(overviewSectionLines(lines, startMarker, endMarker), minHeight)
}

func joinLinesWithHeight(lines []string, minHeight int) (string, int) {
	lines = appendFilteredOverviewLines(nil, lines)
	if len(lines) == 0 {
		return "", 0
	}
	height := len(lines) + 2
	if height < minHeight {
		height = minHeight
	}
	return strings.Join(lines, "\n"), height
}

func appendFilteredOverviewLines(dst []string, lines []string) []string {
	for _, line := range lines {
		if isRemovedOverviewPipelineLine(line) {
			continue
		}
		dst = append(dst, line)
	}
	return dst
}

func isRemovedOverviewPipelineLine(line string) bool {
	line = strings.TrimSpace(line)
	for _, prefix := range []string{
		"[yellow]Resolver[-]:",
		"Resolver:",
		"[yellow]Resolver Pressure[-]:",
		"Resolver Pressure:",
		"[yellow]Stabilizer[-]:",
		"Stabilizer:",
		"[yellow]Temporal[-]:",
		"Temporal:",
		"[yellow]FT Burst[-]:",
		"FT Burst:",
	} {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

func (d *DashboardV2) AppendDropped(line string) {
	if strings.HasPrefix(line, "CTY drop:") {
		d.appendStream(d.ingestValidation, "ingest", "validation", d.validationFrameFn, line)
	}
}

func (d *DashboardV2) AppendCall(line string) {
	d.appendStream(d.pipelineCorrected, "pipeline", "corrected", d.correctedFrameFn, line)
}

func (d *DashboardV2) AppendUnlicensed(line string) {
	d.appendStream(d.ingestUnlicensed, "ingest", "unlicensed", d.unlicensedFrameFn, line)
}

func (d *DashboardV2) AppendHarmonic(line string) {
	d.appendStream(d.pipelineHarmonics, "pipeline", "harmonics", d.harmonicsFrameFn, line)
}

func (d *DashboardV2) appendStream(panel *streamPanel, pageName, scheduleID string, frameFn func(), line string) {
	if d == nil || panel == nil || d.scheduler == nil {
		return
	}
	panel.Append(line)
	if d.currentActivePage() != pageName {
		return
	}
	// Coalesce updates per frame; the scheduler keeps only the latest per ID.
	d.scheduler.Schedule(scheduleID, frameFn)
}

func (d *DashboardV2) AppendReputation(line string) {
	if d == nil {
		return
	}
}

func (d *DashboardV2) AppendSystem(line string) {
	if d == nil {
		return
	}
	d.appendStream(d.eventsStream, "events", "events", d.eventsFrameFn, line)
}

func (d *DashboardV2) SystemWriter() io.Writer {
	if d == nil {
		return nil
	}
	return &dashboardV2LineWriter{append: d.AppendSystem}
}

func newBoxedTextView(title string) *tview.TextView {
	tv := tview.NewTextView().SetDynamicColors(true).SetWrap(false)
	tv.SetBorder(true)
	applyFocusStyle(tv, title, false)
	return tv
}

func applyFocusStyle(tv *tview.TextView, title string, focused bool) {
	if tv == nil {
		return
	}
	applyFocusBoxStyle(tv.Box, title, focused)
}

func applyFocusBoxStyle(box *tview.Box, title string, focused bool) {
	if box == nil {
		return
	}
	if title != "" {
		box.SetTitle(title).SetTitleAlign(tview.AlignLeft)
	}
	if focused {
		box.SetBorderColor(uiFocusBorderColor)
		box.SetTitleColor(uiFocusTitleColor)
		return
	}
	box.SetBorderColor(uiBorderColor)
	box.SetTitleColor(uiTitleColor)
}

func newSpacer() *tview.Box {
	return tview.NewBox()
}

func buildFooter() *tview.TextView {
	return tview.NewTextView().SetDynamicColors(true).SetText(
		accentText("H") + "elp  " + accentText("O") + "verview  " + accentText("I") + "ngest  " + accentText("P") + "ipeline  " + accentText("E") + "vents  [Q]Quit",
	)
}

func (d *DashboardV2) updateOverviewBoxes(lines []string) {
	d.updateOverviewSnapshot(buildDashboardSnapshot(time.Time{}, lines))
}

func (d *DashboardV2) updateOverviewSnapshot(snap *dashboardSnapshot) {
	if snap == nil || !snap.HasContent {
		d.seedOverviewPlaceholders()
		return
	}
	setBoxText(d.overviewHdr, snap.Header)
	if snap.Memory != "" {
		setBoxText(d.overviewMem, snap.Memory)
	}
	if snap.Ingest != "" {
		setBoxText(d.overviewIngest, snap.Ingest)
	}
	if snap.Pipeline != "" {
		setBoxText(d.overviewPipeline, snap.Pipeline)
		if d.overviewRoot != nil && snap.PipelineHeight > 0 && snap.PipelineHeight != d.overviewPipelineHeight {
			d.overviewRoot.ResizeItem(d.overviewPipeline, snap.PipelineHeight, 0)
			d.overviewPipelineHeight = snap.PipelineHeight
		}
	}
	if snap.Caches != "" {
		setBoxText(d.overviewCaches, snap.Caches)
		if d.overviewRoot != nil && snap.CachesHeight > 0 && snap.CachesHeight != d.overviewCachesHeight {
			d.overviewRoot.ResizeItem(d.overviewCaches, snap.CachesHeight, 0)
			d.overviewCachesHeight = snap.CachesHeight
		}
	}
	if snap.Path != "" {
		setBoxText(d.overviewPath, snap.Path)
		// Grow-only resize preserves full path bucket visibility while avoiding
		// repetitive layout churn on every stats refresh.
		if d.overviewRoot != nil && snap.PathHeight > d.overviewPathHeight {
			d.overviewRoot.ResizeItem(d.overviewPath, snap.PathHeight, 0)
			d.overviewPathHeight = snap.PathHeight
		}
	}
	if snap.Sources != "" {
		setBoxText(d.overviewSources, snap.Sources)
		if d.overviewRoot != nil && d.overviewSources != nil && snap.SourcesHeight > 0 && snap.SourcesHeight != d.overviewSourcesHeight {
			d.overviewRoot.ResizeItem(d.overviewSources, snap.SourcesHeight, 0)
			d.overviewSourcesHeight = snap.SourcesHeight
		}
	} else {
		setBoxText(d.overviewSources, placeholderIngestSources)
	}
	if snap.Network != "" {
		setBoxText(d.overviewNetwork, snap.Network)
	}
}

func (d *DashboardV2) updateIngestSnapshot(snap *dashboardSnapshot) {
	if snap == nil || !snap.HasContent {
		d.seedIngestPlaceholders()
		return
	}
	setBoxText(d.ingestHdr, snap.Header)
	if snap.Ingest != "" {
		setBoxText(d.ingestIngest, snap.Ingest)
	}
}

func (d *DashboardV2) updatePipelineSnapshot(snap *dashboardSnapshot) {
	if snap == nil || !snap.HasContent {
		d.seedPipelinePlaceholders()
		return
	}
	setBoxText(d.pipelineHdr, snap.Header)
	if snap.Pipeline != "" {
		setBoxText(d.pipelineQuality, snap.Pipeline)
	}
}

func (d *DashboardV2) updateEventsOverviewBoxes(lines []string) {
	d.updateEventsOverviewSnapshot(buildDashboardSnapshot(time.Time{}, lines))
}

func (d *DashboardV2) updateEventsOverviewSnapshot(snap *dashboardSnapshot) {
	if snap == nil || !snap.HasContent {
		d.seedEventsPlaceholders()
		return
	}
	setBoxText(d.eventsHdr, snap.Header)
	if snap.Memory != "" {
		setBoxText(d.eventsMem, snap.Memory)
	}
	if snap.Ingest != "" {
		setBoxText(d.eventsIngest, snap.Ingest)
	}
	if snap.Pipeline != "" {
		setBoxText(d.eventsPipeline, snap.Pipeline)
	}
}

func (d *DashboardV2) seedOverviewPlaceholders() {
	setBoxText(d.overviewHdr, placeholderHeader)
	setBoxText(d.overviewMem, placeholderMem)
	setBoxText(d.overviewIngest, placeholderIngest)
	setBoxText(d.overviewPipeline, placeholderPipeline)
	setBoxText(d.overviewCaches, placeholderCaches)
	setBoxText(d.overviewPath, placeholderPath)
	setBoxText(d.overviewSources, placeholderIngestSources)
	setBoxText(d.overviewNetwork, placeholderNetwork)
}

func (d *DashboardV2) seedEventsPlaceholders() {
	if d == nil {
		return
	}
	setBoxText(d.eventsHdr, placeholderHeader)
	setBoxText(d.eventsMem, placeholderMem)
	setBoxText(d.eventsIngest, placeholderIngest)
	setBoxText(d.eventsPipeline, placeholderPipeline)
	d.eventsStream.SetText(placeholderEvents)
}

func (d *DashboardV2) seedIngestPlaceholders() {
	if d == nil || d.ingestHdr == nil || d.ingestIngest == nil {
		return
	}
	setBoxText(d.ingestHdr, placeholderHeader)
	setBoxText(d.ingestIngest, placeholderIngest)
	d.ingestValidation.SetText(placeholderValidation)
	d.ingestUnlicensed.SetText(placeholderUnlicensed)
}

func (d *DashboardV2) seedPipelinePlaceholders() {
	if d == nil || d.pipelineHdr == nil || d.pipelineQuality == nil {
		return
	}
	setBoxText(d.pipelineHdr, placeholderHeader)
	setBoxText(d.pipelineQuality, placeholderPipeline)
	d.pipelineCorrected.SetText(placeholderCorrected)
	d.pipelineHarmonics.SetText(placeholderHarmonics)
}

func setBoxText(tv *tview.TextView, text string) {
	if tv == nil {
		return
	}
	tv.SetText(padLines(text))
}

func overviewSectionLines(lines []string, startMarker, endMarker string) []string {
	if len(lines) == 0 {
		return nil
	}
	start := -1
	end := -1
	for i, line := range lines {
		switch line {
		case startMarker:
			start = i
		case endMarker:
			end = i
		}
	}
	if start < 0 || end <= start+1 {
		return nil
	}
	return lines[start+1 : end]
}

func (d *DashboardV2) currentActivePage() string {
	if d == nil {
		return ""
	}
	if value := d.activePage.Load(); value != nil {
		if page, ok := value.(string); ok {
			return page
		}
	}
	return ""
}

func (d *DashboardV2) refreshVisiblePage(page string) {
	if d == nil {
		return
	}
	snap := d.overviewSnapshot()
	switch page {
	case "overview":
		d.updateOverviewSnapshot(snap)
		d.renderOverviewNetwork()
	case "ingest":
		d.updateIngestSnapshot(snap)
	case "pipeline":
		d.updatePipelineSnapshot(snap)
	case "events":
		d.updateEventsOverviewSnapshot(snap)
	}
}

func (d *DashboardV2) overviewSnapshot() *dashboardSnapshot {
	if d == nil {
		return nil
	}
	if snap := d.snapshot.Load(); snap != nil && snap.HasContent {
		return snap
	}
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	return buildDashboardSnapshot(time.Time{}, d.statsLines)
}

func (d *DashboardV2) storeNetworkLines(lines []string) {
	if d == nil {
		return
	}
	text := strings.Join(lines, "\n")
	d.networkMu.Lock()
	d.network = text
	d.networkMu.Unlock()
}

func (d *DashboardV2) networkTextSnapshot() string {
	if d == nil {
		return ""
	}
	d.networkMu.RLock()
	defer d.networkMu.RUnlock()
	return d.network
}

func (d *DashboardV2) renderOverviewNetwork() {
	if d == nil || d.overviewNetwork == nil {
		return
	}
	text := d.networkTextSnapshot()
	if text == "" {
		return
	}
	setBoxText(d.overviewNetwork, text)
}

func addOverviewTopSections(root *tview.Flex, ingest *tview.TextView) {
	if root == nil || ingest == nil {
		return
	}
	root.AddItem(ingest, 6, 0, false)
}

func scrollTextView(target *tview.TextView, event *tcell.EventKey) bool {
	if target == nil || event == nil {
		return false
	}
	row, col := target.GetScrollOffset()
	page := 10
	_, _, _, height := target.GetInnerRect()
	if height > 0 {
		page = height - 1
		if page < 1 {
			page = 1
		}
	}
	switch event.Key() {
	case tcell.KeyUp:
		if row > 0 {
			row--
		}
	case tcell.KeyDown:
		row++
	case tcell.KeyPgUp:
		row -= page
		if row < 0 {
			row = 0
		}
	case tcell.KeyPgDn:
		row += page
	case tcell.KeyHome:
		row = 0
	case tcell.KeyEnd:
		row = 1 << 30
	case tcell.KeyRune:
		switch event.Rune() {
		case 'k':
			if row > 0 {
				row--
			}
		case 'j':
			row++
		default:
			return false
		}
	default:
		return false
	}
	target.ScrollTo(row, col)
	return true
}

func buildHelpOverlay() tview.Primitive {
	help := tview.NewTextView().SetDynamicColors(true).SetWrap(true)
	help.SetText(strings.TrimSpace(fmt.Sprintf(`
KEYBOARD HELP

NAVIGATION
  %sH%s Help   %sO%s Overview   %sI%s Ingest   %sP%s Pipeline   %sE%s Events
  Tab Next pane   Shift+Tab Previous pane   q / Ctrl+C Quit

SCROLLING
  ↑/↓ or k/j Scroll   PageUp/Down Fast scroll   Home/End Top/Bottom

PIPELINE METRICS (PLAIN ENGLISH)
  Primary Dedupe: Duplicate suppression at the main ingest gate.
  Secondary F/M/S: Broadcast dedupe output by policy (fast/med/slow).
  Corrections/Unlicensed/Harmonics/Reputation: Cumulative totals since process start.
  Stabilizer Glyph:
    average stabilizer delay turns by glyph (ordered ? / S / P / V / C).
`, accentTag, accentReset, accentTag, accentReset, accentTag, accentReset, accentTag, accentReset, accentTag, accentReset)))
	help.SetBorder(true).SetTitle("Help")
	help.SetBorderColor(uiBorderColor)
	help.SetTitleColor(uiTitleColor)
	container := tview.NewFlex().
		AddItem(nil, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 1, false).
			AddItem(help, 30, 1, true).
			AddItem(nil, 0, 1, false),
			60, 1, true).
		AddItem(nil, 0, 1, false)
	return container
}

func padLines(text string) string {
	if text == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(text) + 8)
	atLineStart := true
	for _, r := range text {
		if atLineStart && r != '\n' {
			b.WriteByte(' ')
		}
		b.WriteRune(r)
		if r == '\n' {
			atLineStart = true
		} else {
			atLineStart = false
		}
	}
	return b.String()
}

func accentText(text string) string {
	if text == "" {
		return ""
	}
	return accentTag + text + accentReset
}

type dashboardV2LineWriter struct {
	append func(string)
	mu     sync.Mutex
	buf    []byte
}

func (w *dashboardV2LineWriter) Write(p []byte) (int, error) {
	if w == nil || w.append == nil {
		return len(p), nil
	}
	w.mu.Lock()
	w.buf = append(w.buf, p...)
	data := w.buf
	w.mu.Unlock()

	for {
		idx := bytes.IndexByte(data, '\n')
		if idx < 0 {
			break
		}
		line := strings.TrimRight(string(data[:idx]), "\r")
		w.append(line)
		data = data[idx+1:]
	}
	w.mu.Lock()
	w.buf = data
	w.mu.Unlock()
	return len(p), nil
}

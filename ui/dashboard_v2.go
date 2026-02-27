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
	placeholderIngest = "[lightgray]RBN[-]: -- | [lightgray]CW[-] -- | [lightgray]RTTY[-] -- | [lightgray]FT8[-] -- | [lightgray]FT4[-] --\n" +
		"[lightgray]PSK[-]: -- | [lightgray]CW[-] -- | [lightgray]RTTY[-] -- | [lightgray]FT8[-] -- | [lightgray]FT4[-] -- | [lightgray]MSK[-] -- | [lightgray]PSK[-] --\n" +
		"[lightgray]P92[-]: --\n" +
		"[lightgray]Path[-]: -- (U) / -- (S) / -- (N) / -- (G) / -- (H) / -- (B) / -- (M)"
	placeholderPipeline = "[lightgray]Primary Dedupe[-]: -- | [lightgray]Secondary[-]: F-- M-- S--\n" +
		"[lightgray]Corrections[-]: -- | [lightgray]Unlicensed[-]: -- | [lightgray]Harmonics[-]: -- | [lightgray]Reputation[-]: --\n" +
		"\n" +
		"[lightgray]Resolver[-]: --\n" +
		"[lightgray]Resolver Pressure[-]: --\n" +
		"\n" +
		"[lightgray]Stabilizer[-]: --\n" +
		"[lightgray]Stabilizer Reason[-]: --\n" +
		"\n" +
		"[lightgray]Temporal[-]: --\n" +
		"[lightgray]Temporal latency[-]: --"
	placeholderCaches = "[lightgray]Grid cache[-]:  [[white:white]   [black:white]326,629[-:-]   [-:-]░░░░] 98.5%  |  [lightgray]Meta[-]: [[white:white]  [black:white] 5,479[-:-]  [-:-]] 99.5%\n" +
		"\n" +
		"[lightgray]Known calls[-]: --\n" +
		"[lightgray]160m[-]: --  [lightgray]80m[-]: --  [lightgray]40m[-]: --  [lightgray]20m[-]: --\n" +
		"\n" +
		"[lightgray]CTY[-]: --  [lightgray]FCC[-]: --  [lightgray]Skew[-]: --"
	placeholderPath               = "[lightgray]Path pairs[-]: -- (L2) / -- (L1)\n[lightgray]160m[-]: -- / --   [lightgray]80m[-]: -- / --"
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

	snapshot   atomic.Pointer[Snapshot]
	statsMu    sync.Mutex
	statsLines []string
	networkMu  sync.RWMutex
	network    []string

	overviewRoot      *tview.Flex
	overviewHdr       *tview.TextView
	overviewMem       *tview.TextView
	overviewIngest    *tview.TextView
	overviewPipeline  *tview.TextView
	overviewCaches    *tview.TextView
	overviewPath      *tview.TextView
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
}

// NewDashboardV2 constructs the v2 dashboard if enabled.
func NewDashboardV2(cfg config.UIConfig, enable bool) *DashboardV2 {
	if !enable {
		return nil
	}

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
	}

	d.overviewHdr = newBoxedTextView("Overview")
	d.overviewMem = newBoxedTextView("Memory / GC")
	d.overviewIngest = newBoxedTextView("Ingest Rates (per min)")
	d.overviewPipeline = newBoxedTextView("Pipeline Quality")
	d.overviewCaches = newBoxedTextView("Caches & Data Freshness")
	d.overviewPath = newBoxedTextView("Path Predictions")
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
		AddItem(d.overviewNetwork, 0, 1, false)
	d.ingestHdr = newBoxedTextView("Overview")
	d.ingestIngest = newBoxedTextView("Ingest Rates (per min)")
	d.ingestValidation = newStreamPanel("Validation", streamPanelMaxLines, true)
	d.ingestUnlicensed = newStreamPanel("Unlicensed", streamPanelMaxLines, true)
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
	d.pipelineCorrected = newStreamPanel("Corrected", streamPanelMaxLines, true)
	d.pipelineHarmonics = newStreamPanel("Harmonics", streamPanelMaxLines, true)
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
	d.eventsStream = newStreamPanel("Events", streamPanelMaxLines, false)
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

	d.overviewGroup = newFocusGroup(newFocusBox(d.overviewNetwork, "Network", true))
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

	d.installKeybindings(cfg)
	d.installRoot()

	go func() {
		if err := app.Run(); err != nil {
			log.Printf("UI: tview-v2 error: %v", err)
		}
	}()

	return d
}

func (d *DashboardV2) installRoot() {
	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(d.pages, 0, 1, true).
		AddItem(buildFooter(), 1, 0, false)
	d.app.SetRoot(root, true)
	d.showFirstAvailablePage()
}

func (d *DashboardV2) installKeybindings(cfg config.UIConfig) {
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
	d.statsLines = append(d.statsLines[:0], lines...)
	d.statsMu.Unlock()
	d.scheduler.Schedule("stats", d.snapshotFrameFn)
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

func cloneSnapshot(src Snapshot) *Snapshot {
	copyLines := func(lines []string) []string {
		if len(lines) == 0 {
			return nil
		}
		out := make([]string, len(lines))
		copy(out, lines)
		return out
	}
	return &Snapshot{
		GeneratedAt:   src.GeneratedAt,
		OverviewLines: copyLines(src.OverviewLines),
		IngestLines:   copyLines(src.IngestLines),
		PipelineLines: copyLines(src.PipelineLines),
		NetworkLines:  copyLines(src.NetworkLines),
	}
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
	if len(lines) == 0 {
		d.seedOverviewPlaceholders()
		return
	}
	// Expected format from buildOverviewLines:
	// 0 header
	// 1 "MEMORY / GC"
	// 2 memory line
	// 3 "INGEST RATES (per min)"
	// 4 rbn line
	// 5 psk line
	// 6 p92 line
	// 7 path-only line
	// 8 primary/secondary line
	// 9 corrections line
	// Section markers are used to slice cache/path/network blocks.
	setOverviewHeader(d.overviewHdr, lines)
	if len(lines) > 2 {
		setBoxText(d.overviewMem, lines[2])
	}
	setOverviewIngest(d.overviewIngest, lines)
	setOverviewPipeline(d.overviewPipeline, lines)
	pipelineLines := overviewSectionLines(lines, "PIPELINE QUALITY", "CACHES & DATA FRESHNESS")
	if len(pipelineLines) > 0 {
		neededHeight := len(pipelineLines) + 2
		if neededHeight < overviewPipelineMinHeight {
			neededHeight = overviewPipelineMinHeight
		}
		if d.overviewRoot != nil && neededHeight != d.overviewPipelineHeight {
			d.overviewRoot.ResizeItem(d.overviewPipeline, neededHeight, 0)
			d.overviewPipelineHeight = neededHeight
		}
	}
	cacheIdx := -1
	pathIdx := -1
	networkIdx := -1
	for i, line := range lines {
		switch line {
		case "CACHES & DATA FRESHNESS":
			cacheIdx = i
		case "PATH PREDICTIONS":
			pathIdx = i
		case "NETWORK":
			networkIdx = i
		}
	}
	if cacheIdx >= 0 && pathIdx > cacheIdx+1 {
		cacheLines := lines[cacheIdx+1 : pathIdx]
		setBoxText(d.overviewCaches, strings.Join(cacheLines, "\n"))
		neededHeight := len(cacheLines) + 2
		if neededHeight < overviewCachesMinHeight {
			neededHeight = overviewCachesMinHeight
		}
		// Resize only on actual content-height changes.
		if d.overviewRoot != nil && neededHeight != d.overviewCachesHeight {
			d.overviewRoot.ResizeItem(d.overviewCaches, neededHeight, 0)
			d.overviewCachesHeight = neededHeight
		}
	}
	if pathIdx >= 0 && networkIdx > pathIdx+1 {
		pathLines := lines[pathIdx+1 : networkIdx]
		setBoxText(d.overviewPath, strings.Join(pathLines, "\n"))
		neededHeight := len(pathLines) + 2
		if neededHeight < overviewPathMinHeight {
			neededHeight = overviewPathMinHeight
		}
		// Grow-only resize preserves full path bucket visibility while avoiding
		// repetitive layout churn on every stats refresh.
		if d.overviewRoot != nil && neededHeight > d.overviewPathHeight {
			d.overviewRoot.ResizeItem(d.overviewPath, neededHeight, 0)
			d.overviewPathHeight = neededHeight
		}
	}
	if networkIdx >= 0 && len(lines) > networkIdx+1 {
		networkLines := lines[networkIdx+1:]
		setBoxText(d.overviewNetwork, strings.Join(networkLines, "\n"))
	}
}

func (d *DashboardV2) updateIngestBoxes(lines []string) {
	if len(lines) == 0 {
		d.seedIngestPlaceholders()
		return
	}
	setOverviewHeader(d.ingestHdr, lines)
	setOverviewIngest(d.ingestIngest, lines)
}

func (d *DashboardV2) updatePipelineBoxes(lines []string) {
	if len(lines) == 0 {
		d.seedPipelinePlaceholders()
		return
	}
	setOverviewHeader(d.pipelineHdr, lines)
	setOverviewPipeline(d.pipelineQuality, lines)
}

func (d *DashboardV2) updateEventsOverviewBoxes(lines []string) {
	if len(lines) == 0 {
		d.seedEventsPlaceholders()
		return
	}
	setOverviewHeader(d.eventsHdr, lines)
	if len(lines) > 2 {
		setBoxText(d.eventsMem, lines[2])
	}
	setOverviewIngest(d.eventsIngest, lines)
	setOverviewPipeline(d.eventsPipeline, lines)
}

func (d *DashboardV2) seedOverviewPlaceholders() {
	setBoxText(d.overviewHdr, placeholderHeader)
	setBoxText(d.overviewMem, placeholderMem)
	setBoxText(d.overviewIngest, placeholderIngest)
	setBoxText(d.overviewPipeline, placeholderPipeline)
	setBoxText(d.overviewCaches, placeholderCaches)
	setBoxText(d.overviewPath, placeholderPath)
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

func setOverviewHeader(tv *tview.TextView, lines []string) {
	if len(lines) > 0 {
		setBoxText(tv, lines[0])
	}
}

func setOverviewIngest(tv *tview.TextView, lines []string) {
	section := overviewSectionLines(lines, "INGEST RATES (per min)", "PIPELINE QUALITY")
	if len(section) > 0 {
		setBoxText(tv, strings.Join(section, "\n"))
	}
}

func setOverviewPipeline(tv *tview.TextView, lines []string) {
	section := overviewSectionLines(lines, "PIPELINE QUALITY", "CACHES & DATA FRESHNESS")
	if len(section) > 0 {
		setBoxText(tv, strings.Join(section, "\n"))
	}
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
	lines := d.overviewSnapshotLines()
	switch page {
	case "overview":
		d.updateOverviewBoxes(lines)
		d.renderOverviewNetwork()
	case "ingest":
		d.updateIngestBoxes(lines)
	case "pipeline":
		d.updatePipelineBoxes(lines)
	case "events":
		d.updateEventsOverviewBoxes(lines)
	}
}

func (d *DashboardV2) overviewSnapshotLines() []string {
	if d == nil {
		return nil
	}
	if snap := d.snapshot.Load(); snap != nil && len(snap.OverviewLines) > 0 {
		return snap.OverviewLines
	}
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	return append([]string(nil), d.statsLines...)
}

func (d *DashboardV2) storeNetworkLines(lines []string) {
	if d == nil {
		return
	}
	d.networkMu.Lock()
	d.network = append(d.network[:0], lines...)
	d.networkMu.Unlock()
}

func (d *DashboardV2) networkLinesSnapshot() []string {
	if d == nil {
		return nil
	}
	d.networkMu.RLock()
	defer d.networkMu.RUnlock()
	if len(d.network) == 0 {
		return nil
	}
	out := make([]string, len(d.network))
	copy(out, d.network)
	return out
}

func (d *DashboardV2) renderOverviewNetwork() {
	if d == nil || d.overviewNetwork == nil {
		return
	}
	lines := d.networkLinesSnapshot()
	if len(lines) == 0 {
		return
	}
	d.overviewNetwork.SetText(padLines(strings.Join(lines, "\n")))
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
  Resolver C/P/U/S: Current resolver state counts (confident/probable/uncertain/split).
  Resolver Pressure:
    pressure: how often candidate/reporter caps were hit.
    evict: how many entries were evicted because of those caps.
    hw: highest candidate/reporter occupancy seen.
  Stabilizer H/I/D/S/O:
    H held for delay, I immediate release (correction-eligible modes only),
    D released after delay, S suppressed on timeout, O overflow fail-open release.
  Stabilizer Reason by-reason H/D/S:
    u unknown_or_nonrecent, a ambiguous_resolver,
    p p_low_confidence, e edit_neighbor_contested.
  Temporal:
    pending currently waiting, committed accepted by temporal decoder,
    fallback/abstain/bypass are non-commit outcomes,
    latency buckets show commit-delay distribution.
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

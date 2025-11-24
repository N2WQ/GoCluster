package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// dashboard renders the console layout when a compatible terminal is available.
// It shows stats plus three scrolling panes (calls, frequency adjustments,
// harmonic drops) and one system log pane.
type dashboard struct {
	app           *tview.Application
	statsView     *tview.TextView
	callView      *tview.TextView
	frequencyView *tview.TextView
	harmonicView  *tview.TextView
	systemView    *tview.TextView
	callLines     []string
	freqLines     []string
	harmonicLines []string
	systemLines   []string
	paneMu        sync.Mutex
	statsMu       sync.Mutex
	events        chan paneEvent
	closed        atomic.Bool
	ready         chan struct{}
}

const paneMaxLines = 6

type paneType int

const (
	paneCall paneType = iota
	paneFrequency
	paneHarmonic
	paneSystem
)

type paneEvent struct {
	pane paneType
	line string
}

func newDashboard(enable bool) *dashboard {
	if !enable {
		return nil
	}

	makePane := func(title string) *tview.TextView {
		tv := tview.NewTextView().
			SetDynamicColors(true).
			SetWrap(false)
		if title != "" {
			tv.SetTitle(title).SetTitleAlign(tview.AlignLeft)
		}
		return tv
	}

	stats := tview.NewTextView().SetDynamicColors(true).SetWrap(false)
	callPane := makePane("Corrected Calls")
	freqPane := makePane("Corrected Frequencies")
	harmonicPane := makePane("Harmonics")
	systemPane := makePane("System")

	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(stats, 7, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(callPane, 7, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(freqPane, 7, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(harmonicPane, 7, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(systemPane, 7, 0, false)

	app := tview.NewApplication().SetRoot(layout, true).EnableMouse(false)
	ready := make(chan struct{})
	var once sync.Once
	app.SetBeforeDrawFunc(func(screen tcell.Screen) bool {
		once.Do(func() { close(ready) })
		return false
	})
	d := &dashboard{
		app:           app,
		statsView:     stats,
		callView:      callPane,
		frequencyView: freqPane,
		harmonicView:  harmonicPane,
		systemView:    systemPane,
		events:        make(chan paneEvent, 256),
		ready:         ready,
	}

	// Dedicated flusher so the hot path can drop instead of blocking when the UI lags.
	go d.runEventLoop()

	go func() {
		if err := app.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "dashboard error: %v\n", err)
		}
	}()

	return d
}

func (d *dashboard) Stop() {
	if d == nil || d.app == nil {
		return
	}
	d.closed.Store(true)
	close(d.events)
	d.app.Stop()
}

func (d *dashboard) WaitReady() {
	if d == nil || d.ready == nil {
		return
	}
	<-d.ready
}

func (d *dashboard) SetStats(lines []string) {
	if d == nil {
		return
	}
	d.statsMu.Lock()
	text := strings.Join(lines, "\n")
	d.statsMu.Unlock()
	d.app.QueueUpdateDraw(func() {
		d.statsView.SetText(text)
	})
}

func (d *dashboard) AppendCall(line string) {
	d.enqueue(paneCall, line)
}

func (d *dashboard) AppendFrequency(line string) {
	d.enqueue(paneFrequency, line)
}

func (d *dashboard) AppendHarmonic(line string) {
	d.enqueue(paneHarmonic, line)
}

func (d *dashboard) AppendSystem(line string) {
	d.enqueue(paneSystem, line)
}

func (d *dashboard) enqueue(p paneType, line string) {
	if d == nil || d.closed.Load() {
		return
	}
	select {
	case d.events <- paneEvent{pane: p, line: line}:
	default:
		// Drop on saturation to keep the hot path non-blocking.
	}
}

func (d *dashboard) SystemWriter() *paneWriter {
	if d == nil {
		return nil
	}
	return &paneWriter{view: d.systemView, app: d.app}
}

type paneWriter struct {
	view *tview.TextView
	app  *tview.Application
}

func (w *paneWriter) Write(p []byte) (int, error) {
	if w == nil || w.view == nil {
		return len(p), nil
	}
	text := string(p)
	if w.app == nil {
		fmt.Fprint(w.view, text)
		return len(p), nil
	}
	w.app.QueueUpdateDraw(func() {
		fmt.Fprint(w.view, text)
		w.view.ScrollToEnd()
	})
	return len(p), nil
}

func (d *dashboard) runEventLoop() {
	if d == nil {
		return
	}
	for ev := range d.events {
		d.appendLine(ev.pane, ev.line)
	}
}

func (d *dashboard) appendLine(p paneType, line string) {
	if d == nil {
		return
	}
	tsLine := time.Now().Format("2006/01/02 15:04:05 ") + line

	d.paneMu.Lock()
	buf := d.getPaneBuffer(p)
	view := d.getPaneView(p)
	*buf = append(*buf, tsLine)
	if len(*buf) > paneMaxLines {
		*buf = (*buf)[len(*buf)-paneMaxLines:]
	}
	text := strings.Join(*buf, "\n")
	d.paneMu.Unlock()

	if view == nil || d.app == nil {
		return
	}
	d.app.QueueUpdateDraw(func() {
		view.SetText(text)
		view.ScrollToEnd()
	})
}

func (d *dashboard) getPaneBuffer(p paneType) *[]string {
	switch p {
	case paneCall:
		return &d.callLines
	case paneFrequency:
		return &d.freqLines
	case paneHarmonic:
		return &d.harmonicLines
	default:
		return &d.systemLines
	}
}

func (d *dashboard) getPaneView(p paneType) *tview.TextView {
	switch p {
	case paneCall:
		return d.callView
	case paneFrequency:
		return d.frequencyView
	case paneHarmonic:
		return d.harmonicView
	default:
		return d.systemView
	}
}

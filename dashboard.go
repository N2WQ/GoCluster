// Package main also hosts the optional terminal dashboard shown when a TTY is
// available; this file renders stats and event panes using tview/tcell.
package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const (
	// Limits to bound memory usage for TUI panes.
	statsMaxLines   = 200
	paneMaxLines    = 2000
	batchFlushEvery = 100 * time.Millisecond
	batchMaxLines   = 32
)

// uiSurface abstracts the dashboard/UI so alternative console renderers can plug in.
type uiSurface interface {
	WaitReady()
	Stop()
	SetStats(lines []string)
	AppendCall(line string)
	AppendUnlicensed(line string)
	AppendHarmonic(line string)
	AppendSystem(line string)
	SystemWriter() io.Writer
}

// dashboard renders the console layout when a compatible terminal is available.
// It shows stats plus four scrolling panes (call corrections, unlicensed drops,
// harmonic drops) and one system log pane.
type dashboard struct {
	app               *tview.Application
	statsView         *tview.TextView
	callView          *tview.TextView
	unlicensedView    *tview.TextView
	harmonicView      *tview.TextView
	systemView        *tview.TextView
	statsMu           sync.Mutex
	ready             chan struct{}
	callHasText       bool
	unlicensedHasText bool
	harmHasText       bool
	sysHasText        bool

	// Batching to reduce QueueUpdateDraw frequency.
	batchMu         sync.Mutex
	callBatch       []string
	unlicensedBatch []string
	harmBatch       []string
	sysBatch        []string
	batchTicker     *time.Ticker
	batchQuit       chan struct{}
	lastTSSecond    int64
	lastTSString    string
}

func newDashboard(enable bool) *dashboard {
	if !enable {
		return nil
	}

	makePane := func(title string) *tview.TextView {
		tv := tview.NewTextView().
			SetDynamicColors(true).
			SetWrap(false).
			SetMaxLines(paneMaxLines)
		if title != "" {
			tv.SetTitle(title).SetTitleAlign(tview.AlignLeft)
		}
		// Avoid SetChangedFunc+ScrollToEnd because it can create a change->scroll->change loop.
		// We scroll explicitly after appends inside the queued draw callbacks.
		return tv
	}

	stats := tview.NewTextView().SetDynamicColors(true).SetWrap(false).SetMaxLines(statsMaxLines)
	callPane := makePane("Corrected Calls")
	unlicensedPane := makePane("Unlicensed US Calls")
	harmonicPane := makePane("Harmonics")
	systemPane := makePane("System")

	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(stats, 8, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(callPane, 10, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(unlicensedPane, 10, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(harmonicPane, 10, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(systemPane, 10, 0, false)

	app := tview.NewApplication().SetRoot(layout, true).EnableMouse(false)
	ready := make(chan struct{})
	var once sync.Once
	app.SetBeforeDrawFunc(func(screen tcell.Screen) bool {
		once.Do(func() { close(ready) })
		return false
	})
	d := &dashboard{
		app:            app,
		statsView:      stats,
		callView:       callPane,
		unlicensedView: unlicensedPane,
		harmonicView:   harmonicPane,
		systemView:     systemPane,
		ready:          ready,
		batchTicker:    time.NewTicker(batchFlushEvery),
		batchQuit:      make(chan struct{}),
	}

	go d.flushLoop()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "dashboard panic: %v\n", r)
			}
		}()
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
	if d.batchTicker != nil {
		d.batchTicker.Stop()
	}
	if d.batchQuit != nil {
		close(d.batchQuit)
	}
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
		d.statsView.ScrollToEnd()
	})
}

func (d *dashboard) AppendCall(line string) {
	d.enqueue(&d.callBatch, line)
}

func (d *dashboard) AppendUnlicensed(line string) {
	d.enqueue(&d.unlicensedBatch, line)
}

func (d *dashboard) AppendHarmonic(line string) {
	d.enqueue(&d.harmBatch, line)
}

func (d *dashboard) AppendSystem(line string) {
	d.enqueue(&d.sysBatch, line)
}

func (d *dashboard) enqueue(batch *[]string, line string) {
	if d == nil || batch == nil {
		return
	}
	d.batchMu.Lock()
	*batch = append(*batch, line)
	callCount := len(d.callBatch) + len(d.unlicensedBatch) + len(d.harmBatch) + len(d.sysBatch)
	ready := callCount >= batchMaxLines
	d.batchMu.Unlock()
	if ready {
		d.flushBatches()
	}
}

func (d *dashboard) SystemWriter() io.Writer {
	if d == nil {
		return nil
	}
	return &paneWriter{dash: d}
}

type paneWriter struct {
	dash *dashboard
	buf  []byte
	mu   sync.Mutex
}

func (w *paneWriter) Write(p []byte) (int, error) {
	if w == nil || w.dash == nil {
		return len(p), nil
	}
	w.mu.Lock()
	w.buf = append(w.buf, p...)
	data := w.buf
	w.mu.Unlock()

	for {
		idx := bytes.IndexByte(data, '\n')
		if idx == -1 {
			break
		}
		line := string(bytes.TrimRight(data[:idx], "\r"))
		w.dash.enqueue(&w.dash.sysBatch, line)
		data = data[idx+1:]
	}
	// Keep any trailing partial line in the buffer.
	w.mu.Lock()
	w.buf = data
	w.mu.Unlock()
	return len(p), nil
}

func (d *dashboard) flushLoop() {
	if d == nil || d.batchTicker == nil || d.batchQuit == nil {
		return
	}
	for {
		select {
		case <-d.batchTicker.C:
			d.flushBatches()
		case <-d.batchQuit:
			return
		}
	}
}

func (d *dashboard) flushBatches() {
	if d == nil || d.app == nil {
		return
	}
	d.batchMu.Lock()
	callBatch := d.callBatch
	unlBatch := d.unlicensedBatch
	harmBatch := d.harmBatch
	sysBatch := d.sysBatch
	d.callBatch = d.callBatch[:0]
	d.unlicensedBatch = d.unlicensedBatch[:0]
	d.harmBatch = d.harmBatch[:0]
	d.sysBatch = d.sysBatch[:0]
	now := time.Now()
	sec := now.Unix()
	if sec != d.lastTSSecond {
		d.lastTSSecond = sec
		d.lastTSString = now.Format("01-02-2006 15:04:05 ")
	}
	ts := d.lastTSString
	d.batchMu.Unlock()

	total := len(callBatch) + len(unlBatch) + len(harmBatch) + len(sysBatch)
	if total == 0 {
		return
	}

	d.app.QueueUpdateDraw(func() {
		if len(callBatch) > 0 {
			for _, line := range callBatch {
				if d.callHasText {
					fmt.Fprint(d.callView, "\n")
				}
				fmt.Fprint(d.callView, ts+line)
				d.callHasText = true
			}
			d.callView.ScrollToEnd()
		}
		if len(unlBatch) > 0 {
			for _, line := range unlBatch {
				if d.unlicensedHasText {
					fmt.Fprint(d.unlicensedView, "\n")
				}
				fmt.Fprint(d.unlicensedView, ts+line)
				d.unlicensedHasText = true
			}
			d.unlicensedView.ScrollToEnd()
		}
		if len(harmBatch) > 0 {
			for _, line := range harmBatch {
				if d.harmHasText {
					fmt.Fprint(d.harmonicView, "\n")
				}
				fmt.Fprint(d.harmonicView, ts+line)
				d.harmHasText = true
			}
			d.harmonicView.ScrollToEnd()
		}
		if len(sysBatch) > 0 {
			for _, line := range sysBatch {
				if d.sysHasText {
					fmt.Fprint(d.systemView, "\n")
				}
				fmt.Fprint(d.systemView, ts+line)
				d.sysHasText = true
			}
			d.systemView.ScrollToEnd()
		}
	})
}

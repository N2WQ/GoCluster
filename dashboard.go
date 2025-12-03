// Package main also hosts the optional terminal dashboard shown when a TTY is
// available; this file renders stats and event panes using tview/tcell.
package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// dashboard renders the console layout when a compatible terminal is available.
// It shows stats plus four scrolling panes (call corrections, unlicensed drops,
// frequency adjustments, harmonic drops) and one system log pane.
type dashboard struct {
	app               *tview.Application
	statsView         *tview.TextView
	callView          *tview.TextView
	unlicensedView    *tview.TextView
	frequencyView     *tview.TextView
	harmonicView      *tview.TextView
	systemView        *tview.TextView
	statsMu           sync.Mutex
	ready             chan struct{}
	callHasText       bool
	unlicensedHasText bool
	freqHasText       bool
	harmHasText       bool
	sysHasText        bool
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
		tv.SetChangedFunc(func() {
			tv.ScrollToEnd()
		})
		return tv
	}

	stats := tview.NewTextView().SetDynamicColors(true).SetWrap(false)
	callPane := makePane("Corrected Calls")
	unlicensedPane := makePane("Unlicensed US Calls")
	freqPane := makePane("Corrected Frequencies")
	harmonicPane := makePane("Harmonics")
	systemPane := makePane("System")

	layout := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(stats, 7, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(callPane, 7, 0, false).
		AddItem(tview.NewBox(), 1, 0, false).
		AddItem(unlicensedPane, 7, 0, false).
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
		app:            app,
		statsView:      stats,
		callView:       callPane,
		unlicensedView: unlicensedPane,
		frequencyView:  freqPane,
		harmonicView:   harmonicPane,
		systemView:     systemPane,
		ready:          ready,
	}

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
	d.appendLine(d.callView, &d.callHasText, line)
}

func (d *dashboard) AppendUnlicensed(line string) {
	d.appendLine(d.unlicensedView, &d.unlicensedHasText, line)
}

func (d *dashboard) AppendFrequency(line string) {
	d.appendLine(d.frequencyView, &d.freqHasText, line)
}

func (d *dashboard) AppendHarmonic(line string) {
	d.appendLine(d.harmonicView, &d.harmHasText, line)
}

func (d *dashboard) AppendSystem(line string) {
	d.appendLine(d.systemView, &d.sysHasText, line)
}

func (d *dashboard) appendLine(view *tview.TextView, hasText *bool, line string) {
	if d == nil || view == nil {
		return
	}
	// Dashboard timestamps use MM-DD-YYYY HH:MM:SS for readability.
	ts := time.Now().Format("01-02-2006 15:04:05 ")
	d.app.QueueUpdateDraw(func() {
		if hasText != nil && *hasText {
			fmt.Fprint(view, "\n")
		}
		fmt.Fprint(view, ts+line)
		if hasText != nil {
			*hasText = true
		}
	})
}

func (d *dashboard) SystemWriter() *paneWriter {
	if d == nil {
		return nil
	}
	return &paneWriter{view: d.systemView, app: d.app, hasText: &d.sysHasText}
}

type paneWriter struct {
	view *tview.TextView
	app  *tview.Application
	// hasText tracks whether we've already written at least one line so we can
	// avoid leading/trailing blank rows when appending log messages.
	hasText *bool
}

func (w *paneWriter) Write(p []byte) (int, error) {
	if w == nil || w.view == nil {
		return len(p), nil
	}
	// Trim trailing newlines so the TextView doesn't keep an empty row at the end.
	text := strings.TrimRight(string(p), "\n")
	if text == "" {
		return len(p), nil
	}
	lines := strings.Split(text, "\n")
	appendLines := func() {
		ts := time.Now().Format("01-02-2006 15:04:05 ")
		for _, line := range lines {
			if w.hasText != nil && *w.hasText {
				fmt.Fprint(w.view, "\n")
			}
			fmt.Fprint(w.view, ts+line)
			if w.hasText != nil {
				*w.hasText = true
			}
		}
	}
	if w.app == nil {
		appendLines()
		return len(p), nil
	}
	w.app.QueueUpdateDraw(appendLines)
	return len(p), nil
}

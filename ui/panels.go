package ui

import (
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// focusable abstracts a focusable primitive with optional scroll handling.
type focusable interface {
	Primitive() tview.Primitive
	SetFocused(focused bool)
	HandleScroll(event *tcell.EventKey) bool
}

// focusBox wraps a boxed TextView with focus styling metadata.
type focusBox struct {
	tv         *tview.TextView
	baseTitle  string
	scrollable bool
}

func newFocusBox(tv *tview.TextView, baseTitle string, scrollable bool) *focusBox {
	return &focusBox{
		tv:         tv,
		baseTitle:  baseTitle,
		scrollable: scrollable,
	}
}

func (b *focusBox) Primitive() tview.Primitive {
	if b == nil {
		return nil
	}
	return b.tv
}

func (b *focusBox) SetFocused(focused bool) {
	if b == nil {
		return
	}
	applyFocusStyle(b.tv, b.baseTitle, focused)
}

func (b *focusBox) HandleScroll(event *tcell.EventKey) bool {
	if b == nil || !b.scrollable {
		return false
	}
	return scrollTextView(b.tv, event)
}

// focusGroup manages focus cycling and scroll handling for a set of panes.
type focusGroup struct {
	items []focusable
	index int
}

func newFocusGroup(items ...focusable) focusGroup {
	filtered := make([]focusable, 0, len(items))
	for _, item := range items {
		if item == nil || item.Primitive() == nil {
			continue
		}
		filtered = append(filtered, item)
	}
	return focusGroup{items: filtered}
}

func (g *focusGroup) set(app *tview.Application, idx int) {
	if g == nil || len(g.items) == 0 {
		return
	}
	if idx < 0 || idx >= len(g.items) {
		idx = 0
	}
	g.index = idx
	for i, item := range g.items {
		item.SetFocused(i == idx)
	}
	if app != nil {
		app.SetFocus(g.items[idx].Primitive())
	}
}

func (g *focusGroup) cycle(app *tview.Application, delta int) {
	if g == nil || len(g.items) == 0 {
		return
	}
	next := g.index + delta
	if next < 0 {
		next = len(g.items) - 1
	} else if next >= len(g.items) {
		next = 0
	}
	g.set(app, next)
}

func (g *focusGroup) handleScroll(app *tview.Application, event *tcell.EventKey) bool {
	if g == nil || app == nil || event == nil {
		return false
	}
	focused := app.GetFocus()
	for _, item := range g.items {
		if item.Primitive() == focused {
			return item.HandleScroll(event)
		}
	}
	return false
}

// streamPanel is a focused, bounded event panel backed by virtualized rendering.
type streamPanel struct {
	view *virtualLogView
}

type streamPanelOptions struct {
	MaxEvents        int
	MaxBytes         int
	MaxMessageBytes  int
	EvictOnByteLimit bool
}

func newStreamPanel(baseTitle string, max int, dynamicColors bool) *streamPanel {
	return &streamPanel{view: newVirtualLogView(baseTitle, max, dynamicColors)}
}

func newStreamPanelWithOptions(baseTitle string, opts streamPanelOptions, dynamicColors bool) *streamPanel {
	return &streamPanel{view: newVirtualLogViewWithOptions(baseTitle, virtualLogOptions{
		MaxLines:         opts.MaxEvents,
		MaxBytes:         opts.MaxBytes,
		MaxMessageBytes:  opts.MaxMessageBytes,
		EvictOnByteLimit: opts.EvictOnByteLimit,
	}, dynamicColors)}
}

func (p *streamPanel) Primitive() tview.Primitive {
	if p == nil {
		return nil
	}
	return p.view
}

func (p *streamPanel) SetFocused(focused bool) {
	if p == nil || p.view == nil {
		return
	}
	p.view.SetFocused(focused)
}

func (p *streamPanel) HandleScroll(event *tcell.EventKey) bool {
	if p == nil || p.view == nil {
		return false
	}
	return p.view.HandleScroll(event)
}

func (p *streamPanel) Append(line string) {
	if p == nil || p.view == nil {
		return
	}
	p.view.Append(line)
}

// Render keeps the existing scheduler callback contract. Draw work is done by
// the primitive itself during the next QueueUpdateDraw frame.
func (p *streamPanel) Render(_ *tview.Application) {
}

func (p *streamPanel) SetText(text string) {
	if p == nil || p.view == nil {
		return
	}
	if text == "" {
		p.view.Reset(nil)
		return
	}
	p.view.Reset(strings.Split(text, "\n"))
}

func (p *streamPanel) SnapshotText() string {
	if p == nil || p.view == nil {
		return ""
	}
	return p.view.SnapshotText()
}

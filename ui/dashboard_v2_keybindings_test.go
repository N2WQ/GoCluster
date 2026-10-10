package ui

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type dashboardKeyFixture struct {
	dashboard *DashboardV2
	groups    map[string]*focusGroup
}

// Install the production capture/root with actual pane types and counts, but
// start no terminal, scheduler or application goroutine. Each test owns its
// context and invokes events on one goroutine, like the UI input owner.
func newDashboardKeyFixture(t *testing.T) dashboardKeyFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	d := &DashboardV2{
		app: tview.NewApplication(), pages: tview.NewPages(), ctx: ctx, cancel: cancel,
		pageOrder:   []string{"overview", "ingest", "pipeline", "events"},
		pagePresent: make(map[string]bool),
	}
	d.snapshot.Store(&dashboardSnapshot{HasContent: true})
	rows := make([]string, 100)
	for i := range rows {
		rows[i] = fmt.Sprintf("row%03d", i)
	}
	boxes := make([]focusable, 2)
	for i := range boxes {
		tv := newBoxedTextView("overview").SetScrollable(true).SetWrap(false)
		tv.SetText(strings.Join(rows, "\n"))
		boxes[i] = newFocusBox(tv, "overview", true)
	}
	d.overviewGroup = newFocusGroup(boxes...)
	newPanel := func() *streamPanel {
		panel := newStreamPanel("stream", 200, false)
		panel.view.Reset(rows)
		return panel
	}
	d.ingestGroup = newFocusGroup(newPanel(), newPanel())
	d.pipelineGroup = newFocusGroup(newPanel(), newPanel())
	d.eventsGroup = newFocusGroup(newPanel())
	fixture := dashboardKeyFixture{dashboard: d, groups: map[string]*focusGroup{
		"overview": &d.overviewGroup, "ingest": &d.ingestGroup,
		"pipeline": &d.pipelineGroup, "events": &d.eventsGroup,
	}}
	for _, page := range d.pageOrder {
		root := tview.NewFlex()
		for _, pane := range fixture.groups[page].items {
			root.AddItem(pane.Primitive(), 0, 1, true)
		}
		d.addPage(page, root)
	}
	d.addPage("help", buildHelpOverlay())
	d.installRoot()
	d.installKeybindings()
	return fixture
}

func requireDashboardKeyConsumed(t *testing.T, d *DashboardV2, key tcell.Key, value rune) {
	t.Helper()
	if capture := d.app.GetInputCapture(); capture == nil || capture(tcell.NewEventKey(key, value, tcell.ModNone)) != nil {
		t.Fatalf("dashboard did not consume key %v/rune %q", key, value)
	}
}

func requireDashboardFrontPage(t *testing.T, d *DashboardV2, want string, help bool) {
	t.Helper()
	page, primitive := d.pages.GetFrontPage()
	if page != want || primitive == nil || d.helpShown != help {
		t.Fatalf("front=%q/%T help=%t, want %q/help=%t", page, primitive, d.helpShown, want, help)
	}
}

func TestDashboardKeyHelpAliases(t *testing.T) {
	closeKeys := []struct {
		key   tcell.Key
		value rune
	}{{tcell.KeyRune, 'H'}, {tcell.KeyRune, 'h'}, {tcell.KeyRune, '?'}, {tcell.KeyEsc, 0}}
	for _, page := range []string{"overview", "ingest", "pipeline", "events"} {
		for _, open := range []rune{'H', 'h', '?'} {
			for _, closeKey := range closeKeys {
				t.Run(fmt.Sprintf("%s/%c/%d-%c", page, open, closeKey.key, closeKey.value), func(t *testing.T) {
					fixture := newDashboardKeyFixture(t)
					d := fixture.dashboard
					d.showPage(page)
					focus := d.app.GetFocus()
					for range 2 {
						requireDashboardKeyConsumed(t, d, tcell.KeyRune, open)
						requireDashboardFrontPage(t, d, "help", true)
						requireDashboardKeyConsumed(t, d, closeKey.key, closeKey.value)
						requireDashboardFrontPage(t, d, page, false)
						if d.app.GetFocus() != focus {
							t.Fatal("help changed the underlying pane focus")
						}
					}
				})
			}
		}
	}
}

func TestDashboardKeyNavigationAndPaneCycling(t *testing.T) {
	fixture := newDashboardKeyFixture(t)
	d := fixture.dashboard
	for _, target := range []struct {
		keys string
		page string
	}{{"eE", "events"}, {"iI", "ingest"}, {"oO", "overview"}, {"pP", "pipeline"}} {
		group := fixture.groups[target.page]
		for _, key := range target.keys {
			// A different page first prevents a swallowed key from looking green.
			if target.page == "overview" {
				d.showPage("events")
			} else {
				d.showPage("overview")
			}
			requireDashboardKeyConsumed(t, d, tcell.KeyRune, key)
			requireDashboardFrontPage(t, d, target.page, false)
			if d.app.GetFocus() != group.items[0].Primitive() {
				t.Fatalf("%c did not focus the first %s pane", key, target.page)
			}
			for _, direction := range []tcell.Key{tcell.KeyTab, tcell.KeyBacktab} {
				for step := 1; step <= len(group.items); step++ {
					requireDashboardKeyConsumed(t, d, direction, 0)
					want := step % len(group.items) // All production groups have one or two panes.
					if d.app.GetFocus() != group.items[want].Primitive() {
						t.Fatalf("%s key %v step %d did not cycle to pane %d", target.page, direction, step, want)
					}
				}
			}
		}
	}
}

func TestDashboardKeyScrolling(t *testing.T) {
	fixture := newDashboardKeyFixture(t)
	d := fixture.dashboard
	screen := newSimulationScreen(t, 80, 12)
	for _, page := range d.pageOrder {
		d.showPage(page)
		pane := fixture.groups[page].items[0]
		pane.Primitive().SetRect(0, 0, 80, 12) // Border leaves ten rows: a fast scroll is nine.
		for _, test := range []struct {
			key   tcell.Key
			value rune
			want  int
		}{{tcell.KeyUp, 0, 19}, {tcell.KeyDown, 0, 21}, {tcell.KeyRune, 'k', 19},
			{tcell.KeyRune, 'j', 21}, {tcell.KeyPgUp, 0, 11}, {tcell.KeyPgDn, 0, 29},
			{tcell.KeyHome, 0, 0}, {tcell.KeyEnd, 0, 90}} {
			setDashboardPaneOffset(pane, 20)
			requireDashboardKeyConsumed(t, d, test.key, test.value)
			pane.Primitive().Draw(screen) // Resolve TextView's bottom sentinel through actual rendering.
			if got := dashboardPaneOffset(pane); got != test.want {
				t.Fatalf("%s key %v/%q offset=%d, want %d", page, test.key, test.value, got, test.want)
			}
		}
	}
}

func setDashboardPaneOffset(pane focusable, offset int) {
	switch value := pane.(type) {
	case *focusBox:
		value.tv.ScrollTo(offset, 0)
	case *streamPanel:
		value.view.mu.Lock()
		value.view.offset, value.view.follow = offset, false
		value.view.mu.Unlock()
	}
}

func dashboardPaneOffset(pane focusable) int {
	switch value := pane.(type) {
	case *focusBox:
		row, _ := value.tv.GetScrollOffset()
		return row
	case *streamPanel:
		value.view.mu.Lock()
		defer value.view.mu.Unlock()
		return value.view.offset
	default:
		return -1
	}
}

func TestDashboardKeyQuitAndPassThrough(t *testing.T) {
	for _, key := range []struct {
		key   tcell.Key
		value rune
	}{{tcell.KeyRune, 'q'}, {tcell.KeyRune, 'Q'}, {tcell.KeyCtrlC, 0}} {
		for _, help := range []bool{false, true} {
			d := newDashboardKeyFixture(t).dashboard
			if help {
				requireDashboardKeyConsumed(t, d, tcell.KeyRune, 'h')
			}
			unknown := tcell.NewEventKey(tcell.KeyRune, 'x', tcell.ModNone)
			if d.app.GetInputCapture()(unknown) != unknown || d.ctx.Err() != nil || d.helpShown != help {
				t.Fatal("unknown input was consumed/replaced or changed the dashboard")
			}
			requireDashboardKeyConsumed(t, d, key.key, key.value)
			if d.ctx.Err() != context.Canceled {
				t.Fatalf("quit key did not cancel the owned dashboard: %v", d.ctx.Err())
			}
		}
	}
}

func TestDashboardKeyNonScrollablePanePassThrough(t *testing.T) {
	fixture := newDashboardKeyFixture(t)
	d := fixture.dashboard
	tv := fixture.groups["overview"].items[0].(*focusBox).tv
	tv.ScrollTo(12, 0)
	// The TextView itself can scroll. Disable only the focus metadata, so an
	// accidentally forwarded scroll would move it and falsify this control.
	fixture.groups["overview"].items[0] = newFocusBox(tv, "static", false)
	fixture.groups["overview"].set(d.app, 0)
	event := tcell.NewEventKey(tcell.KeyDown, 0, tcell.ModNone)
	if d.app.GetInputCapture()(event) != event {
		t.Fatal("unhandled scroll was consumed or replaced")
	}
	if row, col := tv.GetScrollOffset(); row != 12 || col != 0 {
		t.Fatalf("non-scrollable pane moved to %d/%d", row, col)
	}
}

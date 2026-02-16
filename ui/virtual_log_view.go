package ui

import (
	"strconv"
	"strings"
	"sync"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// virtualLogView is a bounded, virtualized scroll view for hot event streams.
// It keeps a ring of lines and renders only rows currently visible on screen.
// Concurrency: Append/Reset may be called from non-UI goroutines.
// Draw/HandleScroll run on the UI goroutine.
type virtualLogView struct {
	*tview.Box

	mu    sync.Mutex
	lines []string
	head  int
	count int
	max   int
	total uint64

	offset int
	follow bool

	dynamicColors bool
	baseTitle     string

	renderRows []string

	cachedOverflowCount int
	cachedOverflowText  string
}

func newVirtualLogView(title string, max int, dynamicColors bool) *virtualLogView {
	if max <= 0 {
		max = 1
	}
	v := &virtualLogView{
		Box:                 tview.NewBox().SetBorder(true),
		lines:               make([]string, max),
		max:                 max,
		follow:              true,
		dynamicColors:       dynamicColors,
		baseTitle:           title,
		cachedOverflowCount: -1,
	}
	applyFocusBoxStyle(v.Box, title, false)
	return v
}

func (v *virtualLogView) SetFocused(focused bool) {
	if v == nil {
		return
	}
	applyFocusBoxStyle(v.Box, v.baseTitle, focused)
}

func (v *virtualLogView) Append(line string) {
	if v == nil || v.max <= 0 {
		return
	}
	v.mu.Lock()
	if v.count < v.max {
		pos := (v.head + v.count) % v.max
		v.lines[pos] = line
		v.count++
	} else {
		v.lines[v.head] = line
		v.head = (v.head + 1) % v.max
	}
	v.total++
	v.mu.Unlock()
}

func (v *virtualLogView) Reset(lines []string) {
	if v == nil {
		return
	}
	v.mu.Lock()
	for i := range v.lines {
		v.lines[i] = ""
	}
	v.head = 0
	v.count = 0
	v.total = 0
	v.offset = 0
	v.follow = true
	v.cachedOverflowCount = -1
	v.cachedOverflowText = ""
	if len(lines) > 0 {
		start := 0
		if len(lines) > v.max {
			start = len(lines) - v.max
		}
		for _, line := range lines[start:] {
			v.lines[v.count] = line
			v.count++
			v.total++
		}
	}
	v.mu.Unlock()
}

func (v *virtualLogView) Draw(screen tcell.Screen) {
	if v == nil {
		return
	}
	v.DrawForSubclass(screen, v)

	x, y, width, height := v.GetInnerRect()
	if width <= 0 || height <= 0 {
		return
	}

	v.mu.Lock()
	rows := v.visibleRowsLocked(height)
	dynamicColors := v.dynamicColors
	v.mu.Unlock()

	for i, row := range rows {
		yPos := y + i
		if dynamicColors {
			tview.Print(screen, " "+row, x, yPos, width, tview.AlignLeft, tcell.ColorWhite)
			continue
		}
		drawPlainLine(screen, x, yPos, width, row, v.GetBackgroundColor())
	}
}

func (v *virtualLogView) HandleScroll(event *tcell.EventKey) bool {
	if v == nil || event == nil {
		return false
	}

	_, _, _, height := v.GetInnerRect()
	if height < 1 {
		height = 1
	}
	page := height - 1
	if page < 1 {
		page = 1
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	totalRows := v.count
	if int(v.total) > v.count {
		totalRows++
	}
	maxOffset := totalRows - height
	if maxOffset < 0 {
		maxOffset = 0
	}

	next := v.offset
	handled := true

	switch event.Key() {
	case tcell.KeyUp:
		next--
		v.follow = false
	case tcell.KeyDown:
		next++
	case tcell.KeyPgUp:
		next -= page
		v.follow = false
	case tcell.KeyPgDn:
		next += page
	case tcell.KeyHome:
		next = 0
		v.follow = false
	case tcell.KeyEnd:
		next = maxOffset
		v.follow = true
	case tcell.KeyRune:
		switch event.Rune() {
		case 'k':
			next--
			v.follow = false
		case 'j':
			next++
		default:
			handled = false
		}
	default:
		handled = false
	}

	if !handled {
		return false
	}

	if next < 0 {
		next = 0
	}
	if next > maxOffset {
		next = maxOffset
	}
	if event.Key() != tcell.KeyEnd {
		v.follow = next == maxOffset
	}
	v.offset = next
	return true
}

func (v *virtualLogView) SnapshotText() string {
	if v == nil {
		return ""
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	rows := v.allRowsLocked()
	return strings.Join(rows, "\n")
}

func (v *virtualLogView) visibleRowsLocked(height int) []string {
	totalRows := v.count
	overflow := int(v.total) - v.count
	if overflow > 0 {
		totalRows++
	}
	maxOffset := totalRows - height
	if maxOffset < 0 {
		maxOffset = 0
	}
	if v.follow || !v.HasFocus() {
		v.offset = maxOffset
	}
	if v.offset < 0 {
		v.offset = 0
	}
	if v.offset > maxOffset {
		v.offset = maxOffset
	}

	start := v.offset
	end := start + height
	if end > totalRows {
		end = totalRows
	}
	needed := end - start
	if needed < 0 {
		needed = 0
	}

	if cap(v.renderRows) < needed {
		v.renderRows = make([]string, needed)
	} else {
		v.renderRows = v.renderRows[:needed]
	}

	for i := 0; i < needed; i++ {
		row := start + i
		if row < v.count {
			idx := (v.head + row) % v.max
			v.renderRows[i] = v.lines[idx]
			continue
		}
		v.renderRows[i] = v.overflowLineLocked(overflow)
	}
	return v.renderRows
}

func (v *virtualLogView) allRowsLocked() []string {
	rows := make([]string, 0, v.count+1)
	for i := 0; i < v.count; i++ {
		idx := (v.head + i) % v.max
		rows = append(rows, v.lines[idx])
	}
	overflow := int(v.total) - v.count
	if overflow > 0 {
		rows = append(rows, v.overflowLineLocked(overflow))
	}
	return rows
}

func (v *virtualLogView) overflowLineLocked(overflow int) string {
	if overflow == v.cachedOverflowCount {
		return v.cachedOverflowText
	}
	var buf [32]byte
	b := buf[:0]
	b = append(b, '.', '.', '.', ' ', '+')
	b = strconv.AppendInt(b, int64(overflow), 10)
	b = append(b, ' ', 'm', 'o', 'r', 'e')
	v.cachedOverflowCount = overflow
	v.cachedOverflowText = string(b)
	return v.cachedOverflowText
}

func drawPlainLine(screen tcell.Screen, x, y, width int, text string, bg tcell.Color) {
	if width <= 0 {
		return
	}
	style := tcell.StyleDefault.Foreground(tcell.ColorWhite).Background(bg)

	col := 0
	screen.SetContent(x+col, y, ' ', nil, style)
	col++
	for _, r := range text {
		if col >= width {
			return
		}
		if r == '\n' || r == '\r' {
			return
		}
		if r == '\t' {
			r = ' '
		}
		screen.SetContent(x+col, y, r, nil, style)
		col++
	}
}

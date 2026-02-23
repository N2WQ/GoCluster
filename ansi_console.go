package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"dxcluster/config"
	"dxcluster/internal/linebuffer"
	"dxcluster/ui"

	"golang.org/x/term"
)

const (
	ansiWidth      = 90
	ansiStatsLines = 12
	ansiPaneLines  = 10
	ansiPaneCount  = 5
	ansiGapLines   = 1
	ansiTotalRows  = ansiStatsLines + ansiGapLines + (1 + ansiPaneLines) + (ansiPaneCount-1)*(1+1+ansiPaneLines)
	ansiMinRefresh = 16 * time.Millisecond
)

// ansiConsole is a fixed-layout ANSI renderer for the local console.
// It is selected solely via ui.mode=ansi in the YAML config.
type ansiConsole struct {
	mu      sync.Mutex
	stats   []string
	dropped ringPane
	calls   ringPane
	unlic   ringPane
	harm    ringPane
	system  ringPane
	refresh time.Duration
	quit    chan struct{}
	writer  *ansiWriter
	isTTY   bool
	color   bool

	renderCh chan struct{}
	stopOnce sync.Once
	disabled atomic.Bool

	snapStats   []string
	snapDropped []string
	snapCalls   []string
	snapUnlic   []string
	snapHarm    []string
	snapSys     []string
	frame       []string
	lastFrame   []string
	blankLine   string
	headerDrop  string
	headerCall  string
	headerUnlic string
	headerHarm  string
	headerSys   string
	lastWidth   int
	lastHeight  int
}

type ringPane struct {
	lines []string
	idx   int
	count int
}

// Purpose: Construct the ANSI console renderer when UI output is allowed.
// Key aspects: Fixed layout, size check, and event-driven render loop.
// Upstream: main UI selection based on config.
// Downstream: ansiWriter and refreshLoop goroutine.
func newANSIConsole(uiCfg config.UIConfig, allowRender bool) ui.Surface {
	if !allowRender {
		return nil
	}

	refresh := time.Duration(uiCfg.RefreshMS) * time.Millisecond
	if refresh < 0 {
		refresh = 0
	}
	if refresh > 0 && refresh < ansiMinRefresh {
		log.Printf("UI: clamping refresh interval to %dms (requested %dms too low)", ansiMinRefresh/time.Millisecond, refresh/time.Millisecond)
		refresh = ansiMinRefresh
	}

	width, height, ok := termSize()
	if !ok {
		log.Printf("UI: ANSI disabled (terminal size unavailable)")
		return nil
	}
	if width < ansiWidth || height < ansiTotalRows {
		log.Printf("UI: ANSI disabled (terminal too small: %dx%d, need %dx%d)", width, height, ansiWidth, ansiTotalRows)
		return nil
	}

	c := &ansiConsole{
		stats:       make([]string, ansiStatsLines),
		dropped:     ringPane{lines: make([]string, ansiPaneLines)},
		calls:       ringPane{lines: make([]string, ansiPaneLines)},
		unlic:       ringPane{lines: make([]string, ansiPaneLines)},
		harm:        ringPane{lines: make([]string, ansiPaneLines)},
		system:      ringPane{lines: make([]string, ansiPaneLines)},
		refresh:     refresh,
		quit:        make(chan struct{}),
		isTTY:       true, // caller only constructs when rendering is permitted
		color:       uiCfg.Color,
		renderCh:    make(chan struct{}, 1),
		snapStats:   make([]string, ansiStatsLines),
		snapDropped: make([]string, ansiPaneLines),
		snapCalls:   make([]string, ansiPaneLines),
		snapUnlic:   make([]string, ansiPaneLines),
		snapHarm:    make([]string, ansiPaneLines),
		snapSys:     make([]string, ansiPaneLines),
		frame:       make([]string, ansiTotalRows),
		lastFrame:   make([]string, ansiTotalRows),
		lastWidth:   width,
		lastHeight:  height,
	}

	c.blankLine = formatLine("", ansiWidth, false)
	c.headerDrop = formatLine("<<<<<<<<<< Dropped >>>>>>>>>>", ansiWidth, false)
	c.headerCall = formatLine("<<<<<<<<<< Corrected >>>>>>>>>>", ansiWidth, false)
	c.headerUnlic = formatLine("<<<<<<<<<< Unlicensed >>>>>>>>>>", ansiWidth, false)
	c.headerHarm = formatLine("<<<<<<<<<< Harmonics >>>>>>>>>>", ansiWidth, false)
	c.headerSys = formatLine("<<<<<<<<<< System Log >>>>>>>>>>", ansiWidth, false)
	for i := range c.stats {
		c.stats[i] = c.blankLine
	}

	c.writer = &ansiWriter{append: c.AppendSystem}

	// Only render when a TTY is present.
	if c.isTTY {
		// Purpose: Run periodic ANSI renders on the configured cadence.
		// Key aspects: Detached goroutine; exits when Stop closes quit.
		// Upstream: newANSIConsole.
		// Downstream: refreshLoop.
		go c.refreshLoop()
		c.signalRender()
	}

	return c
}

// WaitReady satisfies ui.Surface readiness contract for ANSI consoles.
// Key aspects: No-op because ANSI renderer has no async initialization.
// Upstream: main UI setup.
// Downstream: None.
func (c *ansiConsole) WaitReady() {}

// SetSnapshot satisfies ui.Surface snapshot contract (ANSI ignores structured snapshots).
// Key aspects: No-op for ANSI renderer.
// Upstream: main stats loop.
// Downstream: None.
func (c *ansiConsole) SetSnapshot(_ ui.Snapshot) {}

// UpdateNetworkStatus satisfies ui.Surface network update contract (ANSI ignores live updates).
// Key aspects: No-op for ANSI renderer.
// Upstream: telnet client change notifier.
// Downstream: None.
func (c *ansiConsole) UpdateNetworkStatus(summaryLine string, clientLines []string) {}

// Stop stops the ANSI console render loop.
// Key aspects: Ensures quit is closed once.
// Upstream: main shutdown path.
// Downstream: None (channel close only).
func (c *ansiConsole) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		close(c.quit)
	})
}

// SetStats replaces the current stats pane contents.
// Key aspects: Bounds copy to fixed pane size and pads blank lines.
// Upstream: stats ticker in main.
// Downstream: formatLine and render signal.
func (c *ansiConsole) SetStats(lines []string) {
	if c == nil {
		return
	}
	if c.disabled.Load() {
		return
	}
	c.mu.Lock()
	limit := len(lines)
	if limit > len(c.stats) {
		limit = len(c.stats)
	}
	for i := 0; i < limit; i++ {
		c.stats[i] = formatLine(lines[i], ansiWidth, c.color)
	}
	for i := limit; i < len(c.stats); i++ {
		c.stats[i] = c.blankLine
	}
	c.mu.Unlock()
	c.signalRender()
}

// AppendDropped appends a drop line to the dropped pane.
// Key aspects: Delegates to the shared ring-buffer append logic.
// Upstream: drop reporters.
// Downstream: c.append.
func (c *ansiConsole) AppendDropped(line string) { c.append(&c.dropped, line) }

// AppendCall appends a call-correction line to the calls pane.
// Key aspects: Delegates to the shared ring-buffer append logic.
// Upstream: dashboard/system log writers.
// Downstream: c.append.
func (c *ansiConsole) AppendCall(line string) { c.append(&c.calls, line) }

// AppendUnlicensed appends an unlicensed call line to the unlicensed pane.
// Key aspects: Delegates to the shared ring-buffer append logic.
// Upstream: unlicensed reporter path.
// Downstream: c.append.
func (c *ansiConsole) AppendUnlicensed(line string) { c.append(&c.unlic, line) }

// AppendHarmonic appends a harmonic suppression line to the harmonic pane.
// Key aspects: Delegates to the shared ring-buffer append logic.
// Upstream: harmonic suppression path.
// Downstream: c.append.
func (c *ansiConsole) AppendHarmonic(line string) { c.append(&c.harm, line) }

// AppendReputation appends a reputation drop line to the dropped pane.
// Key aspects: Routes reputation drops into the dropped pane for ANSI UI.
// Upstream: reputation gate path.
// Downstream: c.append.
func (c *ansiConsole) AppendReputation(line string) { c.append(&c.dropped, line) }

// AppendSystem appends a system log line to the system pane.
// Key aspects: Delegates to the shared ring-buffer append logic.
// Upstream: log routing for UI mode.
// Downstream: c.append.
func (c *ansiConsole) AppendSystem(line string) { c.append(&c.system, line) }

// SystemWriter provides an io.Writer for system log output.
// Key aspects: Returns the ANSI writer wrapper or nil when inactive.
// Upstream: UI logging setup in main.
// Downstream: None (returns existing writer).
func (c *ansiConsole) SystemWriter() io.Writer {
	if c == nil {
		return nil
	}
	return c.writer
}

// Purpose: Append a line to a specific ring pane.
// Key aspects: Applies markup, wraps index, and caps count.
// Upstream: AppendDropped/AppendCall/AppendUnlicensed/AppendHarmonic/AppendSystem.
// Downstream: formatLine and render signal.
func (c *ansiConsole) append(pane *ringPane, line string) {
	if c == nil || pane == nil {
		return
	}
	if c.disabled.Load() {
		c.writeFallback(line)
		return
	}
	formatted := formatLine(line, ansiWidth, c.color)
	c.mu.Lock()
	if len(pane.lines) == 0 {
		c.mu.Unlock()
		return
	}
	pane.lines[pane.idx] = formatted
	pane.idx = (pane.idx + 1) % len(pane.lines)
	if pane.count < len(pane.lines) {
		pane.count++
	}
	c.mu.Unlock()
	c.signalRender()
}

// Purpose: Signal the render loop to refresh the screen.
// Key aspects: Non-blocking coalesced notification.
// Upstream: SetStats and append paths.
// Downstream: refreshLoop.
func (c *ansiConsole) signalRender() {
	if c == nil || !c.isTTY || c.disabled.Load() {
		return
	}
	select {
	case c.renderCh <- struct{}{}:
	default:
	}
}

// Purpose: Periodic render loop for ANSI console output.
// Key aspects: Coalesces events and enforces min refresh spacing.
// Upstream: goroutine started in newANSIConsole.
// Downstream: time.NewTimer and c.render.
func (c *ansiConsole) refreshLoop() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "ANSI console panic: %v\n", r)
		}
	}()
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	var pending bool
	var lastRender time.Time
	for {
		select {
		case <-c.renderCh:
			if c.refresh <= 0 {
				c.render()
				lastRender = time.Now().UTC()
				pending = false
				continue
			}
			now := time.Now().UTC()
			if lastRender.IsZero() || now.Sub(lastRender) >= c.refresh {
				c.render()
				lastRender = time.Now().UTC()
				pending = false
				continue
			}
			if !pending {
				delay := c.refresh - now.Sub(lastRender)
				timer.Reset(delay)
				pending = true
			}
		case <-timer.C:
			pending = false
			c.render()
			lastRender = time.Now().UTC()
		case <-c.quit:
			return
		}
	}
}

// Purpose: Render the current snapshot to stdout.
// Key aspects: Copies panes under lock and emits row-diff updates.
// Upstream: refreshLoop.
// Downstream: snapshotPane, fillFrame, and output writes.
func (c *ansiConsole) render() {
	if c == nil || !c.isTTY || c.disabled.Load() {
		return
	}

	width, height, ok := termSize()
	if !ok || width < ansiWidth || height < ansiTotalRows {
		c.disable(fmt.Sprintf("terminal too small (%dx%d, need %dx%d)", width, height, ansiWidth, ansiTotalRows))
		return
	}
	forceFull := false
	if width != c.lastWidth || height != c.lastHeight {
		c.lastWidth = width
		c.lastHeight = height
		forceFull = true
	}

	c.mu.Lock()
	copy(c.snapStats, c.stats)
	dropped := snapshotPane(&c.dropped, c.snapDropped)
	calls := snapshotPane(&c.calls, c.snapCalls)
	unlic := snapshotPane(&c.unlic, c.snapUnlic)
	harm := snapshotPane(&c.harm, c.snapHarm)
	system := snapshotPane(&c.system, c.snapSys)
	c.mu.Unlock()

	c.fillFrame(c.frame, c.snapStats, dropped, calls, unlic, harm, system)

	var buf bytes.Buffer
	if forceFull {
		writeFullFrame(&buf, c.frame)
	} else {
		writeDiffFrame(&buf, c.frame, c.lastFrame)
	}
	if _, err := buf.WriteTo(os.Stdout); err != nil {
		c.disable("stdout write failed: " + err.Error())
		return
	}
	copy(c.lastFrame, c.frame)
}

// Purpose: Disable the ANSI UI and fall back to plain logging.
// Key aspects: Stops render loop and emits a single warning to stdout.
// Upstream: render size checks.
// Downstream: stopOnce and writeFallback.
func (c *ansiConsole) disable(reason string) {
	if c == nil {
		return
	}
	if !c.disabled.CompareAndSwap(false, true) {
		return
	}
	c.writeFallback("ANSI UI disabled: " + reason)
	c.Stop()
}

// Purpose: Write a fallback log line to stdout when ANSI is disabled.
// Key aspects: Strips markup and appends a newline.
// Upstream: append paths and disable.
// Downstream: os.Stdout.Write.
func (c *ansiConsole) writeFallback(line string) {
	clean := stripANSIMarkup(line)
	if clean == "" {
		return
	}
	if _, err := os.Stdout.Write([]byte(clean + "\n")); err != nil {
		log.Printf("ANSI UI fallback write failed: %v", err)
	}
}

func (c *ansiConsole) fillFrame(dst, stats, dropped, calls, unlic, harm, system []string) {
	idx := 0
	for i := 0; i < ansiStatsLines; i++ {
		dst[idx] = stats[i]
		idx++
	}
	dst[idx] = c.blankLine
	idx++
	dst[idx] = c.headerDrop
	idx++
	idx = fillPaneLines(dst, idx, dropped, c.blankLine)
	dst[idx] = c.blankLine
	idx++
	dst[idx] = c.headerCall
	idx++
	idx = fillPaneLines(dst, idx, calls, c.blankLine)
	dst[idx] = c.blankLine
	idx++
	dst[idx] = c.headerUnlic
	idx++
	idx = fillPaneLines(dst, idx, unlic, c.blankLine)
	dst[idx] = c.blankLine
	idx++
	dst[idx] = c.headerHarm
	idx++
	idx = fillPaneLines(dst, idx, harm, c.blankLine)
	dst[idx] = c.blankLine
	idx++
	dst[idx] = c.headerSys
	idx++
	fillPaneLines(dst, idx, system, c.blankLine)
}

func fillPaneLines(dst []string, idx int, lines []string, blank string) int {
	for i := 0; i < ansiPaneLines; i++ {
		if i < len(lines) {
			dst[idx] = lines[i]
		} else {
			dst[idx] = blank
		}
		idx++
	}
	return idx
}

type ansiWriter struct {
	append func(string)
	buf    []byte
	mu     sync.Mutex
}

// Write implements io.Writer for system logs routed to the ANSI console.
// Key aspects: Buffers until newline and forwards complete lines.
// Upstream: log output when ANSI UI is active.
// Downstream: bytes.IndexByte and w.append.
func (w *ansiWriter) Write(p []byte) (int, error) {
	if w == nil || w.append == nil {
		return len(p), nil
	}
	w.mu.Lock()
	const maxWriterBufferSize = 16 * 1024
	remaining, lines := linebuffer.AppendAndExtractLines(w.buf, p, maxWriterBufferSize)
	w.buf = remaining
	w.mu.Unlock()

	for _, line := range lines {
		w.append(line)
	}
	return len(p), nil
}

// Purpose: Snapshot a ring pane into a caller-provided buffer.
// Key aspects: Respects current count and ring order.
// Upstream: render.
// Downstream: None.
func snapshotPane(p *ringPane, buf []string) []string {
	if p == nil || len(p.lines) == 0 || p.count == 0 || len(buf) == 0 {
		return buf[:0]
	}
	start := p.idx - p.count
	if start < 0 {
		start += len(p.lines)
	}
	limit := p.count
	if limit > len(buf) {
		limit = len(buf)
	}
	for i := 0; i < limit; i++ {
		pos := (start + i) % len(p.lines)
		buf[i] = p.lines[pos]
	}
	return buf[:limit]
}

func writeFullFrame(buf *bytes.Buffer, frame []string) {
	buf.WriteString("\x1b[2J\x1b[H")
	for i, line := range frame {
		buf.WriteString(line)
		buf.WriteString("\x1b[K")
		if i < len(frame)-1 {
			buf.WriteByte('\n')
		}
	}
}

func writeDiffFrame(buf *bytes.Buffer, frame, last []string) {
	for i, line := range frame {
		if i < len(last) && last[i] == line {
			continue
		}
		writeCursor(buf, i+1)
		buf.WriteString(line)
		buf.WriteString("\x1b[K")
	}
}

func writeCursor(buf *bytes.Buffer, row int) {
	buf.WriteString("\x1b[")
	buf.WriteString(strconv.Itoa(row))
	buf.WriteString(";1H")
}

const resetANSI = "\x1b[0m"

func formatLine(line string, width int, enableColor bool) string {
	if width <= 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(width + 16)
	visible := 0
	usedColor := false
	colorActive := false
	for i := 0; i < len(line) && visible < width; {
		if line[i] == '[' {
			if ansi, consumed, ok := markupToken(line[i:]); ok {
				if enableColor {
					b.WriteString(ansi)
					usedColor = true
					if ansi == resetANSI {
						colorActive = false
					} else {
						colorActive = true
					}
				}
				i += consumed
				continue
			}
		}
		r, size := utf8.DecodeRuneInString(line[i:])
		if r == utf8.RuneError && size == 1 {
			i++
			continue
		}
		if isControlRune(r) {
			i += size
			continue
		}
		b.WriteRune(r)
		visible++
		i += size
	}
	if enableColor && usedColor && colorActive {
		b.WriteString(resetANSI)
	}
	for visible < width {
		b.WriteByte(' ')
		visible++
	}
	return b.String()
}

func markupToken(s string) (string, int, bool) {
	switch {
	case strings.HasPrefix(s, "[red]"):
		return "\x1b[31m", 5, true
	case strings.HasPrefix(s, "[green]"):
		return "\x1b[32m", 7, true
	case strings.HasPrefix(s, "[yellow]"):
		return "\x1b[33m", 8, true
	case strings.HasPrefix(s, "[blue]"):
		return "\x1b[34m", 6, true
	case strings.HasPrefix(s, "[magenta]"):
		return "\x1b[35m", 9, true
	case strings.HasPrefix(s, "[cyan]"):
		return "\x1b[36m", 6, true
	case strings.HasPrefix(s, "[white]"):
		return "\x1b[37m", 7, true
	case strings.HasPrefix(s, "[-]"):
		return resetANSI, 3, true
	default:
		return "", 0, false
	}
}

func isControlRune(r rune) bool {
	return r < 0x20 || r == 0x7f
}

func stripANSIMarkup(line string) string {
	if line == "" {
		return ""
	}
	return ansiStripReplacer.Replace(line)
}

var ansiStripReplacer = strings.NewReplacer(
	"[red]", "",
	"[green]", "",
	"[yellow]", "",
	"[blue]", "",
	"[magenta]", "",
	"[cyan]", "",
	"[white]", "",
	"[-]", "",
)

func termSize() (int, int, bool) {
	width, height, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 0, 0, false
	}
	return width, height, true
}

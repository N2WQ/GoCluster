package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"dxcluster/config"
)

// ansiConsole is a lightweight, fixed-buffer console renderer that uses ANSI
// escape codes. It is selected solely via ui.mode=ansi in the YAML config.
type ansiConsole struct {
	mu        sync.Mutex
	stats     []string
	calls     ringPane
	unlic     ringPane
	harm      ringPane
	system    ringPane
	refresh   time.Duration
	quit      chan struct{}
	writer    *ansiWriter
	isTTY     bool
	color     bool
	clear     bool
	renderBuf bytes.Buffer
	snapCalls []string
	snapUnlic []string
	snapHarm  []string
	snapSys   []string
	stopOnce  sync.Once
}

type ringPane struct {
	lines []string
	idx   int
	count int
}

func newANSIConsole(uiCfg config.UIConfig, allowRender bool) uiSurface {
	if !allowRender {
		return nil
	}

	refresh := time.Duration(uiCfg.RefreshMS) * time.Millisecond
	if refresh < 0 {
		refresh = 0
	}
	const minRefresh = 16 * time.Millisecond
	if refresh > 0 && refresh < minRefresh {
		log.Printf("UI: clamping refresh interval to %dms (requested %dms too low)", minRefresh/time.Millisecond, refresh/time.Millisecond)
		refresh = minRefresh
	}

	statsLines := uiCfg.PaneLines.Stats
	if statsLines <= 0 {
		statsLines = 1
	}
	callsLines := uiCfg.PaneLines.Calls
	if callsLines <= 0 {
		callsLines = 1
	}
	unlicensedLines := uiCfg.PaneLines.Unlicensed
	if unlicensedLines <= 0 {
		unlicensedLines = 1
	}
	harmonicLines := uiCfg.PaneLines.Harmonics
	if harmonicLines <= 0 {
		harmonicLines = 1
	}
	systemLines := uiCfg.PaneLines.System
	if systemLines <= 0 {
		systemLines = 1
	}

	c := &ansiConsole{
		stats:     make([]string, statsLines),
		calls:     ringPane{lines: make([]string, callsLines)},
		unlic:     ringPane{lines: make([]string, unlicensedLines)},
		harm:      ringPane{lines: make([]string, harmonicLines)},
		system:    ringPane{lines: make([]string, systemLines)},
		refresh:   refresh,
		quit:      make(chan struct{}),
		isTTY:     true, // caller only constructs when rendering is permitted
		color:     uiCfg.Color,
		clear:     uiCfg.ClearScreen,
		snapCalls: make([]string, callsLines),
		snapUnlic: make([]string, unlicensedLines),
		snapHarm:  make([]string, harmonicLines),
		snapSys:   make([]string, systemLines),
	}
	c.writer = &ansiWriter{append: c.AppendSystem, color: uiCfg.Color}

	// Only render when a TTY is present and refresh is positive.
	if c.isTTY && c.refresh > 0 {
		go c.refreshLoop()
	}

	return c
}

func (c *ansiConsole) WaitReady() {}

func (c *ansiConsole) Stop() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		close(c.quit)
	})
}

func (c *ansiConsole) SetStats(lines []string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	limit := len(lines)
	if limit > len(c.stats) {
		limit = len(c.stats)
	}
	copy(c.stats, lines[:limit])
	for i := limit; i < len(c.stats); i++ {
		c.stats[i] = ""
	}
	c.mu.Unlock()
}

func (c *ansiConsole) AppendCall(line string)       { c.append(&c.calls, line) }
func (c *ansiConsole) AppendUnlicensed(line string) { c.append(&c.unlic, line) }
func (c *ansiConsole) AppendHarmonic(line string)   { c.append(&c.harm, line) }
func (c *ansiConsole) AppendSystem(line string)     { c.append(&c.system, line) }

func (c *ansiConsole) SystemWriter() io.Writer {
	if c == nil {
		return nil
	}
	return c.writer
}

func (c *ansiConsole) append(pane *ringPane, line string) {
	if c == nil || pane == nil {
		return
	}
	line = applyANSIMarkup(line, c.color)
	c.mu.Lock()
	if len(pane.lines) == 0 {
		c.mu.Unlock()
		return
	}
	pane.lines[pane.idx] = line
	pane.idx = (pane.idx + 1) % len(pane.lines)
	if pane.count < len(pane.lines) {
		pane.count++
	}
	c.mu.Unlock()
}

func (c *ansiConsole) refreshLoop() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "ANSI console panic: %v\n", r)
		}
	}()
	ticker := time.NewTicker(c.refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.render()
		case <-c.quit:
			return
		}
	}
}

func (c *ansiConsole) render() {
	if c == nil || !c.isTTY {
		return
	}

	c.mu.Lock()
	stats := make([]string, len(c.stats))
	copy(stats, c.stats[:])
	calls := snapshotPane(&c.calls, c.snapCalls)
	unlic := snapshotPane(&c.unlic, c.snapUnlic)
	harm := snapshotPane(&c.harm, c.snapHarm)
	system := snapshotPane(&c.system, c.snapSys)
	c.mu.Unlock()

	c.renderBuf.Reset()
	// Clear screen + home cursor.
	if c.clear {
		c.renderBuf.WriteString("\x1b[2J\x1b[H")
	}

	for _, line := range stats {
		if line != "" {
			c.renderBuf.WriteString(line)
		}
		c.renderBuf.WriteByte('\n')
	}

	writePane(&c.renderBuf, "---- Call Corrections ----", calls)
	writePane(&c.renderBuf, "---- Unlicensed US Calls ----", unlic)
	writePane(&c.renderBuf, "---- Harmonics ----", harm)
	writePane(&c.renderBuf, "---- System ----", system)

	_, _ = c.renderBuf.WriteTo(os.Stdout)
}

type stringByteWriter interface {
	WriteString(string) (int, error)
	WriteByte(byte) error
}

func writePane(w stringByteWriter, title string, lines []string) {
	w.WriteString(title)
	w.WriteByte('\n')
	for _, line := range lines {
		if line != "" {
			w.WriteString(line)
		}
		w.WriteByte('\n')
	}
}

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

type ansiWriter struct {
	append func(string)
	buf    []byte
	color  bool
	mu     sync.Mutex
}

func (w *ansiWriter) Write(p []byte) (int, error) {
	if w == nil || w.append == nil {
		return len(p), nil
	}
	w.mu.Lock()
	w.buf = append(w.buf, p...)
	data := w.buf
	w.mu.Unlock()

	for {
		idx := indexByte(data, '\n')
		if idx == -1 {
			break
		}
		line := strings.TrimRight(string(data[:idx]), "\r")
		line = applyANSIMarkup(line, w.color)
		w.append(line)
		data = data[idx+1:]
	}

	w.mu.Lock()
	const maxWriterBufferSize = 16 * 1024
	if len(data) > maxWriterBufferSize {
		// Drop overflow by forcing a flush of the partial line to avoid unbounded growth.
		trimmed := strings.TrimRight(string(data), "\r")
		if trimmed != "" {
			w.append(applyANSIMarkup(trimmed, w.color))
		}
		data = data[:0]
	}
	w.buf = data
	w.mu.Unlock()
	return len(p), nil
}

func indexByte(b []byte, c byte) int {
	return bytes.IndexByte(b, c)
}

func applyANSIMarkup(line string, enableColor bool) string {
	if line == "" {
		return line
	}
	if enableColor {
		// Heuristic: any markup brackets triggers a reset append after replacement.
		hasMarkup := strings.Contains(line, "[")
		line = ansiColorReplacer.Replace(line)
		if hasMarkup {
			line += resetANSI
		}
		return line
	}
	return ansiStripReplacer.Replace(line)
}

const resetANSI = "\x1b[0m"

var ansiColorReplacer = strings.NewReplacer(
	"[red]", "\x1b[31m",
	"[green]", "\x1b[32m",
	"[yellow]", "\x1b[33m",
	"[blue]", "\x1b[34m",
	"[magenta]", "\x1b[35m",
	"[cyan]", "\x1b[36m",
	"[white]", "\x1b[37m",
	"[-]", resetANSI,
)

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

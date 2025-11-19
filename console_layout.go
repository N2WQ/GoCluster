package main

import (
	"fmt"
	"io"
	"sync"

	"golang.org/x/term"
)

// consoleLayout coordinates drawing a pinned header while allowing the log stream
// to scroll underneath without disturbing the header.
type consoleLayout struct {
	out           io.Writer
	enabled       bool
	fd            int
	rows          int
	reservedLines int
	mu            sync.Mutex
}

func newConsoleLayout(out io.Writer, enabled bool, fd int) *consoleLayout {
	if out == nil {
		out = io.Discard
	}
	if enabled {
		fmt.Fprint(out, "\x1b[2J\x1b[H")
	}
	rows := 24
	if enabled {
		if _, h, err := term.GetSize(fd); err == nil && h > 0 {
			rows = h
		} else {
			enabled = false
		}
	}
	return &consoleLayout{
		out:     out,
		enabled: enabled && fd >= 0,
		fd:      fd,
		rows:    rows,
	}
}

func (c *consoleLayout) Close() {
	if !c.enabled {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetScrollRegion()
}

func (c *consoleLayout) resetScrollRegion() {
	fmt.Fprint(c.out, "\x1b[r")
}

// LogWriter returns an io.Writer that serializes log writes with stats redraws.
func (c *consoleLayout) LogWriter() io.Writer {
	return &layoutWriter{layout: c}
}

func (c *consoleLayout) screenRows() int {
	if c.fd < 0 {
		return c.rows
	}
	_, h, err := term.GetSize(c.fd)
	if err != nil || h <= 0 {
		return c.rows
	}
	c.rows = h
	return c.rows
}

// Render rewrites the pinned stats header. With VT support we protect the top lines
// from scrolling by narrowing the terminal's scroll region; otherwise we fall back
// to the original behaviour.
func (c *consoleLayout) Render(lines []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.enabled {
		for _, line := range lines {
			fmt.Fprintln(c.out, line)
		}
		fmt.Fprintln(c.out, "---")
		return
	}

	rows := c.screenRows()
	if rows <= 1 {
		for _, line := range lines {
			fmt.Fprintln(c.out, line)
		}
		fmt.Fprintln(c.out, "---")
		return
	}

	reserved := len(lines) + 2 // include two blank spacer lines
	if reserved >= rows {
		reserved = rows - 1
	}
	if reserved < 1 {
		reserved = 1
	}

	if reserved != c.reservedLines {
		c.resetScrollRegion()
		fmt.Fprintf(c.out, "\x1b[%d;%dr", reserved+1, rows)
		c.reservedLines = reserved
	}

	fmt.Fprint(c.out, "\x1b[H") // Jump to top-left.
	for i := 0; i < reserved; i++ {
		var text string
		if i < len(lines) {
			text = lines[i]
		}
		fmt.Fprintf(c.out, "%s\x1b[K\n", text)
	}
	fmt.Fprintf(c.out, "\x1b[%d;1H", reserved+1)
}

// MoveCursorToLogStart positions the cursor at the first line below the pinned
// stats block. Useful after the initial render to ensure log output begins in the
// scrolling region rather than overwriting the header.
func (c *consoleLayout) MoveCursorToLogStart() {
	if !c.enabled {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	reserved := c.reservedLines
	if reserved <= 0 {
		reserved = 1
	}
	fmt.Fprintf(c.out, "\x1b[%d;1H", reserved+1)
}

type layoutWriter struct {
	layout *consoleLayout
}

func (w *layoutWriter) Write(p []byte) (int, error) {
	w.layout.mu.Lock()
	defer w.layout.mu.Unlock()
	return w.layout.out.Write(p)
}

package stats

import (
	"strings"
	"sync"
)

type stabilizerTransitionCell struct {
	turns   uint64
	samples uint64
}

// Fixed storage bounds process-lifetime accounting to 18 cells. The mutex
// keeps each sum/sample pair coherent during observation, snapshot and reset.
// No spot references or caller-supplied labels survive an observation.
type stabilizerTransitionTracker struct {
	mu    sync.Mutex
	cells [3][6]stabilizerTransitionCell
}

func stabilizerGlyphIndex(glyph string) int {
	switch strings.ToUpper(strings.TrimSpace(glyph)) {
	case "", "?":
		return 0
	case "S":
		return 1
	case "P":
		return 2
	case "V":
		return 3
	case "C":
		return 4
	case "B":
		return 5
	default:
		return -1
	}
}

// ObserveStabilizerTransition records one terminal delayed spot. Callers own
// once-only accounting; retries and initial overflow must not call this method.
// Invalid entry/terminal glyphs and incomplete turns cannot create labels.
func (t *Tracker) ObserveStabilizerTransition(entered, ended string, turns int) {
	if t == nil || turns < 1 {
		return
	}
	row, col := stabilizerGlyphIndex(entered), stabilizerGlyphIndex(ended)
	if row < 0 || row >= 3 || col < 0 {
		return
	}
	t.stabilizerTransitions.mu.Lock()
	cell := &t.stabilizerTransitions.cells[row][col]
	cell.turns += uint64(turns)
	cell.samples++
	t.stabilizerTransitions.mu.Unlock()
}

// StabilizerTransitionStats returns a coherent value snapshot. Rows are ?, S,
// P; columns are ?, S, P, V, C, B. Zero samples means no terminal observations.
func (t *Tracker) StabilizerTransitionStats() (out [3][6]StabilizerGlyphTurnStat) {
	if t == nil {
		return out
	}
	t.stabilizerTransitions.mu.Lock()
	defer t.stabilizerTransitions.mu.Unlock()
	for row := range out {
		for col := range out[row] {
			cell := t.stabilizerTransitions.cells[row][col]
			if cell.samples > 0 {
				out[row][col] = StabilizerGlyphTurnStat{
					AverageTurns: float64(cell.turns) / float64(cell.samples),
					Samples:      cell.samples,
				}
			}
		}
	}
	return out
}

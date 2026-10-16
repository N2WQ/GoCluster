package cluster

import (
	"strings"
	"testing"
	"unicode/utf8"

	"dxcluster/stats"
)

func TestStabilizerMatrixOneDecimalAndEmptyCells(t *testing.T) {
	tr := stats.NewTracker()
	tr.ObserveStabilizerTransition("S", "V", 2)
	tr.ObserveStabilizerTransition("S", "V", 3)
	tr.ObserveStabilizerTransition("?", "?", 17)
	tr.ObserveStabilizerTransition("S", "B", 3)
	for _, turns := range []int{2, 2, 2, 3} {
		tr.ObserveStabilizerTransition("P", "C", turns)
	}
	lines := formatStabilizerMatrix(tr)
	if len(lines) != 5 || lines[0] != "Stabilizer: average turns per call (entered → ended)" {
		t.Fatalf("wrong matrix labels: %q", lines)
	}
	if got := strings.Fields(lines[1]); strings.Join(got, " ") != "Entered ↓ / Ended → ? S P V C B" {
		t.Fatalf("wrong columns: %q", got)
	}
	want := []string{"? 17.0 — — — — —", "S — — — 2.5 — 3.0", "P — — — — 2.3 —"}
	for row, expected := range want {
		if got := strings.Join(strings.Fields(lines[row+2]), " "); got != expected {
			t.Errorf("row %d = %q, want %q", row, got, expected)
		}
	}
	for _, line := range lines {
		if utf8.RuneCountInString(line) > 56 {
			t.Errorf("matrix exceeds 56 columns: %q", line)
		}
	}
	tr.Reset()
	for _, tracker := range []*stats.Tracker{tr, nil} {
		empty := strings.Join(formatStabilizerMatrix(tracker), "\n")
		if strings.Count(empty, "—") != 18 || strings.Contains(empty, "17.0") {
			t.Errorf("empty matrix: %q", empty)
		}
	}
}

func TestStabilizerMatrixReplacesOverviewGlyphLine(t *testing.T) {
	lines := buildOverviewLinesForStatsTest(fixedOverviewSSNProvider{})
	joined := strings.Join(lines, "\n")
	if strings.Contains(joined, "Stabilizer Glyph") ||
		!strings.Contains(joined, "Stabilizer: average turns per call (entered → ended)") ||
		strings.Count(joined, "—") < 18 {
		t.Fatalf("overview did not replace old glyph summary with empty matrix: %s", joined)
	}
}

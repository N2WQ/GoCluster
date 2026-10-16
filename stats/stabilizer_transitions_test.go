package stats

import (
	"fmt"
	"sync"
	"testing"
)

func TestStabilizerTransitionAggregationAndReset(t *testing.T) {
	tr := NewTracker()
	if got := tr.StabilizerTransitionStats(); got != [3][6]StabilizerGlyphTurnStat{} {
		t.Fatalf("new tracker has observations: %v", got)
	}
	tr.ObserveStabilizerTransition(" s ", "v", 2)
	tr.ObserveStabilizerTransition("S", "V", 5)
	tr.ObserveStabilizerTransition("", "?", 17)
	tr.ObserveStabilizerTransition("P", "C", 1)
	tr.ObserveStabilizerTransition("P", "b", 4)
	got := tr.StabilizerTransitionStats()
	if got[1][3] != (StabilizerGlyphTurnStat{AverageTurns: 3.5, Samples: 2}) ||
		got[0][0] != (StabilizerGlyphTurnStat{AverageTurns: 17, Samples: 1}) ||
		got[2][4] != (StabilizerGlyphTurnStat{AverageTurns: 1, Samples: 1}) ||
		got[2][5] != (StabilizerGlyphTurnStat{AverageTurns: 4, Samples: 1}) || got[1][1].Samples != 0 {
		t.Fatalf("incorrect transition cells: %v", got)
	}
	// Arbitrary labels cannot grow the fixed aggregate or alias valid cells.
	for i := 0; i < 1000; i++ {
		label := fmt.Sprintf("invalid-%d", i)
		tr.ObserveStabilizerTransition(label, "V", 1)
		tr.ObserveStabilizerTransition("S", label, 1)
	}
	tr.ObserveStabilizerTransition("V", "V", 2)
	tr.ObserveStabilizerTransition("S", "V", 0)
	if after := tr.StabilizerTransitionStats(); after != got {
		t.Fatalf("invalid observations changed aggregates: %v", after)
	}
	tr.Reset()
	if after := tr.StabilizerTransitionStats(); after != [3][6]StabilizerGlyphTurnStat{} {
		t.Fatalf("reset retained transitions: %v", after)
	}
	if got[1][3].Samples != 2 {
		t.Fatal("reset mutated an earlier value snapshot")
	}
	var absent *Tracker
	absent.ObserveStabilizerTransition("S", "V", 2)
	if absent.StabilizerTransitionStats() != [3][6]StabilizerGlyphTurnStat{} {
		t.Fatal("nil tracker must return empty cells")
	}
}

func TestStabilizerTransitionConcurrentSnapshotsAndReset(t *testing.T) {
	tr := NewTracker()
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Go(func() {
			for i := 0; i < 1000; i++ {
				tr.ObserveStabilizerTransition("S", "V", 2)
				cell := tr.StabilizerTransitionStats()[1][3]
				if cell.Samples > 0 && cell.AverageTurns != 2 {
					t.Errorf("incoherent sum/sample pair: %+v", cell)
					return
				}
			}
		})
	}
	wg.Go(func() {
		for i := 0; i < 100; i++ {
			tr.Reset()
		}
	})
	wg.Wait()
	tr.Reset()
	tr.ObserveStabilizerTransition("S", "V", 2)
	if got := tr.StabilizerTransitionStats()[1][3]; got.Samples != 1 || got.AverageTurns != 2 {
		t.Fatalf("post-churn accounting: %+v", got)
	}
}

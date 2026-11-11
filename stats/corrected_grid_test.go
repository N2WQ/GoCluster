package stats

import (
	"sync"
	"testing"
)

func TestCorrectedOutputWithoutGridResetAndConcurrency(t *testing.T) {
	tr := NewTracker()
	var writers sync.WaitGroup
	for range 4 {
		writers.Go(func() {
			for range 1000 {
				tr.IncrementCorrectedOutputWithoutGrid()
				_ = tr.CorrectedOutputWithoutGrid()
			}
		})
	}
	writers.Wait()
	if got := tr.CorrectedOutputWithoutGrid(); got != 4000 {
		t.Fatalf("admissions=%d want 4000", got)
	}
	tr.Reset()
	if tr.CorrectedOutputWithoutGrid() != 0 {
		t.Fatal("statistics reset retained the admission counter")
	}
	for range 4 {
		writers.Go(func() {
			for range 100 {
				tr.IncrementCorrectedOutputWithoutGrid()
				tr.Reset()
				_ = tr.CorrectedOutputWithoutGrid()
			}
		})
	}
	writers.Wait()
	tr.Reset()
	if tr.CorrectedOutputWithoutGrid() != 0 {
		t.Fatal("joined reset did not clear the counter")
	}
	var absent *Tracker
	absent.IncrementCorrectedOutputWithoutGrid()
	if absent.CorrectedOutputWithoutGrid() != 0 {
		t.Fatal("nil tracker did not remain empty")
	}
}

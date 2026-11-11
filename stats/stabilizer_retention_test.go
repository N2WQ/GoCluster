package stats

import (
	"sync"
	"testing"
)

func TestStabilizerRetainedCounterResetAndConcurrency(t *testing.T) {
	tr := NewTracker()
	var writers sync.WaitGroup
	for range 4 {
		writers.Go(func() {
			for range 1000 {
				tr.IncrementStabilizerRetainedAboveFresh()
				tr.IncrementStabilizerAgeDropped()
				_ = tr.StabilizerRetainedAboveFresh()
				_ = tr.StabilizerAgeDropped()
			}
		})
	}
	writers.Wait()
	if got := tr.StabilizerRetainedAboveFresh(); got != 4000 || tr.StabilizerAgeDropped() != 4000 {
		t.Fatalf("terminal outcomes=%d want 4000", got)
	}
	tr.Reset()
	if tr.StabilizerRetainedAboveFresh() != 0 || tr.StabilizerAgeDropped() != 0 {
		t.Fatal("quiescent statistics reset retained the new counter")
	}
	for range 4 {
		writers.Go(func() {
			for range 100 {
				tr.IncrementStabilizerRetainedAboveFresh()
				tr.IncrementStabilizerAgeDropped()
				tr.Reset()
				_ = tr.StabilizerRetainedAboveFresh()
				_ = tr.StabilizerAgeDropped()
			}
		})
	}
	writers.Wait()
	tr.Reset()
	if tr.StabilizerRetainedAboveFresh() != 0 || tr.StabilizerAgeDropped() != 0 {
		t.Fatal("joined reset did not clear the counter")
	}
	var absent *Tracker
	absent.IncrementStabilizerRetainedAboveFresh()
	absent.IncrementStabilizerAgeDropped()
	if absent.StabilizerRetainedAboveFresh() != 0 || absent.StabilizerAgeDropped() != 0 {
		t.Fatal("nil tracker did not remain empty")
	}
}

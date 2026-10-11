package stats

import (
	"sync"
	"testing"
	"time"
)

func TestTrackerTemporalCounters(t *testing.T) {
	tr := NewTracker()
	tr.IncrementTemporalPending()
	tr.IncrementTemporalPending()
	tr.DecrementTemporalPending()
	tr.IncrementTemporalProposed()
	tr.IncrementTemporalProposed()
	tr.IncrementTemporalHandoffAttempt()
	tr.IncrementTemporalCommitted()
	tr.IncrementTemporalFallbackResolver()
	tr.IncrementTemporalAbstainLowMargin()
	tr.IncrementTemporalOverflowBypass()
	tr.IncrementTemporalPathSwitch()
	tr.ObserveTemporalCommitLatency(400 * time.Millisecond)
	tr.ObserveTemporalCommitLatency(900 * time.Millisecond)
	tr.ObserveTemporalCommitLatency(1500 * time.Millisecond)
	tr.ObserveTemporalCommitLatency(3 * time.Second)
	tr.ObserveTemporalCommitLatency(7 * time.Second)

	if tr.TemporalPending() != 1 {
		t.Fatalf("expected temporal pending=1, got %d", tr.TemporalPending())
	}
	if tr.TemporalCommitted() != 1 {
		t.Fatalf("expected temporal committed=1, got %d", tr.TemporalCommitted())
	}
	if tr.TemporalProposed() != 2 || tr.TemporalHandoffAttempts() != 1 {
		t.Fatalf("proposal/handoff counters conflated with mutations: proposed=%d handoff=%d committed=%d", tr.TemporalProposed(), tr.TemporalHandoffAttempts(), tr.TemporalCommitted())
	}
	if tr.TemporalFallbackResolver() != 1 {
		t.Fatalf("expected temporal fallback=1, got %d", tr.TemporalFallbackResolver())
	}
	if tr.TemporalAbstainLowMargin() != 1 {
		t.Fatalf("expected temporal abstain=1, got %d", tr.TemporalAbstainLowMargin())
	}
	if tr.TemporalOverflowBypass() != 1 {
		t.Fatalf("expected temporal bypass=1, got %d", tr.TemporalOverflowBypass())
	}
	if tr.TemporalPathSwitches() != 1 {
		t.Fatalf("expected temporal path switches=1, got %d", tr.TemporalPathSwitches())
	}
	buckets := tr.TemporalCommitLatencyBuckets()
	if buckets["le_500"] != 1 || buckets["le_1000"] != 1 || buckets["le_2000"] != 1 || buckets["le_5000"] != 1 || buckets["le_10000"] != 1 || buckets["gt_10000"] != 0 {
		t.Fatalf("unexpected temporal latency buckets: %+v", buckets)
	}
	tr.Reset()
	if tr.TemporalPending() != 0 || tr.TemporalProposed() != 0 || tr.TemporalHandoffAttempts() != 0 || tr.TemporalCommitted() != 0 ||
		tr.TemporalFallbackResolver() != 0 || tr.TemporalAbstainLowMargin() != 0 || tr.TemporalOverflowBypass() != 0 || tr.TemporalPathSwitches() != 0 || len(tr.TemporalCommitLatencyBuckets()) != 0 {
		t.Fatal("Reset retained temporal proposal, handoff, mutation or queue counters")
	}
}

func TestTrackerTemporalProposalsDoNotCountMutations(t *testing.T) {
	tr := NewTracker()
	tr.IncrementTemporalProposed()
	tr.IncrementTemporalHandoffAttempt()
	if tr.TemporalProposed() != 1 || tr.TemporalHandoffAttempts() != 1 || tr.TemporalCommitted() != 0 || tr.TemporalPathSwitches() != 0 {
		t.Fatal("proposal/handoff attempt counted an accepted mutation or path switch")
	}
	var nilTracker *Tracker
	nilTracker.IncrementTemporalProposed()
	nilTracker.IncrementTemporalHandoffAttempt()
	if nilTracker.TemporalProposed() != 0 || nilTracker.TemporalHandoffAttempts() != 0 {
		t.Fatal("nil tracker exposed proposal/handoff counts")
	}
}

func TestTrackerTemporalProposalCountersConcurrent(t *testing.T) {
	tr := NewTracker()
	const workers, attempts = 4, 100
	var group sync.WaitGroup
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < attempts; j++ {
				tr.IncrementTemporalProposed()
				tr.IncrementTemporalHandoffAttempt()
				_ = tr.TemporalProposed()
				_ = tr.TemporalHandoffAttempts()
			}
		}()
	}
	group.Wait()
	if tr.TemporalProposed() != workers*attempts || tr.TemporalHandoffAttempts() != workers*attempts || tr.TemporalCommitted() != 0 {
		t.Fatal("concurrent proposal/handoff increments lost counts or invented mutations")
	}
}

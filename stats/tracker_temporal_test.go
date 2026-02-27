package stats

import (
	"testing"
	"time"
)

func TestTrackerTemporalCounters(t *testing.T) {
	tr := NewTracker()
	tr.IncrementTemporalPending()
	tr.IncrementTemporalPending()
	tr.DecrementTemporalPending()
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
}

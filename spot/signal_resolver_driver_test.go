package spot

import (
	"testing"
	"time"
)

func TestSignalResolverDriverRejectsStartedResolver(t *testing.T) {
	resolver := NewSignalResolver(SignalResolverConfig{
		QueueSize:           8,
		MaxActiveKeys:       10,
		MaxCandidatesPerKey: 4,
		MaxReportersPerCand: 4,
		InactiveTTL:         time.Minute,
		EvalMinInterval:     time.Millisecond,
		SweepInterval:       time.Second,
		HysteresisWindows:   2,
	})
	resolver.Start()
	defer resolver.Stop()

	if _, err := NewSignalResolverDriver(resolver); err == nil {
		t.Fatalf("expected error constructing driver for already-started resolver")
	}
}

func TestSignalResolverDriverStepDrainsAndUpdatesSnapshots(t *testing.T) {
	resolver := NewSignalResolver(SignalResolverConfig{
		QueueSize:           8,
		MaxActiveKeys:       10,
		MaxCandidatesPerKey: 4,
		MaxReportersPerCand: 4,
		InactiveTTL:         time.Minute,
		EvalMinInterval:     time.Millisecond,
		SweepInterval:       time.Second,
		HysteresisWindows:   2,
	})
	driver, err := NewSignalResolverDriver(resolver)
	if err != nil {
		t.Fatalf("NewSignalResolverDriver: %v", err)
	}

	now := time.Date(2026, 2, 23, 20, 0, 0, 0, time.UTC)
	key := NewResolverSignalKey(14070.0, "20m", "CW", 1000)

	ok := resolver.Enqueue(ResolverEvidence{
		ObservedAt:    now,
		Key:           key,
		DXCall:        "W1AW",
		Spotter:       "K1AAA",
		FrequencyKHz:  14070.0,
		RecencyWindow: 30 * time.Second,
	})
	if !ok {
		t.Fatalf("expected enqueue to succeed")
	}

	if processed := driver.Step(now); processed != 1 {
		t.Fatalf("expected processed=1, got %d", processed)
	}

	if snap, ok := resolver.Lookup(key); !ok {
		t.Fatalf("expected snapshot to exist after step")
	} else if !snap.EvaluatedAt.Equal(now) {
		t.Fatalf("expected snapshot evaluated_at=%s, got %s", now.Format(time.RFC3339Nano), snap.EvaluatedAt.Format(time.RFC3339Nano))
	}
}

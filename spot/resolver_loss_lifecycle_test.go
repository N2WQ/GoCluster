package spot

import (
	"fmt"
	"testing"
	"time"
)

func TestResolverProcessClockSelectionAndObservationClockIndependence(t *testing.T) {
	r, d, key, at := evidenceTestDriver(t, SignalResolverConfig{InactiveTTL: time.Second, EvalMinInterval: time.Nanosecond})
	if d.SetProcessElapsed(-time.Nanosecond) == nil {
		t.Fatal("negative process elapsed accepted")
	}
	if !r.Enqueue(ResolverEvidence{Key: key, DXCall: "K1ABC", Spotter: "N0AAA", ObservedAt: at, FrequencyKHz: 7010.1}) {
		t.Fatal("enqueue before manual selection")
	}
	if err := d.SetProcessElapsed(time.Hour); err != nil {
		t.Fatal("enqueue incorrectly became owner admission", err)
	}
	if err := d.SetProcessElapsed(time.Hour); err != nil {
		t.Fatal("equal process elapsed refused", err)
	}
	if d.SetProcessElapsed(time.Hour-time.Nanosecond) == nil {
		t.Fatal("decreasing process elapsed accepted")
	}
	d.Step(at)
	s := lossTestSnapshot(t, r, key)
	if !s.EvaluatedAt.Equal(at) || !d.lastSweepAt.Equal(at) {
		t.Fatalf("process clock replaced observation clock %+v", s)
	}
	if err := d.SetProcessElapsed(2 * time.Hour); err != nil {
		t.Fatal(err)
	}
	d.Sweep(at.Add(2 * time.Second))
	if len(d.states) != 0 {
		t.Fatal("ordinary observation TTL ignored")
	}
	// First-time manual selection is prohibited after any admitted receipt,
	// including after its key has physically retired.
	r2, d2, key2, at2 := evidenceTestDriver(t, SignalResolverConfig{InactiveTTL: time.Second})
	lossTestAdmit(t, r2, d2, key2, "K1ABC", "N0AAA", at2, time.Second)
	d2.Sweep(at2.Add(2 * time.Second))
	if d2.SetProcessElapsed(0) == nil {
		t.Fatal("late manual selection accepted after retirement")
	}
	// Owner reliability refusal is not an admitted receipt.
	r3, d3, key3, at3 := evidenceTestDriver(t, SignalResolverConfig{MinSpotterReliability: 0.5, SpotterReliability: SpotterReliability{"N0BAD": 0.1}})
	lossTestAdmit(t, r3, d3, key3, "K1ABC", "N0BAD", at3, time.Minute)
	if len(d3.states) != 0 || r3.MetricsSnapshot().DropReliability != 1 {
		t.Fatal("reliability refusal fixture did not refuse")
	}
	if err := d3.SetProcessElapsed(0); err != nil {
		t.Fatal("refused receipt prevented manual mode", err)
	}
}

func lossTestQuarantine(t testing.TB, cfg SignalResolverConfig) (*SignalResolver, *SignalResolverDriver, ResolverSignalKey, time.Time, ResolverSnapshot) {
	t.Helper()
	cfg.MaxReportersPerCand = 1
	r, d, key, at := lossTestDriver(t, cfg)
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A1", at, time.Hour)
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A2", at, time.Hour)
	s := lossTestSnapshot(t, r, key)
	if !s.EvidenceLoss {
		t.Fatal("fixture did not lose a receiver")
	}
	return r, d, key, at, s
}

func TestResolverLossGenerationLogicalIdleDeadlineAndFreshAdmission(t *testing.T) {
	r, d, key, at, old := lossTestQuarantine(t, SignalResolverConfig{})
	_, cell, ok := r.lookupSnapshot(key)
	if !ok {
		t.Fatal("capture old cell")
	}
	if err := d.SetProcessElapsed(10*time.Minute - time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Lookup(key); !ok {
		t.Fatal("retired before exact idle deadline")
	}
	if _, ok := r.LookupEligible(key, at); !ok {
		t.Fatal("eligible read retired early")
	}
	if err := d.SetProcessElapsed(10 * time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Lookup(key); ok {
		t.Fatal("raw lookup authorized logically retired generation")
	}
	if _, ok := r.LookupEligible(key, at); ok {
		t.Fatal("eligible lookup authorized logically retired generation")
	}
	if len(d.states) != 1 || cell.terminal.Load() {
		t.Fatal("read physically retired/latches owner generation")
	}
	// A report at equality must create a new declared population, even when its
	// observation timestamp is old and no periodic sweep has occurred.
	lossTestAdmit(t, r, d, key, "K1ABD", "N0NEW", at, time.Hour)
	current := lossTestSnapshot(t, r, key)
	if current.Generation == old.Generation || current.EvidenceLoss || current.TotalReporters != 1 || current.Winner != "K1ABD" {
		t.Fatalf("arrival revived old generation %+v", current)
	}
	if !cell.terminal.Load() || cell.snapshot.Load() != nil || r.cellCurrent(cell) {
		t.Fatal("old captured cell survived replacement")
	}
	if row, exists := lossTestRow(old, "K1ABC", "N0A2"); !exists || row.ObservedAt != at || old.Generation == 0 || !old.EvidenceLoss {
		t.Fatalf("captured snapshot mutated %+v", old)
	}
	if _, exists := lossTestRow(current, "K1ABC", "N0A2"); exists {
		t.Fatal("new generation retained retired rows")
	}
}

func TestResolverLossActivityUsesAdmittedReceiptNotMutationOrEnqueue(t *testing.T) {
	for _, tc := range []struct {
		name    string
		receipt string
		refresh bool
	}{
		{"duplicate", "duplicate", true},
		{"stale", "stale", true},
		{"queued only", "queued", false},
		{"reliability refusal", "rejected", false},
		{"other key refusal", "other-key", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, d, key, at, original := lossTestQuarantine(t, SignalResolverConfig{MaxActiveKeys: 1, MinSpotterReliability: 0.5, SpotterReliability: SpotterReliability{"N0BAD": 0.1}})
			view := original.ReceiverEvidence()
			if err := d.SetProcessElapsed(9 * time.Minute); err != nil {
				t.Fatal(err)
			}
			ev := ResolverEvidence{Key: key, DXCall: "K1ABC", Spotter: "N0A2", ObservedAt: at, FrequencyKHz: 7010.1, RecencyWindow: time.Hour}
			switch tc.receipt {
			case "stale":
				ev.ObservedAt = at.Add(-time.Second)
			case "rejected":
				ev.Spotter = "N0BAD"
			case "other-key":
				ev.FrequencyKHz = 7011.1
				ev.Key = NewResolverSignalKey(ev.FrequencyKHz, "40m", "CW", 500)
			}
			if !r.Enqueue(ev) {
				t.Fatal("enqueue activity fixture")
			}
			if tc.receipt != "queued" {
				d.Drain()
			}
			if tc.receipt == "rejected" && r.MetricsSnapshot().DropReliability != 1 {
				t.Fatal("reliability fixture not refused")
			}
			if tc.receipt == "other-key" && r.MetricsSnapshot().DropMaxKeys != 1 {
				t.Fatal("key capacity fixture not refused")
			}
			if tc.refresh {
				if after := lossTestSnapshot(t, r, key); after.Generation != original.Generation || after.ReceiverEvidence() != view {
					t.Fatal("no-op receipt republished receiver population")
				}
			}
			if err := d.SetProcessElapsed(10 * time.Minute); err != nil {
				t.Fatal(err)
			}
			_, ok := r.Lookup(key)
			if ok != tc.refresh {
				t.Fatalf("idle freshness = %v, want %v", ok, tc.refresh)
			}
			if tc.refresh {
				if err := d.SetProcessElapsed(19 * time.Minute); err != nil {
					t.Fatal(err)
				}
				if _, ok := r.Lookup(key); ok {
					t.Fatal("receipt refreshed beyond its own exact deadline")
				}
			}
		})
	}
}

func TestResolverActiveRowlessLossBlocksObservationCleanupUntilExpiry(t *testing.T) {
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{MaxCandidatesPerKey: 1, InactiveTTL: 10 * time.Second})
	// The discarded future observation is still eligible after the retained
	// earlier observation expires. This is a real owner cap eviction/prune path.
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A1", at.Add(30*time.Second), time.Minute)
	lossTestAdmit(t, r, d, key, "K1ABD", "N0B1", at, time.Minute)
	generation := lossTestSnapshot(t, r, key).Generation
	d.Sweep(at.Add(61 * time.Second))
	s := lossTestSnapshot(t, r, key)
	if !s.EvidenceLoss || s.Generation != generation || s.TotalReporters != 0 || s.ReceiverEvidence().Len() != 0 || len(d.states) != 1 || len(d.states[key].candidates) != 0 {
		t.Fatalf("active rowless generation removed or tallied %+v", s)
	}
	d.Sweep(at.Add(90 * time.Second))
	if s = lossTestSnapshot(t, r, key); !s.EvidenceLoss {
		t.Fatal("inclusive lost-observation boundary cleared")
	}
	_, cell, _ := r.lookupSnapshot(key)
	d.Sweep(at.Add(90*time.Second + time.Nanosecond))
	if len(d.states) != 0 || !cell.terminal.Load() || cell.snapshot.Load() != nil {
		t.Fatal("expired empty generation retained references")
	}
	if _, ok := r.Lookup(key); ok {
		t.Fatal("expired generation still readable")
	}
}

func TestResolverLossGenerationsBoundedThroughMultipleRetirementBatches(t *testing.T) {
	r, d, _, at := lossTestDriver(t, SignalResolverConfig{MaxActiveKeys: 3, MaxCandidatesPerKey: 1})
	for batch := 0; batch < 5; batch++ {
		for index := 0; index < 3; index++ {
			frequency := 7010.1 + float64(index)
			key := NewResolverSignalKey(frequency, "40m", "CW", 500)
			for _, call := range []string{"K1ABC", "K1ABD"} {
				if !r.Enqueue(ResolverEvidence{Key: key, DXCall: call, Spotter: fmt.Sprintf("N0R%d", index), ObservedAt: at.Add(24 * time.Hour), FrequencyKHz: frequency, RecencyWindow: time.Hour}) {
					t.Fatal("enqueue bounded churn")
				}
				d.Drain()
			}
		}
		if len(d.states) != 3 || r.MetricsSnapshot().ActiveKeys != 3 {
			t.Fatal("live generation cap changed")
		}
		var cells []*resolverSnapshotCell
		r.snapshots.Range(func(_, value any) bool { cells = append(cells, value.(*resolverSnapshotCell)); return true })
		if len(cells) != 3 {
			t.Fatalf("secondary publication count %d", len(cells))
		}
		if err := d.SetProcessElapsed(time.Duration(batch+1) * 10 * time.Minute); err != nil {
			t.Fatal(err)
		}
		d.Sweep(at)
		if len(d.states) != 0 || r.MetricsSnapshot().ActiveKeys != 0 || r.MetricsSnapshot().PublishedReceiverRecords != 0 {
			t.Fatal("retirement did not converge to empty owners")
		}
		for _, cell := range cells {
			if !cell.terminal.Load() || cell.snapshot.Load() != nil {
				t.Fatal("retired cell retained publication")
			}
		}
	}
}

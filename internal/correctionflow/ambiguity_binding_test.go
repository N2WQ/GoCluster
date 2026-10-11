package correctionflow

import (
	"testing"
	"time"

	"dxcluster/spot"
)

func bindingResolver(t *testing.T) (*spot.SignalResolver, spot.ResolverSignalKey, *spot.SignalResolverDriver, time.Time) {
	t.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 64, HysteresisWindows: 1, EvalMinInterval: time.Nanosecond,
		SweepInterval: time.Nanosecond, FreqGuardMinSeparationKHz: .3, FreqGuardRunnerUpRatio: .6, MaxEditDistance: 6, DistanceModelCW: "morse"})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for _, row := range []struct {
		call, receiver string
		frequency      float64
	}{
		{"K1ABC", "N0AAA", 7010}, {"K1ABC", "N0BBB", 7010}, {"K1ABC", "N0CCC", 7010},
		{"K1ABD", "N0DDD", 7010.2}, {"K1ABD", "N0EEE", 7010.2},
		{"K1ABC", "N0FFF", 7010.5}, {"K1ABC", "N0GGG", 7010.5}, {"K1ABC", "N0HHH", 7010.5}, {"K1ABC", "N0III", 7010.5},
	} {
		if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: row.call, Spotter: row.receiver, FrequencyKHz: row.frequency, RecencyWindow: time.Minute}) {
			t.Fatal("enqueue")
		}
	}
	d.Drain()
	d.Sweep(at.Add(time.Millisecond))
	return r, key, d, at
}

func TestMethodAmbiguityExactAndTemporalBinding(t *testing.T) {
	r, key, d, at := bindingResolver(t)
	exact, ok := r.Lookup(key)
	if !ok || exact.State != spot.ResolverStateSplit {
		t.Fatalf("fixture is not an exact split: %+v", exact)
	}
	cfg := accountingConfig()
	s := SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABD")
	if !s.SnapshotOK || s.Snapshot.State != spot.ResolverStateSplit || s.Snapshot.Winner != "" {
		t.Errorf("neighborhood erased exact split: %+v", s)
	}
	for _, base := range []ResolverPrimarySelection{
		{Snapshot: exact, SnapshotOK: true},
		{Snapshot: exact, SnapshotOK: true, NeighborhoodSplit: true},
	} {
		transformed := selectionForTemporalWinner(base, "K1ABC")
		if transformed.Snapshot.State != spot.ResolverStateSplit || transformed.Snapshot.Winner != "" || ResolverConfidenceGlyphForCall(transformed.Snapshot, true, "K1ABD") != "P" {
			t.Errorf("temporal erased protection: %+v", transformed)
		}
	}
	flagOnly := ResolverPrimarySelection{SnapshotOK: true, NeighborhoodSplit: true,
		Snapshot: spot.ResolverSnapshot{State: spot.ResolverStateConfident, Winner: "K1ABC", TotalReporters: 5}}
	if protected := selectionForTemporalWinner(flagOnly, "K1ABC"); protected.Snapshot.State != spot.ResolverStateSplit || protected.Snapshot.Winner != "" || ResolverConfidenceGlyphForCall(protected.Snapshot, true, "K1ABD") != "P" {
		t.Fatalf("neighborhood flag-only protection erased: %+v", protected)
	}
	if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at.Add(time.Second), Key: key, DXCall: "K1ABC", Spotter: "N0JJJ", FrequencyKHz: 7010, RecencyWindow: time.Minute}) {
		t.Fatal("enqueue resolution")
	}
	d.Drain()
	d.Sweep(at.Add(time.Second + time.Millisecond))
	s = SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABD")
	if s.Snapshot.State == spot.ResolverStateSplit || s.Snapshot.Winner != "K1ABC" {
		t.Fatalf("changed exact population did not resolve: %+v", s)
	}
}

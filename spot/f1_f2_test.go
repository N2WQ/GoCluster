package spot

import (
	"testing"
	"time"
)

func TestF2QualifyingEqualReceiverSupportBlocksUnequalWeights(t *testing.T) {
	at := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	weights := SpotterReliability{"N0AAA": .95, "N0AAB": .95, "N0AAC": .95}
	r, d, key, _ := freshProjectionOwner(t, SignalResolverConfig{
		HysteresisWindows: 100, MinSpotterReliability: .95, SpotterReliability: weights,
		FreqGuardMinSeparationKHz: .1, FreqGuardRunnerUpRatio: .6, MaxEditDistance: 3,
	})
	for _, population := range []struct {
		call string
		rx   []string
	}{{"K1ABC", []string{"N0AAA", "N0AAB", "N0AAC"}}, {"K1ABD", []string{"N0BBA", "N0BBB", "N0BBC"}}} {
		for _, rx := range population.rx {
			if !r.Enqueue(ResolverEvidence{Key: key, DXCall: population.call, Spotter: rx,
				FrequencyKHz: 7010.05, ObservedAt: at, RecencyWindow: time.Minute}) {
				t.Fatal("fixture evidence refused")
			}
		}
	}
	d.Drain()
	d.Sweep(at.Add(time.Millisecond))
	s, ok := r.Lookup(key)
	if !ok || s.State != ResolverStateSplit || s.Winner != "" {
		t.Fatalf("qualifying raw 3:3 must block despite weights 2850:3000: %+v", s)
	}
	if s.TotalReporters != 6 || s.TotalWeightedSupportMilli != 5850 {
		t.Fatalf("tie protection discarded the population: %+v", s)
	}
}

func TestF1PendingPublicationCompactionAndCandidateLifetime(t *testing.T) {
	at := time.Date(2026, 10, 14, 15, 0, 0, 0, time.UTC)
	r, d, key, _ := freshProjectionOwner(t, SignalResolverConfig{HysteresisWindows: 100,
		MaxCandidatesPerKey: 2, SpotterReliability: SpotterReliability{"N0AAA": 0, "N0AAB": 0, "N0AAC": 0}})
	for _, rx := range []string{"N0AAA", "N0AAB", "N0AAC"} {
		freshProjectionObserve(t, r, d, key, "K1ABC", rx, at, 7010.05, time.Minute)
	}
	for _, rx := range []string{"N0BBA", "N0BBB", "N0BBC", "N0BBD", "N0BBE"} {
		freshProjectionObserve(t, r, d, key, "K1ABD", rx, at.Add(time.Second), 7010.05, time.Minute)
	}
	published, ok := r.Lookup(key)
	a, b, pending := published.PendingTransition()
	if !ok || !pending || a != "K1ABC" || b != "K1ABD" {
		t.Fatalf("zero-weight raw membership lost its owner pair: %+v", published)
	}
	compact := published.Compact()
	if x, y, present := compact.PendingTransition(); !present || x != a || y != b || compact.ReceiverEvidence() != nil {
		t.Fatal("compaction lost scalar provenance or retained receiver generation")
	}
	projected, available := r.LookupEligible(key, at.Add(time.Minute+time.Nanosecond))
	if !available || projected.State != ResolverStateUncertain || projected.TotalReporters != 5 {
		t.Fatalf("expiry must remove A without settling B: %+v", projected)
	}
	if x, y, present := projected.PendingTransition(); !present || x != a || y != b {
		t.Fatal("projection lost owner provenance")
	}
	st := d.states[key]
	if st.pendingWinner != b || st.stableWinner != a || st.pendingWins == 0 {
		t.Fatal("projection advanced or cleared owner transition")
	}
	// The third candidate evicts the smaller A population through the real cap
	// path. Its owner invalidation/control publication must clear the old pair.
	freshProjectionObserve(t, r, d, key, "K1ABE", "N0CCA", at.Add(2*time.Second), 7010.05, time.Minute)
	after, _ := r.Lookup(key)
	if _, _, present := after.PendingTransition(); present || st.pendingWinner != "" || st.pendingWins != 0 {
		t.Fatalf("candidate eviction retained stale pending authority: %+v", after)
	}
	if x, y, present := compact.PendingTransition(); !present || x != a || y != b {
		t.Fatal("owner mutation changed an older immutable compact publication")
	}
}

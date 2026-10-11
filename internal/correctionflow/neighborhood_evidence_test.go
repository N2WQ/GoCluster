package correctionflow

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func accountingResolver(tb testing.TB, buckets int, dissent string, overlap bool, weights spot.SpotterReliability) (*spot.SignalResolver, spot.ResolverSignalKey, *spot.SignalResolverDriver) {
	tb.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 128, HysteresisWindows: 1, EvalMinInterval: time.Nanosecond,
		SweepInterval: time.Nanosecond, SpotterReliability: weights,
	})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		tb.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for bucket := 0; bucket < buckets; bucket++ {
		for i, call := range []string{"K1ABC", "K1ABC", dissent} {
			receiver := []string{"N0AAA", "N0BBB", "N0CCC"}[i]
			if overlap && i == 2 {
				receiver = "N0AAA"
			}
			if !r.Enqueue(spot.ResolverEvidence{ObservedAt: now, Key: key, DXCall: call,
				Spotter: receiver, FrequencyKHz: 7010 + float64(bucket)*.5, RecencyWindow: time.Minute}) {
				tb.Fatal("enqueue accounting fixture")
			}
		}
	}
	d.Drain()
	d.Sweep(now.Add(time.Millisecond))
	return r, key, d
}

func accountingConfig() config.CallCorrectionConfig {
	return config.CallCorrectionConfig{ResolverNeighborhoodEnabled: true,
		ResolverNeighborhoodBucketRadius: 1, ResolverNeighborhoodMaxDistance: 1,
		FreqGuardRunnerUpRatio: .6, DistanceModelCW: "morse", DistanceModelRTTY: "baudot"}
}

func TestResolverNeighborhoodReceiverUnion(t *testing.T) {
	for _, tc := range []struct {
		name, dissent            string
		overlap                  bool
		weights                  spot.SpotterReliability
		total, weighted, percent int
	}{
		{"distinct dissent", "K1ABD", false, nil, 3, 3000, 66},
		{"unrelated losing dissent", "DL9ZZZ", false, nil, 3, 3000, 66},
		{"overlapping membership", "K1ABD", true, nil, 2, 2000, 100},
		{"weighted unrelated dissent", "DL9ZZZ", false, spot.SpotterReliability{"N0AAA": .9, "N0BBB": .95}, 3, 2850, 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, buckets := range []int{1, 2} {
				r, key, _ := accountingResolver(t, buckets, tc.dissent, tc.overlap, tc.weights)
				s := SelectResolverPrimarySnapshotForCall(r, key, accountingConfig(), "K1ABC").Snapshot
				if s.TotalReporters != tc.total || s.TotalWeightedSupportMilli != tc.weighted || ResolverSupportForCall(s, "K1ABC") != 2 || ResolverSupportForCall(s, tc.dissent) != 1 {
					t.Errorf("%d buckets: counts=%+v; want A=2 dissent=1 total=%d weight=%d", buckets, s, tc.total, tc.weighted)
				}
				if p, ok := ResolverCallConfidencePercent(s, "K1ABC"); !ok || p != tc.percent {
					t.Errorf("%d buckets: A percentage=%d/%t, want %d", buckets, p, ok, tc.percent)
				}
			}
		})
	}
}

func TestResolverNeighborhoodExcludesUnrelatedWinningBucket(t *testing.T) {
	r, key, d := accountingResolver(t, 1, "K1ABD", false, nil)
	now := time.Date(2026, 10, 10, 12, 0, 1, 0, time.UTC)
	for _, receiver := range []string{"N0DDD", "N0EEE", "N0FFF"} {
		if !r.Enqueue(spot.ResolverEvidence{ObservedAt: now, Key: key, DXCall: "DL9ZZZ", Spotter: receiver,
			FrequencyKHz: 7010.5, RecencyWindow: time.Minute}) {
			t.Fatal("enqueue unrelated winner")
		}
	}
	d.Drain()
	d.Sweep(now.Add(time.Millisecond))
	s := SelectResolverPrimarySnapshotForCall(r, key, accountingConfig(), "K1ABC")
	if s.Snapshot.TotalReporters != 3 || ResolverSupportForCall(s.Snapshot, "DL9ZZZ") != 0 || s.NeighborhoodExcludedUnrelated+s.NeighborhoodExcludedDistance != 1 {
		t.Fatalf("unrelated winning bucket entered population: %+v", s)
	}
}

func TestResolverNeighborhoodThirdCandidate(t *testing.T) {
	r, key, d := accountingResolver(t, 2, "K1ABD", false, spot.SpotterReliability{"N0AAA": .9, "N0BBB": .95})
	at := time.Date(2026, 10, 10, 12, 0, 1, 0, time.UTC)
	for _, frequency := range []float64{7010, 7010.5} {
		if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, DXCall: "DL9ZZZ", Spotter: "N0EEE", FrequencyKHz: frequency, Key: key, RecencyWindow: time.Minute}) {
			t.Fatal("enqueue third candidate")
		}
	}
	d.Drain()
	d.Sweep(at.Add(time.Millisecond))
	s := SelectResolverPrimarySnapshotForCall(r, key, accountingConfig(), "K1ABC")
	if s.Snapshot.TotalReporters != 4 || s.Snapshot.TotalWeightedSupportMilli != 3850 || len(s.Snapshot.CandidateRanks) != 3 {
		t.Fatalf("third candidate accounting: %+v", s)
	}
	for call, want := range map[string]int{"K1ABC": 2, "K1ABD": 1, "DL9ZZZ": 1} {
		if got := ResolverSupportForCall(s.Snapshot, call); got != want {
			t.Errorf("%s support=%d want %d", call, got, want)
		}
	}
	if p, ok := ResolverCallConfidencePercent(s.Snapshot, "K1ABC"); !ok || p != 48 || s.allowsReplacement("DL9ZZZ") {
		t.Fatalf("third-candidate weight or replacement eligibility: %d/%t %+v", p, ok, s)
	}
}

func TestResolverNeighborhoodExactFallbackClearsEligibility(t *testing.T) {
	r, key, d := accountingResolver(t, 1, "DL9ZZZ", false, nil)
	at := time.Date(2026, 10, 10, 12, 0, 1, 0, time.UTC)
	for _, row := range []struct {
		call, receiver string
		frequency      float64
	}{
		{"DL9ZZZ", "N0DDD", 7010}, {"DL9ZZZ", "N0EEE", 7010},
		{"K1ABC", "N0FFF", 7010.5}, {"K1ABC", "N0GGG", 7010.5}, {"K1ABC", "N0HHH", 7010.5},
		{"K1ABC", "N0III", 7010.5},
	} {
		if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: row.call, Spotter: row.receiver, FrequencyKHz: row.frequency, RecencyWindow: time.Minute}) {
			t.Fatal("enqueue exact fallback")
		}
	}
	d.Drain()
	d.Sweep(at.Add(time.Millisecond))
	s := SelectResolverPrimarySnapshotForCall(r, key, accountingConfig(), "K1ABD")
	if s.Snapshot.Winner != "DL9ZZZ" || s.WinnerOverride || s.replacementCalls != nil {
		t.Fatalf("exact fallback retained synthesized eligibility: %+v", s)
	}
}

func BenchmarkResolverNeighborhoodEvidence(b *testing.B) {
	r, key, _ := accountingResolver(b, 2, "K1ABD", false, nil)
	cfg := accountingConfig()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABC")
	}
}

func BenchmarkResolverNeighborhoodEvidenceCapped(b *testing.B) {
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 16384, MaxCandidatesPerKey: 16, MaxReportersPerCand: 64,
		EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, HysteresisWindows: 1})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		b.Fatal(err)
	}
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for bucket := -2; bucket <= 2; bucket++ {
		for candidate := 0; candidate < 16; candidate++ {
			for receiver := 0; receiver < 64; receiver++ {
				if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: fmt.Sprintf("K1A%02d", candidate),
					Spotter: fmt.Sprintf("N0A%02d", receiver), FrequencyKHz: 7010 + float64(bucket)*.5, RecencyWindow: time.Minute}) {
					b.Fatal("enqueue capped fixture")
				}
			}
		}
	}
	d.Drain()
	d.Sweep(at.Add(time.Millisecond))
	cfg := accountingConfig()
	cfg.ResolverNeighborhoodBucketRadius = 2
	selection := SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1A00")
	for bucket := -2; bucket <= 2; bucket++ {
		bucketKey := spot.NewResolverSignalKey(7010+float64(bucket)*.5, "40m", "CW", 500)
		published, ok := r.Lookup(bucketKey)
		if !ok || published.ReceiverEvidence().Len() != 1024 || published.Winner != "K1A00" {
			b.Fatalf("capped bucket fixture: %+v", published)
		}
	}
	if !selection.UsedNeighborhood || selection.CandidateCount != 16 || selection.Snapshot.TotalReporters != 64 {
		b.Fatalf("capped fixture: %+v", selection)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1A00")
	}
}

func TestTemporalSelectionCompaction(t *testing.T) {
	r, key, _ := accountingResolver(t, 1, "K1ABD", false, nil)
	snapshot, _ := r.Lookup(key)
	if snapshot.ReceiverEvidence().Len() != 3 {
		t.Fatal("fixture has no receiver payload")
	}
	decoder := NewTemporalDecoder(config.CallCorrectionConfig{TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
		Enabled: true, Scope: "all_correction_candidates", LagSeconds: 1, MaxWaitSeconds: 2,
		MaxPending: 8, MaxActiveKeys: 2, MaxEventsPerKey: 1,
	}})
	for id := uint64(1); id <= 4; id++ {
		if ok, reason := decoder.Observe(TemporalObservation{ID: id, Key: key, SubjectCall: "K1ABD",
			ObservedAt: snapshot.EvaluatedAt, Selection: ResolverPrimarySelection{Snapshot: snapshot, SnapshotOK: true}}); !ok {
			t.Fatalf("Observe: %s", reason)
		}
		st := decoder.keyStates[key]
		if len(st.events) != 1 || st.events[0].selection.Snapshot.ReceiverEvidence() != nil || len(st.events[0].selection.Snapshot.CandidateRanks) != 2 {
			t.Fatalf("temporal retained receiver payload or lost scoring ranks: %+v", st.events)
		}
	}
}

func TestTemporalNeighborhoodRetainsDissentWithoutPromotingIneligibleCall(t *testing.T) {
	r, key, d := accountingResolver(t, 1, "DL9ZZZ", false, nil)
	at := time.Date(2026, 10, 10, 12, 0, 1, 0, time.UTC)
	// Every bucket is A2 / B1, but B's receivers are distinct across buckets.
	for offset, receiver := range map[int]string{-1: "N0DDD", 1: "N0EEE"} {
		for _, report := range []struct{ call, receiver string }{{"K1ABC", "N0AAA"}, {"K1ABC", "N0BBB"}, {"DL9ZZZ", receiver}} {
			if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: report.call, Spotter: report.receiver,
				FrequencyKHz: 7010 + float64(offset)*.5, RecencyWindow: time.Minute}) {
				t.Fatal("enqueue")
			}
		}
	}
	d.Drain()
	d.Sweep(at.Add(time.Millisecond))
	selection := SelectResolverPrimarySnapshotForCall(r, key, accountingConfig(), "K1ABC")
	if selection.Snapshot.Winner != "K1ABC" || ResolverSupportForCall(selection.Snapshot, "DL9ZZZ") != 3 || selection.Snapshot.TotalReporters != 5 {
		t.Fatalf("dissent missing or promoted: %+v", selection)
	}
	decoder := NewTemporalDecoder(config.CallCorrectionConfig{TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{MaxObsCandidates: 4}})
	for _, candidate := range decoder.candidatesForEvent(temporalEvent{subject: "K1ABC", selection: selection}, "K1ABC") {
		if candidate == "DL9ZZZ" {
			t.Fatal("ineligible dissent became a temporal replacement")
		}
	}
}

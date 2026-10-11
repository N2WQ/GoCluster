package cluster

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func benchmarkTelnetFamilyPolicy() spot.CorrectionFamilyPolicy {
	return spot.CorrectionFamilyPolicy{
		Configured: true, TruncationEnabled: true, TruncationMaxLengthDelta: 1,
		TruncationMinShorterLength: 3, TruncationAllowPrefix: true, TruncationAllowSuffix: true,
	}
}

func benchmarkCallCorrectionConfig() config.CallCorrectionConfig {
	return telnetFamilyTestConfig()
}

func benchmarkFamilyCall(index int) string {
	return fmt.Sprintf("K%d%c%c%c", index%10, 'A'+rune((index/10)%26),
		'A'+rune((index/260)%26), 'A'+rune((index/6760)%26))
}

func benchmarkMakeSpots(count int, startFreq, step float64) []*spot.Spot {
	spots := make([]*spot.Spot, count)
	for i := range spots {
		spots[i] = spot.NewSpot(benchmarkFamilyCall(i), "W0BENCH", startFreq+float64(i)*step, "CW")
	}
	return spots
}

// Build actual immutable owner publications at the default 16-candidate cap.
// A has64, B16, and each of14 dissenting candidates has1 receiver per key.
// Half of A's observations are ten seconds old so a later lookup projects them
// out. Several keys share A/B receivers but retain their independent dissent.
func benchmarkFamilyResolver(tb testing.TB, keys int) (*spot.SignalResolver, spot.ResolverSignalKey, config.CallCorrectionConfig, time.Time) {
	tb.Helper()
	cfg := benchmarkCallCorrectionConfig()
	cfg.ResolverNeighborhoodEnabled = keys > 1
	cfg.ResolverNeighborhoodBucketRadius = 2
	cfg.FreqGuardRunnerUpRatio = 0.6
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 1024, MaxActiveKeys: keys, MaxCandidatesPerKey: 16, MaxReportersPerCand: 64,
		InactiveTTL: time.Hour, EvalMinInterval: time.Nanosecond, SweepInterval: time.Hour,
		HysteresisWindows: 1, FreqGuardMinSeparationKHz: 0.2, FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance: 1, DistanceModelCW: "morse", DistanceModelRTTY: "baudot",
		FamilyPolicy: benchmarkTelnetFamilyPolicy(),
	})
	tb.Cleanup(r.Stop)
	base := time.Now().UTC()
	exactKey := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	accepted := uint64(0)
	for keyIndex := 0; keyIndex < keys; keyIndex++ {
		offset := 0
		if keys > 1 {
			offset = keyIndex - 2
		}
		frequency := 7010 + float64(offset)*0.5
		key := spot.NewResolverSignalKey(frequency, "40m", "CW", 500)
		enqueue := func(call, receiver string, at time.Time) {
			tb.Helper()
			if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: call, Spotter: receiver,
				FrequencyKHz: frequency, RecencyWindow: 20 * time.Second}) {
				tb.Fatal("bounded benchmark resolver refused setup evidence")
			}
			accepted++
		}
		for i := 0; i < 32; i++ {
			enqueue("W1AB", fmt.Sprintf("R%03d", i), base.Add(-10*time.Second+time.Duration(i)*time.Microsecond))
		}
		for i := 32; i < 64; i++ {
			enqueue("W1AB", fmt.Sprintf("R%03d", i), base.Add(time.Duration(i)*time.Microsecond))
		}
		for i := 0; i < 16; i++ {
			enqueue("W1ABC", fmt.Sprintf("R%03d", i+64), base.Add(time.Duration(i+64)*time.Microsecond))
		}
		for i := 0; i < 14; i++ {
			enqueue(fmt.Sprintf("N%dZ%02d", keyIndex, i), fmt.Sprintf("D%dR%02d", keyIndex, i), base.Add(time.Duration(i+80)*time.Microsecond))
		}
	}
	r.Start()
	deadline := time.Now().Add(5 * time.Second)
	for r.MetricsSnapshot().Processed < accepted {
		if time.Now().After(deadline) {
			tb.Fatal("benchmark resolver did not process bounded setup")
		}
		time.Sleep(time.Millisecond)
	}
	// Joining the owner makes the final Processed increment and publication
	// ordering explicit; no owner work or real clock drift enters the timing.
	r.Stop()
	clock := base.Add(time.Second)
	selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, exactKey, cfg, "W1ABC", clock)
	if !selection.SnapshotOK || selection.NeighborhoodSplit || selection.Snapshot.EvidenceLoss ||
		(selection.Snapshot.State != spot.ResolverStateConfident && selection.Snapshot.State != spot.ResolverStateProbable) {
		tb.Fatalf("benchmark population is unavailable: %+v", selection)
	}
	wantCandidates := 16
	if keys > 1 {
		wantCandidates = 2 + 14*keys
	}
	if len(selection.Snapshot.CandidateRanks) != wantCandidates {
		tb.Fatalf("candidate ranks=%d, want bounded %d", len(selection.Snapshot.CandidateRanks), wantCandidates)
	}
	if stats := r.MetricsSnapshot(); stats.DropQueueFull != 0 || stats.DropMaxCandidates != 0 || stats.DropMaxReporters != 0 {
		tb.Fatalf("benchmark fixture discarded evidence: %+v", stats)
	}
	return r, exactKey, cfg, base
}

func benchmarkWarmFamilyHistory(b *testing.B, entries int, cfg config.CallCorrectionConfig, clock time.Time) *telnetFamilySuppressor {
	b.Helper()
	s := newTelnetFamilySuppressor(time.Minute, entries, benchmarkTelnetFamilyPolicy(), 500)
	all := familyLanes{true, true, true}
	publish := func(familyLanes) bool { return true }
	for _, sp := range benchmarkMakeSpots(entries-2, 7010, 0) {
		if got := s.handoff(sp, cfg, clock, all, nil, publish); got != all {
			b.Fatal("dense setup unexpectedly hidden")
		}
	}
	for _, call := range []string{"W1AB", "W1ABC"} {
		if got := s.handoff(spot.NewSpot(call, "W0BENCH", 7010, "CW"), cfg, clock, all, nil, publish); got != all {
			b.Fatal("pair setup unexpectedly hidden")
		}
	}
	telnetFamilyState(b, s, entries)
	return s
}

func BenchmarkTelnetFamilySuppressorDenseHistory(b *testing.B) {
	const entries = 20000
	for _, tc := range []struct {
		name, call string
		admitted   bool
		want       familyLanes
	}{
		{"hidden", "W1ABC", true, familyLanes{}},
		{"admitted_repeat", "W1AB", true, familyLanes{true, true, true}},
		{"refused_repeat", "W1AB", false, familyLanes{true, true, true}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			resolver, key, cfg, base := benchmarkFamilyResolver(b, 1)
			clock := base.Add(time.Second)
			s := benchmarkWarmFamilyHistory(b, entries, cfg, clock)
			incoming := spot.NewSpot(tc.call, "W0BENCH", 7010, "CW")
			selected, published := 0, 0
			selectEvidence := func(at time.Time) correctionflow.ResolverPrimarySelection {
				selected++
				return correctionflow.SelectResolverPrimarySnapshotForCallAt(resolver, key, cfg, incoming.DXCallNorm, at)
			}
			publish := func(lanes familyLanes) bool {
				published++
				if lanes != tc.want {
					b.Fatalf("publisher mask=%+v want %+v", lanes, tc.want)
				}
				return tc.admitted
			}
			// Fixed eligibility time and repeated existing identities retain
			// exactly20,000 fresh entries for every measured operation.
			b.ReportAllocs()
			b.ResetTimer()
			b.ReportMetric(entries, "history_entries")
			b.ReportMetric(16, "candidate_ranks")
			for i := 0; i < b.N; i++ {
				if got := s.handoff(incoming, cfg, clock, familyLanes{true, true, true}, selectEvidence, publish); got != tc.want {
					b.Fatalf("attempt mask=%+v want %+v", got, tc.want)
				}
			}
			b.StopTimer()
			wantPublished := b.N
			if tc.want == (familyLanes{}) {
				wantPublished = 0
			}
			if selected != b.N || published != wantPublished {
				b.Fatalf("selections/publications=%d/%d want%d/%d", selected, published, b.N, wantPublished)
			}
			telnetFamilyState(b, s, entries)
		})
	}
}

func BenchmarkTelnetFamilySuppressorAtCapacityEvict(b *testing.B) {
	const entries = 2048
	cfg := benchmarkCallCorrectionConfig()
	s := newTelnetFamilySuppressor(time.Minute, entries, benchmarkTelnetFamilyPolicy(), 500)
	pool := benchmarkMakeSpots(entries*2, 7000, 1)
	clock := time.Unix(1700000000, 0).UTC()
	all := familyLanes{true, true, true}
	publish := func(familyLanes) bool { return true }
	for i := 0; i < entries; i++ {
		s.handoff(pool[i], cfg, clock, all, nil, publish)
	}
	index := entries
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := s.handoff(pool[index], cfg, clock, all, nil, publish); got != all {
			b.Fatal("capacity churn unexpectedly hidden")
		}
		index++
		if index == len(pool) {
			index = 0
		}
	}
	b.StopTimer()
	telnetFamilyState(b, s, entries)
}

// Selection-only costs are reported separately from dense history arbitration.
// The five-key case has72 real union candidates, not one candidate per entry.
func BenchmarkTelnetFamilySuppressorFreshSelection(b *testing.B) {
	for _, tc := range []struct {
		name                          string
		keys                          int
		delta                         time.Duration
		wantReceivers, wantCandidates int
	}{
		{"all_fresh_capped16", 1, time.Second, 94, 16},
		{"partial_expiry_capped16", 1, 15 * time.Second, 62, 16},
		{"neighborhood_five_keys", 5, time.Second, 150, 72},
	} {
		b.Run(tc.name, func(b *testing.B) {
			resolver, key, cfg, base := benchmarkFamilyResolver(b, tc.keys)
			clock := base.Add(tc.delta)
			selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(resolver, key, cfg, "W1ABC", clock)
			if !selection.SnapshotOK || selection.NeighborhoodSplit || selection.Snapshot.EvidenceLoss ||
				selection.Snapshot.TotalReporters != tc.wantReceivers || len(selection.Snapshot.CandidateRanks) != tc.wantCandidates {
				b.Fatalf("fresh projection fixture mismatched: %+v", selection)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.ReportMetric(float64(tc.wantCandidates), "candidate_ranks")
			b.ReportMetric(float64(tc.keys), "resolver_keys")
			for i := 0; i < b.N; i++ {
				got := correctionflow.SelectResolverPrimarySnapshotForCallAt(resolver, key, cfg, "W1ABC", clock)
				if !got.SnapshotOK || got.NeighborhoodSplit || got.Snapshot.TotalReporters != tc.wantReceivers {
					b.Fatal("timed selection lost its declared population")
				}
			}
		})
	}
}

func BenchmarkTelnetFamilySuppressorConcurrentAdmission(b *testing.B) {
	resolver, key, cfg, base := benchmarkFamilyResolver(b, 1)
	clock := base.Add(time.Second)
	s := benchmarkWarmFamilyHistory(b, 64, cfg, clock)
	incoming := spot.NewSpot("W1AB", "W0BENCH", 7010, "CW")
	all := familyLanes{true, true, true}
	selectEvidence := func(at time.Time) correctionflow.ResolverPrimarySelection {
		return correctionflow.SelectResolverPrimarySnapshotForCallAt(resolver, key, cfg, incoming.DXCallNorm, at)
	}
	publications := 0 // Existing family mutex owns this callback and counter.
	publish := func(lanes familyLanes) bool {
		publications++
		if lanes != all {
			b.Errorf("concurrent publisher mask=%+v", lanes)
		}
		return publications%4 != 0
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if got := s.handoff(incoming, cfg, clock, all, selectEvidence, publish); got != all {
				b.Errorf("concurrent mask=%+v", got)
			}
		}
	})
	b.StopTimer()
	if publications != b.N {
		b.Fatalf("concurrent publications=%d want%d", publications, b.N)
	}
	telnetFamilyState(b, s, 64)
}

package cluster

import (
	"testing"
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestCWDecayTerminalGateEvidence(t *testing.T) {
	cfg := selectiveLiveConfig()
	cfg.MinConfidencePercent = 60
	p := outputPipeline{correctionCfg: cfg}
	key := spot.NewResolverSignalKey(7030, "40m", "CW", 400)
	s := spot.NewSpotNormalized("W1ABC", "K2XYZ", 7030, "CW")
	at := time.Unix(1900000000, 0)
	for _, test := range []struct {
		name, winner string
		weight       int
		keep         bool
	}{
		{"same_call", "W1ABC", 2000, true},
		{"max_distance", "JA1XYZ", 2000, true},
		{"low_confidence", "W1ABD", 1700, true},
		{"same_nearby_leader_with_adequate_confidence", "W1ABD", 2000, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := spot.ResolverSnapshot{Key: key, Winner: test.winner, State: spot.ResolverStateProbable,
				WinnerSupport: 2, TotalReporters: 3, WinnerWeightedSupportMilli: test.weight, TotalWeightedSupportMilli: 3000,
				SupportBasis: spot.ResolverSupportBasisWeighted, CandidateRanks: []spot.ResolverCandidateSupport{
					{Call: test.winner, Support: 2, WeightedSupportMilli: test.weight},
				}}
			if got := p.cwDecayKeepsIdentity(s, key, correctionflow.ResolverPrimarySelection{Snapshot: snapshot, SnapshotOK: true}, at); got != test.keep {
				t.Fatalf("keep=%t, want %t", got, test.keep)
			}
		})
	}
}

func TestCWDecayActualStabilizerCanOverridePrior(t *testing.T) {
	cfg := selectiveLiveConfig()
	cfg.SelectiveRescueEnabled, cfg.StabilizerEnabled = false, true
	cfg.MaxEditDistance = 1
	p := outputPipeline{correctionCfg: cfg, recentBandStore: spot.NewRecentBandStore(time.Minute)}
	p.signalResolver = spot.NewSignalResolver(spot.SignalResolverConfig{CWDecayComparison: true, HysteresisWindows: 1})
	d, err := spot.NewSignalResolverDriver(p.signalResolver)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7030, "40m", "CW", cfg.FrequencyToleranceHz)
	now := time.Unix(1900000000, 0)
	for i, label := range []string{"JA1XYZ", "JA1XYZ", "JA1XYZ", "W1ABC", "W1ABC"} {
		e := spot.ResolverEvidence{Key: key, ObservedAt: now.Add(-time.Second), FrequencyKHz: 7030, DXCall: label,
			Spotter: []string{"N0AAA", "N0BBB", "N0CCC", "N0DDD", "N0EEE"}[i], RecencyWindow: time.Minute}
		if !p.signalResolver.Enqueue(e) {
			t.Fatal("fixture lost evidence")
		}
	}
	d.Drain()
	d.Sweep(now)
	prior := p.signalResolver.CaptureCWDecayPrior(key)
	for _, test := range []struct {
		call, wantGlyph string
		pass            bool
	}{
		{"W1ABC", "P", false}, {"JA1XYZ", "V", true},
	} {
		s := spot.NewSpotNormalized(test.call, "K2XYZ", 7030, "CW")
		ctx := outputSpotContext{spot: s, cwDecayPrior: prior}
		at := now.Add(time.Second)
		p.stabilizerEnabled = false
		snap, ok := p.cwDecayRetainsImmediate(&ctx, key, at)
		if !ok || resolverConfidenceGlyph(snap, true, test.call) != test.wantGlyph {
			t.Fatalf("%s: missing pinned original with glyph %s: %+v", test.call, test.wantGlyph, snap)
		}
		setResolverConfidence(s, snap, true, test.call)
		fresh := spot.ResolverSnapshot{Key: key, State: spot.ResolverStateSplit}
		wouldHold := evaluateTelnetStabilizerDelay(s, p.recentBandStore, cfg, at, fresh, true).ShouldDelay
		p.stabilizerEnabled = true
		_, ok = p.cwDecayRetainsImmediate(&ctx, key, at)
		if ok != test.pass || ok == wouldHold {
			t.Fatalf("%s glyph %s: certified %v vs actual later ambiguous publication hold %v", test.call, test.wantGlyph, ok, wouldHold)
		}
	}
}

func TestCWDecaySettledNeighborhoodRetention(t *testing.T) {
	cfg := selectiveLiveConfig()
	cfg.SelectiveRescueEnabled, cfg.ResolverNeighborhoodEnabled = false, true
	cfg.ResolverNeighborhoodMaxDistance, cfg.ResolverNeighborhoodBucketRadius = 1, 1
	p := outputPipeline{correctionCfg: cfg}
	p.signalResolver = spot.NewSignalResolver(spot.SignalResolverConfig{CWDecayComparison: true, HysteresisWindows: 1})
	d, err := spot.NewSignalResolverDriver(p.signalResolver)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7030, "40m", "CW", cfg.FrequencyToleranceHz)
	now := time.Unix(1900000000, 0)
	for i, receiver := range []string{"N0AAA", "N0BBB"} {
		k := key
		k.Bucket += int64(i)
		if !p.signalResolver.Enqueue(spot.ResolverEvidence{Key: k, DXCall: "W1ABC", Spotter: receiver,
			ObservedAt: now.Add(-time.Second), FrequencyKHz: 7030 + float64(i)*cfg.FrequencyToleranceHz/1000, RecencyWindow: time.Minute}) {
			t.Fatal("fixture lost evidence")
		}
	}
	d.Drain()
	d.Sweep(now)
	s := spot.NewSpotNormalized("W1ABC", "K2XYZ", 7030, "CW")
	ctx := outputSpotContext{spot: s, cwDecayPrior: p.signalResolver.CaptureCWDecayPrior(key)}
	snap, ok := p.cwDecayRetainsImmediate(&ctx, key, now.Add(time.Second))
	if !ok || snap.TotalReporters != 2 || snap.ReceiverEvidence() != nil || snap.Winner != "W1ABC" {
		t.Fatalf("expected admitted two-slot merged veto: ok=%t %+v", ok, snap)
	}
	ctx.cwDecayPrior = spot.CWDecayPrior{}
	if _, ok := p.cwDecayRetainsImmediate(&ctx, key, now.Add(time.Second)); ok {
		t.Fatal("adjacent evidence overrode a missing exact publication")
	}
	for _, state := range []spot.ResolverState{spot.ResolverStateSplit, spot.ResolverStateUncertain} {
		snap.State = state
		if p.cwDecayKeepsIdentity(s, key, correctionflow.ResolverPrimarySelection{Snapshot: snap, SnapshotOK: true}, now) {
			t.Fatalf("state %s certified an immediate retention", state)
		}
	}
	snap.State = spot.ResolverStateConfident
	if p.cwDecayKeepsIdentity(s, key, correctionflow.ResolverPrimarySelection{Snapshot: snap, SnapshotOK: true, NeighborhoodSplit: true}, now) {
		t.Fatal("split neighborhood certified an immediate retention")
	}
}

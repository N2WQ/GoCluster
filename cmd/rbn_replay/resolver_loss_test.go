// File role: Checks resolver-only replay's actual cap-loss rejection and fresh
// recovery, preserving the retained identity and accepted-only correction count.
package main

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
)

func TestReplayResolverLossRetainsAndRecovers(t *testing.T) {
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 32, MaxCandidatesPerKey: 2,
		MaxReportersPerCand: 8, EvalMinInterval: time.Nanosecond, HysteresisWindows: 1})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	admit := func(call string, observed time.Time, receivers ...string) {
		t.Helper()
		for _, receiver := range receivers {
			if !r.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: observed, DXCall: call,
				Spotter: receiver, FrequencyKHz: 7010, RecencyWindow: time.Minute}) {
				t.Fatal("fixture enqueue")
			}
		}
		if d.Drain() != len(receivers) {
			t.Fatal("fixture owner admission")
		}
		// Each batch uses a distinct owner evaluation instant while all report
		// timestamps stay literal; the fixture must publish its final population.
		d.Sweep(observed.Add(time.Duration(r.MetricsSnapshot().Processed+1) * time.Nanosecond))
	}
	admit("K1ABC", at, "N0AAA")
	admit("K1ABD", at, "N0BBB", "N0CCC")
	admit("K1ABE", at, "N0DDD")
	lost, ok := r.LookupEligible(key, at)
	if !ok || !lost.EvidenceLoss || lost.TotalReporters != 3 ||
		correctionflow.ResolverSupportForCall(lost, "K1ABC") != 0 ||
		correctionflow.ResolverSupportForCall(lost, "K1ABD") != 2 || r.MetricsSnapshot().EvictedCandidates != 1 {
		t.Fatalf("fixture must actually discard A1 from B2/C1: %+v", lost)
	}
	cfg := config.CallCorrectionConfig{Enabled: true, RecencySeconds: 60, FrequencyToleranceHz: 500,
		MaxEditDistance: 1, MinConsensusReports: 2, MinAdvantage: 1, MinConfidencePercent: 60,
		DistanceModelCW: "plain", DistanceModelRTTY: "plain", InvalidAction: "broadcast"}
	tracker := stats.NewTracker()
	s := spot.NewSpotNormalized("K1ABC", "N0AAA", 7010, "CW")
	s.Time = at
	s.ConfidencePercent, s.ConfidencePercentOK = 77, true
	retained := maybeApplyResolverCorrectionReplay(s, r, spot.ResolverEvidence{Key: key}, true,
		cfg, nil, tracker, nil, nil, at)
	if retained.Applied || retained.Suppress || s.DXCall != "K1ABC" || s.Confidence != "?" ||
		s.ConfidencePercent != 0 || s.ConfidencePercentOK || tracker.CallCorrections() != 0 ||
		tracker.CorrectionDecisionReasons()["resolver_evidence_loss"] != 1 {
		t.Fatalf("loss must retain A without consensus promotion/share/acceptance: spot=%+v outcome=%+v", s, retained)
	}
	// The same live generation recovers after genuine evidence expiry. Process
	// elapsed stays zero and the evidence horizon stays one minute throughout.
	freshAt := at.Add(61 * time.Second)
	admit("K1ABD", freshAt, "N0BBB", "N0CCC", "N0EEE")
	admit("K1ABC", freshAt, "N0AAA")
	fresh, ok := r.LookupEligible(key, freshAt)
	if !ok || fresh.EvidenceLoss || fresh.Generation != lost.Generation || fresh.TotalReporters != 4 ||
		fresh.Winner != "K1ABD" || r.MetricsSnapshot().EvictedCandidates != 1 {
		t.Fatalf("same-generation genuine B3/A1 recovery, no new discard: %+v", fresh)
	}
	s = spot.NewSpotNormalized("K1ABC", "N0AAA", 7010, "CW")
	s.Time = freshAt
	accepted := maybeApplyResolverCorrectionReplay(s, r, spot.ResolverEvidence{Key: key}, true,
		cfg, nil, tracker, nil, nil, freshAt)
	if !accepted.Applied || accepted.Suppress || s.DXCall != "K1ABD" || s.Confidence != "C" ||
		tracker.CallCorrections() != 1 || correctionflow.ResolverWinnerConfidence(fresh) != 75 {
		t.Fatalf("fresh independent gates must accept B at its literal 3/4 share: spot=%+v outcome=%+v", s, accepted)
	}
}

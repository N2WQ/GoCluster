// File role: Prevents the offline evaluator from authorizing correction with
// a known cap-incomplete retained population, matching shared live/replay policy.
package main

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestRevengResolverLossDecision(t *testing.T) {
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 16,
		MaxCandidatesPerKey: 2, EvalMinInterval: time.Nanosecond, HysteresisWindows: 1})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for _, row := range []struct{ call, receiver string }{
		{"K1ABC", "N0AAA"}, {"K1ABD", "N0BBB"}, {"K1ABD", "N0CCC"}, {"K1ABE", "N0DDD"},
	} {
		if !r.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: at, DXCall: row.call,
			Spotter: row.receiver, FrequencyKHz: 7010, RecencyWindow: time.Minute}) {
			t.Fatal("fixture enqueue")
		}
	}
	if d.Drain() != 4 {
		t.Fatal("fixture owner barrier")
	}
	d.Sweep(at.Add(time.Millisecond))
	snapshot, ok := r.Lookup(key)
	if !ok || !snapshot.EvidenceLoss || snapshot.TotalReporters != 3 || r.MetricsSnapshot().EvictedCandidates != 1 {
		t.Fatalf("actual candidate-cap fixture: %+v", snapshot)
	}
	st := &runState{resolver: r, cfg: &config.Config{CallCorrection: config.CallCorrectionConfig{
		Enabled: true, MaxEditDistance: 1, MinConsensusReports: 2, MinAdvantage: 1,
		MinConfidencePercent: 60, RecencySeconds: 60, FrequencyToleranceHz: 500,
		DistanceModelCW: "plain", DistanceModelRTTY: "plain", InvalidAction: "broadcast",
	}}, counters: primaryCounters{RejectedReasonCounts: make(map[string]int64)}}
	s := spot.NewSpotNormalized("K1ABC", "N0AAA", 7010, "CW")
	decision := evaluateResolverPrimaryDecision(st, s, spot.ResolverEvidence{Key: key}, true, "K1ABC", at)
	if decision.Applied || decision.Reason != "resolver_evidence_loss" || st.counters.DecisionTotal != 1 ||
		st.counters.Rejected != 1 || st.counters.RejectedReasonCounts["resolver_evidence_loss"] != 1 || s.DXCall != "K1ABC" {
		t.Fatalf("offline loss rejection/identity accounting: decision=%+v counters=%+v", decision, st.counters)
	}
}

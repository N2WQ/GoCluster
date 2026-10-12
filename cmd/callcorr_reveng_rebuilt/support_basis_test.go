// File role: Verifies offline correction gates consume actual zero-weight
// populations consistently while retaining their existing result format.
package main

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestRevengSupportBasisZeroPopulation(t *testing.T) {
	for _, mode := range []string{"CW", "RTTY"} {
		for _, allZero := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/all_zero_%t", mode, allZero), func(t *testing.T) {
				at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
				weights := spot.SpotterReliability{"N0AAA": 0, "N0BBB": 0, "N0CCC": 1}
				if allZero {
					weights["N0CCC"] = 0
				}
				r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 8, HysteresisWindows: 1, EvalMinInterval: time.Nanosecond, SpotterReliability: weights})
				t.Cleanup(r.Stop)
				d, err := spot.NewSignalResolverDriver(r)
				if err != nil {
					t.Fatal(err)
				}
				key := spot.NewResolverSignalKey(14020, "20m", mode, 500)
				for _, row := range []struct{ call, receiver string }{{"K1ABC", "N0AAA"}, {"K1ABC", "N0BBB"}, {"K1ABD", "N0CCC"}} {
					if !r.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: at, DXCall: row.call, Spotter: row.receiver, FrequencyKHz: 14020, RecencyWindow: time.Minute}) {
						t.Fatal("offline support fixture admission failed")
					}
				}
				d.Drain()
				d.Sweep(at.Add(time.Millisecond))
				snapshot, ok := r.Lookup(key)
				weight, reason := 1000, "resolver_gate_min_reports"
				if allZero {
					weight, reason = 0, "resolver_state_uncertain"
				}
				if !ok || snapshot.SupportBasis != spot.ResolverSupportBasisWeighted || snapshot.EvidenceLoss || snapshot.TotalReporters != 3 || snapshot.TotalWeightedSupportMilli != weight {
					t.Fatalf("actual offline population changed: %+v", snapshot)
				}
				if percent, available := correctionflow.ResolverCallConfidencePercent(snapshot, "K1ABC"); percent != 0 || available == allZero {
					t.Fatalf("offline support share=%d/%t all_zero=%t", percent, available, allZero)
				}
				st := &runState{resolver: r, cfg: &config.Config{CallCorrection: config.CallCorrectionConfig{Enabled: true, MinConsensusReports: 2, RecencySeconds: 60, MaxEditDistance: 1, FrequencyToleranceHz: 500, DistanceModelCW: "plain", DistanceModelRTTY: "plain"}}, counters: primaryCounters{RejectedReasonCounts: make(map[string]int64)}}
				s := spot.NewSpotNormalized("K1ABC", "N0AAA", 14020, mode)
				out := evaluateResolverPrimaryDecision(st, s, spot.ResolverEvidence{Key: key}, true, "K1ABC", at)
				if out.Applied || out.Reason != reason || s.DXCallNorm != "K1ABC" || st.counters.DecisionTotal != 1 || st.counters.Rejected != 1 || st.counters.RejectedReasonCounts[reason] != 1 {
					t.Fatalf("offline retained outcome disagrees with weighted population: %+v counters=%+v", out, st.counters)
				}
			})
		}
	}
}

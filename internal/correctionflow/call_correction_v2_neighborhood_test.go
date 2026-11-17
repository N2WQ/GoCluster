package correctionflow

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestCCV2NeighborhoodAdmission(t *testing.T) {
	for _, mode := range []string{"CW", "RTTY"} {
		for _, subject := range []string{"K1AXZ", "K1XYZ"} {
			t.Run(mode+"/"+subject, func(t *testing.T) {
				at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
				r := spot.NewSignalResolver(spot.SignalResolverConfig{HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
				d, err := spot.NewSignalResolverDriver(r)
				if err != nil {
					t.Fatal(err)
				}
				key := spot.NewResolverSignalKey(14020, "20m", mode, 500)
				for n := 0; n < 7; n++ {
					call := "K1ABC"
					if n == 6 {
						call = subject
					}
					if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: call, Spotter: fmt.Sprintf("N%dAAA", n), FrequencyKHz: 14020, RecencyWindow: time.Minute}) {
						t.Fatal("exact input refused")
					}
				}
				for n := 0; n < 3; n++ {
					if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: "DL9ZZZ", Spotter: fmt.Sprintf("N%dBBB", n), FrequencyKHz: 14020.5, RecencyWindow: time.Minute}) {
						t.Fatal("adjacent input refused")
					}
				}
				if d.Drain() != 10 {
					t.Fatal("missing owner evidence")
				}
				d.Sweep(at.Add(time.Millisecond))
				exact, ok := r.Lookup(key)
				if !ok || exact.Winner != "K1ABC" || exact.WinnerSupport != 6 {
					t.Fatalf("bad exact control: %+v", exact)
				}
				cfg := config.CallCorrectionConfig{ResolverNeighborhoodEnabled: true, ResolverNeighborhoodBucketRadius: 1,
					ResolverNeighborhoodMaxDistance: 1, MaxEditDistance: 3, DistanceModelCW: "plain", DistanceModelRTTY: "plain"}
				for _, release := range []bool{false, true} {
					var selected ResolverPrimarySelection
					if release {
						selected = SelectResolverPrimarySnapshotForCallAt(r, key, cfg, subject, at.Add(time.Second))
					} else {
						selected = SelectResolverPrimarySnapshotForCall(r, key, cfg, subject)
					}
					if selected.UsedNeighborhood || !selected.SnapshotOK || selected.Snapshot.ReceiverEvidence() != exact.ReceiverEvidence() || selected.Snapshot.Winner != exact.Winner || len(selected.Snapshot.CandidateRanks) != 2 {
						t.Fatalf("excluded neighbor narrowed exact authority (release=%t): %+v", release, selected)
					}
					if selected.NeighborhoodExcludedDistance+selected.NeighborhoodExcludedUnrelated != 1 {
						t.Fatal("excluded population not accounted")
					}
				}
			})
		}
	}
}

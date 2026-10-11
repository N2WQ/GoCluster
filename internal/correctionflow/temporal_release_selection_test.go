package correctionflow

import (
	"dxcluster/config"
	"dxcluster/spot"
	"testing"
	"time"
)

func TestTemporalReleasePreservesExactComparabilityRail(t *testing.T) {
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 16, HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
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
		{"K1ABD", "N0AAA", 7010}, {"K1ABD", "N0BBB", 7010},
		{"K1ABC", "N0CCC", 7010.5}, {"K1ABC", "N0DDD", 7010.5}, {"K1ABC", "N0EEE", 7010.5}, {"K1ABC", "N0FFF", 7010.5},
		{"K1AEC", "N0GGG", 7010.5},
	} {
		if !r.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: at, DXCall: row.call, Spotter: row.receiver, FrequencyKHz: row.frequency, RecencyWindow: time.Minute}) {
			t.Fatal("enqueue")
		}
	}
	d.Drain()
	d.Sweep(at.Add(time.Millisecond))
	cfg := config.CallCorrectionConfig{ResolverNeighborhoodEnabled: true, ResolverNeighborhoodBucketRadius: 1, ResolverNeighborhoodMaxDistance: 1, DistanceModelCW: "plain", FreqGuardRunnerUpRatio: .6}
	ordinary := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABC", at.Add(time.Second))
	if ordinary.Snapshot.Winner != "K1ABC" || ordinary.exactWinner != "K1ABD" || !ordinary.allowsReplacement("K1AEC") {
		t.Fatalf("nontransitive rail fixture: %+v", ordinary)
	}
	selected := SelectTemporalReleaseSnapshot(r, key, cfg, "K1ABC", TemporalDecision{Action: TemporalDecisionActionApply, Winner: "K1AEC"}, at.Add(time.Second))
	if selected.Snapshot.Winner != "K1ABC" || ResolverSupportForCall(selected.Snapshot, "K1AEC") != 1 {
		t.Fatalf("proposal bypassed exact rail or removed dissent: %+v", selected)
	}
}

func TestTemporalReleaseUsesFreshNeighborhoodRows(t *testing.T) {
	r, key, _ := accountingResolver(t, 2, "K1ABD", false, nil)
	cfg := accountingConfig()
	at := time.Date(2026, 10, 10, 12, 1, 0, 1, time.UTC)
	selected := SelectTemporalReleaseSnapshot(r, key, cfg, "K1ABD", TemporalDecision{Action: TemporalDecisionActionApply, Winner: "K1ABC"}, at)
	if selected.Snapshot.Winner != "" || selected.Snapshot.TotalReporters != 0 || selected.Snapshot.State != spot.ResolverStateUncertain {
		t.Fatalf("expired neighborhood authorized correction: %+v", selected)
	}
}

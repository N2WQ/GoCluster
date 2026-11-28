package main

import (
	"dxcluster/config"
	"dxcluster/spot"
	"fmt"
	"testing"
	"time"
)

func checkCapacity(t *testing.T, r *spot.SignalResolver, d *spot.SignalResolverDriver, want int) {
	t.Helper()
	at := time.Date(2026, 11, 28, 3, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 400)
	for i := range 129 {
		ev := spot.ResolverEvidence{Key: key, DXCall: "K1ABC", Spotter: fmt.Sprintf("N0R%d", i),
			ObservedAt: at, FrequencyKHz: 14020, RecencyWindow: 5 * time.Minute}
		if !r.Enqueue(ev) || d.Drain() != 1 {
			t.Fatal("resolver refused fixture input")
		}
	}
	d.Sweep(at.Add(time.Second))
	snap, ok := r.Lookup(key)
	if !ok || snap.TotalReporters != want || snap.EvidenceLoss != (want < 129) {
		t.Fatalf("CW resolver reported %d receivers and loss=%t, expected %d", snap.TotalReporters, snap.EvidenceLoss, want)
	}
}

func TestRevengUsesLiveCWReporterLimits(t *testing.T) {
	r, d, _, err := newRevengHistoryOwners(config.CallCorrectionConfig{}, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkCapacity(t, r, d, 129)
}

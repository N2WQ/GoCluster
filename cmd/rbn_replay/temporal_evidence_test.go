package main

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestReplayTemporalPendingCompaction(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond})
	driver, err := spot.NewSignalResolverDriver(resolver)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for _, receiver := range []string{"N0AAA", "N0BBB"} {
		if !resolver.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: "K1ABC", Spotter: receiver, FrequencyKHz: 7010}) {
			t.Fatal("enqueue")
		}
	}
	driver.Drain()
	driver.Sweep(at.Add(time.Millisecond))
	cfg := &config.Config{CallCorrection: config.CallCorrectionConfig{TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
		Enabled: true, Scope: "all_correction_candidates", LagSeconds: 1, MaxWaitSeconds: 2,
		MaxPending: 8, MaxActiveKeys: 2, MaxEventsPerKey: 4, BeamSize: 4, MaxObsCandidates: 4,
	}}}
	r := &replayRunner{cfg: cfg, resolver: resolver, driver: driver, temporalNextID: 1,
		temporalDecoder: correctionflow.NewTemporalDecoder(cfg.CallCorrection), temporalPending: make(map[uint64]replayTemporalPending)}
	if !r.maybeHoldTemporal(spot.NewSpot("K1ABC", "N0AAA", 7010, "CW"), spot.ResolverEvidence{Key: key}, true, at) {
		t.Fatal("temporal fixture was not held")
	}
	for _, pending := range r.temporalPending {
		if pending.selection.Snapshot.ReceiverEvidence() != nil || pending.selection.Snapshot.TotalReporters != 2 {
			t.Fatal("replay pending retained receiver generation or lost tally")
		}
	}
}

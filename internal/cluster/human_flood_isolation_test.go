package cluster

import (
	"testing"
	"time"

	"dxcluster/commands"
	"dxcluster/config"
	"dxcluster/reputation"
	"dxcluster/spot"
)

func TestExternalFloodDoesNotConsumeLocalCommandAllowance(t *testing.T) {
	cfg := activityWiringConfig(true, config.FloodActionSuppress)
	thresholds := cfg.FloodControl.Rails.DECall.ThresholdsPerSourceType
	thresholds["MANUAL"], thresholds["UPSTREAM"] = 1, 1
	r := activityWiringRuntimeConfig(t, cfg)
	for index, source := range []spot.SourceType{spot.SourcePeer, spot.SourceUpstream} {
		for repeat := range 2 {
			dx := []string{"DL1ABC", "DL1ABD"}[index]
			s := activityWiringSpot(dx, "DL1AAA", "CW")
			s.SourceType = source
			activityWiringSend(t, r, s)
			if repeat == 0 {
				activityWiringReceive(t, r)
			}
		}
	}
	gate, err := reputation.NewGate(config.ReputationConfig{Enabled: true,
		ReputationDir: t.TempDir(), RampWindowSeconds: 3600,
		PerBandStart: 1, PerBandCap: 1, TotalCapStart: 2, TotalCapPostRamp: 2,
		IPv4BucketSize: 10000, IPv4BucketRefillPerSec: 10000}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gate.Close)
	gate.RecordLogin("DL1AAA", "203.0.113.1", time.Now().UTC())
	processor := commands.NewProcessor(nil, nil, r.ingestInput, nil, gate, nil)
	response := processor.ProcessCommandForClient("DX 14030 DL1MAN CW TEST", "DL1AAA", "203.0.113.1", nil, "go")
	if response != "Spot queued.\n" {
		t.Fatalf("first local command was rejected: %q", response)
	}
	if local := activityWiringReceive(t, r); local.SourceType != spot.SourceManual || local.DECallNorm != "DL1AAA" {
		t.Fatalf("wrong local report: source=%s reporter=%s", local.SourceType, local.DECallNorm)
	}
	processed, _, suppressed, _, _, _ := r.floodController.GetStats()
	if processed != 5 || suppressed != 2 {
		t.Fatalf("external/local accounting: processed=%d suppressed=%d", processed, suppressed)
	}
	// The independent reputation cap is still enforced, before flood admission.
	if response := processor.ProcessCommandForClient("DX 14030 DL1MAN CW TEST", "DL1AAA", "203.0.113.1", nil, "go"); response != "Spot rejected.\n" {
		t.Fatalf("reputation cap lost: %q", response)
	}
	if p, _, _, _, _, _ := r.floodController.GetStats(); p != processed {
		t.Fatal("reputation-rejected command reached flood accounting")
	}
}

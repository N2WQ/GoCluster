package main

import (
	"testing"
	"time"

	"dxcluster/pskreporter"
	"dxcluster/spot"
)

func TestSourceStatsLabel(t *testing.T) {
	cases := []struct {
		name string
		spot *spot.Spot
		want string
	}{
		{"manual", &spot.Spot{SourceType: spot.SourceManual}, "HUMAN"},
		{"rbn-digital", &spot.Spot{SourceType: spot.SourceRBN, SourceNode: "RBN-DIGITAL"}, "RBN-DIGITAL"},
		{"rbn", &spot.Spot{SourceType: spot.SourceRBN, SourceNode: "RBN"}, "RBN"},
		{"ft8", &spot.Spot{SourceType: spot.SourceFT8}, "RBN-FT"},
		{"psk", &spot.Spot{SourceType: spot.SourcePSKReporter}, "PSKREPORTER"},
		{"peer", &spot.Spot{SourceType: spot.SourcePeer}, "PEER"},
		{"upstream", &spot.Spot{SourceType: spot.SourceUpstream}, "UPSTREAM"},
		{"node-fallback", &spot.Spot{SourceNode: "PSKREPORTER"}, "PSKREPORTER"},
		{"other", &spot.Spot{}, "OTHER"},
	}
	for _, tc := range cases {
		if got := sourceStatsLabel(tc.spot); got != tc.want {
			t.Fatalf("%s: expected %s, got %s", tc.name, tc.want, got)
		}
	}
}

func TestRBNIngestDeltasUsesRBNFT(t *testing.T) {
	sourceTotals := map[string]uint64{
		"RBN":    10,
		"RBN-FT": 7,
	}
	prevSourceTotals := map[string]uint64{
		"RBN":    3,
		"RBN-FT": 2,
	}
	sourceModeTotals := map[string]uint64{
		"RBN|CW":         5,
		"RBN|RTTY":       2,
		"RBN-FT|FT8":     6,
		"RBN-FT|FT4":     1,
		"RBN-DIGITAL|CW": 9,
	}
	prevSourceModeTotals := map[string]uint64{
		"RBN|CW":     1,
		"RBN|RTTY":   0,
		"RBN-FT|FT8": 4,
		"RBN-FT|FT4": 1,
	}

	rbnTotal, rbnCW, rbnRTTY, rbnFTTotal, rbnFT8, rbnFT4 :=
		rbnIngestDeltas(sourceTotals, prevSourceTotals, sourceModeTotals, prevSourceModeTotals)

	if rbnTotal != 7 || rbnCW != 4 || rbnRTTY != 2 {
		t.Fatalf("unexpected RBN CW/RTTY deltas: total=%d cw=%d rtty=%d", rbnTotal, rbnCW, rbnRTTY)
	}
	if rbnFTTotal != 5 || rbnFT8 != 2 || rbnFT4 != 0 {
		t.Fatalf("unexpected RBN-FT deltas: total=%d ft8=%d ft4=%d", rbnFTTotal, rbnFT8, rbnFT4)
	}
}

func TestWithIngestStatusLabel(t *testing.T) {
	if got := withIngestStatusLabel("RBN", true); got != "[green]RBN[-]" {
		t.Fatalf("expected live label, got %q", got)
	}
	if got := withIngestStatusLabel("RBN", false); got != "[red]RBN[-]" {
		t.Fatalf("expected offline label, got %q", got)
	}
}

func TestPSKReporterLive(t *testing.T) {
	now := time.Date(2026, 2, 5, 9, 30, 0, 0, time.UTC)
	if got := pskReporterLive(pskreporter.HealthSnapshot{}, now); got {
		t.Fatal("expected disconnected snapshot to be false")
	}
	snap := pskreporter.HealthSnapshot{
		Connected:     true,
		LastPayloadAt: now.Add(-ingestIdleThreshold + time.Second),
	}
	if got := pskReporterLive(snap, now); !got {
		t.Fatal("expected recent payload to be live")
	}
	snap.LastPayloadAt = now.Add(-ingestIdleThreshold - time.Second)
	if got := pskReporterLive(snap, now); got {
		t.Fatal("expected stale payload to be false")
	}
}

//go:build !qualification

// File role: Connects rejection of a selected nonfinite receiver weight to
// actual local/peer TCP output, sealed ring rows, and reopened archive contents.
package cluster

import (
	"math"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestCorrectionNumericDeliveredInvalidSelectedReceiver(t *testing.T) {
	for _, modeOverride := range []bool{false, true} {
		name := "nan/global"
		if modeOverride {
			name = "nan/selected_cw_override"
		}
		t.Run(name, func(t *testing.T) {
			correctionNumericCheckInvalidDelivery(t, math.NaN(), modeOverride, false)
		})
	}
	// Absence is distinct from an invalid selected override: a genuinely absent
	// CW entry still uses the existing finite global weight and may corroborate B.
	t.Run("absent_cw_override_uses_valid_global", func(t *testing.T) {
		correctionNumericCheckInvalidDelivery(t, 1, true, true)
	})
}

func correctionNumericCheckInvalidDelivery(t *testing.T, weight float64, modeOverride, valid bool) {
	t.Helper()
	temporalDeliveryDirectory(t)
	at := time.Now().UTC().Truncate(time.Second)
	global := spot.SpotterReliability{"N0AAA": 1, "N0BBB": 1, "N0CCC": weight}
	var cw spot.SpotterReliability
	if modeOverride {
		global["N0CCC"] = 1
		cw = spot.SpotterReliability{}
		if !valid {
			cw["N0CCC"] = weight
		}
	}
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 16, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 8,
		InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Millisecond, SweepInterval: time.Millisecond,
		HysteresisWindows: 1, MaxEditDistance: 3, DistanceModelCW: "morse", DistanceModelRTTY: "baudot",
		SpotterReliability: global, SpotterReliabilityCW: cw,
	})
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	for _, row := range []struct{ call, receiver string }{
		{"K1ABC", "N0AAA"}, {"K1ABD", "N0BBB"}, {"K1ABD", "N0CCC"},
	} {
		if !r.Enqueue(spot.ResolverEvidence{
			ObservedAt: at, Key: key, DXCall: row.call, Spotter: row.receiver, FrequencyKHz: 14020, RecencyWindow: 2 * time.Minute,
		}) {
			t.Fatal("bounded queue refused normalized fixture evidence")
		}
	}
	if driver.Drain() != 3 {
		t.Fatal("positive processing barrier did not consume all three submitted rows")
	}
	driver.Sweep(at.Add(2 * time.Millisecond))
	snapshot, ok := r.Lookup(key)
	wantCall, wantGlyph, wantPercent, wantTotal, wantWinnerWeight := "K1ABC", "P", 50, 2, 1000
	wantCorrections, wantDropped := uint64(0), uint64(1)
	if valid {
		wantCall, wantGlyph, wantPercent, wantTotal, wantWinnerWeight = "K1ABD", "C", 66, 3, 2000
		wantCorrections, wantDropped = 1, 0
	}
	if !ok || snapshot.Winner != wantCall || snapshot.TotalReporters != wantTotal || snapshot.TotalWeightedSupportMilli != wantTotal*1000 || snapshot.WinnerWeightedSupportMilli != wantWinnerWeight {
		t.Errorf("published population must contain only valid selected receiver weights: available=%t snapshot=%+v", ok, snapshot)
	}
	if got := r.MetricsSnapshot().DropReliability; got != wantDropped {
		t.Errorf("processed selected invalid weight must be refused before fallback: drops=%d want %d", got, wantDropped)
	}
	if snapshot.ReceiverEvidence() == nil {
		t.Error("positive publication barrier did not publish receiver rows")
	} else {
		snapshot.ReceiverEvidence().Range(func(_ string, observation spot.ResolverReceiverObservation) bool {
			if !valid && observation.Receiver == "N0CCC" {
				t.Error("invalid selected receiver survived into immutable evidence")
			}
			return true
		})
	}
	cfg := config.CallCorrectionConfig{
		Enabled: true, RecencySeconds: 120, MaxEditDistance: 3, MinConsensusReports: 2,
		MinAdvantage: 1, MinConfidencePercent: 0, FrequencyToleranceHz: 500,
		DistanceModelCW: "morse", DistanceModelRTTY: "baudot", InvalidAction: "broadcast",
	}
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	f.pipeline.spotterReliability, f.pipeline.spotterReliabilityCW = global, cw
	s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
	s.SourceType = spot.SourceManual
	f.pipeline.processSpot(s, nil)
	f.check(t, correctionEvidenceDeliveryCase{
		subject: "K1ABC", wantCall: wantCall, wantGlyph: wantGlyph, wantPercent: wantPercent, wantCorrections: wantCorrections,
	}, at)
}

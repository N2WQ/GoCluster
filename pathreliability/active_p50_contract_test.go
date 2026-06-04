package pathreliability

import (
	"testing"
	"time"
)

func TestPredictActiveP50ReceiverModeContract(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)

	cases := []struct {
		name        string
		mode        string
		wantP50     float64
		wantCount   uint32
		wantRaw     uint32
		wantCapped  uint32
		wantLimited bool
	}{
		{
			name:       "off uses raw p50",
			mode:       ReceiverContributionOff,
			wantP50:    24,
			wantCount:  5,
			wantRaw:    5,
			wantCapped: 5,
		},
		{
			name:        "shadow keeps raw p50 active",
			mode:        ReceiverContributionShadow,
			wantP50:     24,
			wantCount:   5,
			wantRaw:     5,
			wantCapped:  3,
			wantLimited: true,
		},
		{
			name:        "enforce switches active p50 to capped",
			mode:        ReceiverContributionEnforce,
			wantP50:     -24,
			wantCount:   3,
			wantRaw:     5,
			wantCapped:  3,
			wantLimited: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ReceiverContributionMode = tc.mode
			cfg.ReceiverMaxEffectiveCount = 1
			cfg.ReceiverMaxEffectiveWeight = 1
			cfg.MinEffectiveWeight = 0.01
			cfg.MinObservationCount = 1
			cfg.StaleAfterHalfLifeMultiplier = 100
			cfg.MaxPredictionAgeHalfLifeMultiplier = 100
			predictor := NewPredictor(cfg, []string{"20m"})

			strongReceiver := ReceiverIdentityHash("STRONG")
			weakOne := ReceiverIdentityHash("WEAK1")
			weakTwo := ReceiverIdentityHash("WEAK2")
			for i := 0; i < 3; i++ {
				predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", 30, 1, now, false, strongReceiver)
			}
			predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -30, 1, now, false, weakOne)
			predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -30, 1, now, false, weakTwo)

			res := predictor.PredictWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, 1, now)
			if res.Source != SourceCombined {
				t.Fatalf("expected usable combined prediction, got source=%v reason=%v", res.Source, res.InsufficientReason)
			}
			if !res.HasP50 || res.P50DB != tc.wantP50 {
				t.Fatalf("p50=%v has=%v, want %v", res.P50DB, res.HasP50, tc.wantP50)
			}
			if res.Count != tc.wantCount || res.RawCount != tc.wantRaw || res.CappedCount != tc.wantCapped {
				t.Fatalf("counts=%d raw=%d capped=%d, want %d/%d/%d", res.Count, res.RawCount, res.CappedCount, tc.wantCount, tc.wantRaw, tc.wantCapped)
			}
			if res.CapLimited != tc.wantLimited {
				t.Fatalf("capLimited=%v, want %v", res.CapLimited, tc.wantLimited)
			}
			if res.P50Glyph != res.Glyph {
				t.Fatalf("active p50 glyph=%q should match glyph=%q", res.P50Glyph, res.Glyph)
			}
		})
	}
}

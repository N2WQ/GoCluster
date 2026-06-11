package pathreliability

import (
	"math"
	"testing"
	"time"
)

func TestBeaconReceiveOnlyUsesBeaconFloorAndReceiverGate(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionEnforce
	cfg.ReceiverMaxEffectiveCount = 8
	cfg.ReceiverMaxEffectiveWeight = 10
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 21
	cfg.BeaconMinObservationCount = 11
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Unix(1_700_000_000, 0).UTC()
	receivers := []uint64{
		ReceiverIdentityHash("N2WQ"),
		ReceiverIdentityHash("K1ABC"),
	}

	for i := 0; i < 11; i++ {
		receiver := receivers[0]
		if i >= 8 {
			receiver = receivers[1]
		}
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -10, 1.0, now, true, receiver)
	}

	normal := predictor.PredictWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, cfg.MinObservationCount, now)
	if normal.Source != SourceInsufficient || normal.InsufficientReason != InsufficientLowCount {
		t.Fatalf("expected normal one-way path to fail normal count floor, got %+v", normal)
	}

	beacon := predictor.PredictBeaconReceiveOnlyWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, cfg.BeaconMinObservationCount, now)
	if beacon.Source != SourceCombined || !beacon.BeaconRX {
		t.Fatalf("expected beacon receive-only prediction, got %+v", beacon)
	}
	if beacon.ObservationCount != 11 || beacon.ReceiverCount != 2 || beacon.ReceiverRequired != 2 {
		t.Fatalf("unexpected beacon gate diagnostics: obs=%d receivers=%d/%d result=%+v", beacon.ObservationCount, beacon.ReceiverCount, beacon.ReceiverRequired, beacon)
	}
}

func TestBeaconReceiveOnlyDoesNotApplyReverseHintDiscount(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionOff
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 21
	cfg.BeaconMinObservationCount = 11
	cfg.ReverseHintDiscount = 0.5
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Unix(1_700_000_000, 0).UTC()

	for i := 0; i < 11; i++ {
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -10, 1.0, now, true, ReceiverIdentityHash("RX"))
	}

	beacon := predictor.PredictBeaconReceiveOnlyWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, 11, now)
	if beacon.Source != SourceCombined {
		t.Fatalf("expected beacon prediction to pass, got %+v", beacon)
	}
	if math.Abs(beacon.Weight-11) > 1e-9 {
		t.Fatalf("beacon receive-only weight = %v, want undiscounted 11", beacon.Weight)
	}
}

func TestBeaconReceiveOnlyIgnoresTransmitEvidence(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionOff
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 21
	cfg.BeaconMinObservationCount = 11
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Unix(1_700_000_000, 0).UTC()

	for i := 0; i < 11; i++ {
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -20, 1.0, now, true, ReceiverIdentityHash("RX"))
		predictor.UpdateWithReceiverHash(BucketCombined, dxCell, userCell, dxCoarse, userCoarse, "20m", 20, 1.0, now, false, ReceiverIdentityHash("TX"))
	}

	beacon := predictor.PredictBeaconReceiveOnlyWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, 11, now)
	if beacon.Source != SourceCombined || !beacon.BeaconRX {
		t.Fatalf("expected beacon receive-only prediction, got %+v", beacon)
	}
	if beacon.P50DB > -19 {
		t.Fatalf("beacon p50 used transmit evidence: p50=%v result=%+v", beacon.P50DB, beacon)
	}
}

func TestBeaconReceiveOnlyAppliesNoisePenalty(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionOff
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 21
	cfg.BeaconMinObservationCount = 11
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Unix(1_700_000_000, 0).UTC()

	for i := 0; i < 11; i++ {
		predictor.UpdateWithReceiverHash(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -10, 1.0, now, true, ReceiverIdentityHash("RX"))
	}

	beacon := predictor.PredictBeaconReceiveOnlyWithMinObservationCount(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 5, 11, now)
	if beacon.Source != SourceCombined {
		t.Fatalf("expected beacon prediction to pass, got %+v", beacon)
	}
	if beacon.P50DB != -14.5 {
		t.Fatalf("expected receive noise penalty to shift p50 to -14.5, got %v", beacon.P50DB)
	}
}

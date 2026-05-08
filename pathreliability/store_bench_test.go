package pathreliability

import (
	"testing"
	"time"
)

func BenchmarkStoreUpdate(b *testing.B) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionOff
	store := NewStore(cfg, []string{"20m"})
	now := time.Now().UTC()
	power := dbToPower(-5)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.UpdateWithReceiverHashDB(1, 2, 3, 4, "20m", -5, power, 1.0, now, 0)
	}
}

func BenchmarkStoreUpdateReceiverCapShadow(b *testing.B) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionShadow
	store := NewStore(cfg, []string{"20m"})
	now := time.Now().UTC()
	power := dbToPower(-5)
	receivers := []uint64{
		ReceiverIdentityHash("N2WQ"),
		ReceiverIdentityHash("K1ABC"),
		ReceiverIdentityHash("W1AW"),
		ReceiverIdentityHash("VE3XYZ"),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.UpdateWithReceiverHashDB(1, 2, 3, 4, "20m", -5, power, 1.0, now, receivers[i&3])
	}
}

func BenchmarkStoreUpdateP50ElapsedSecondDecay(b *testing.B) {
	cfg := DefaultConfig()
	cfg.ReceiverContributionMode = ReceiverContributionShadow
	store := NewStore(cfg, []string{"20m"})
	now := time.Now().UTC()
	power := dbToPower(-5)
	receivers := []uint64{
		ReceiverIdentityHash("N2WQ"),
		ReceiverIdentityHash("K1ABC"),
		ReceiverIdentityHash("W1AW"),
		ReceiverIdentityHash("VE3XYZ"),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := now.Add(time.Duration(i) * time.Second)
		store.UpdateWithReceiverHashDB(1, 2, 3, 4, "20m", -5, power, 1.0, t, receivers[i&3])
	}
}

func BenchmarkStoreRefreshStatsSnapshot(b *testing.B) {
	cfg := DefaultConfig()
	cfg.StaleAfterSeconds = 3600
	store := NewStore(cfg, []string{"20m"})
	now := time.Now().UTC()
	power := dbToPower(-5)

	for i := 0; i < 20000; i++ {
		receiver := CellID((i % 30000) + 1)
		sender := CellID(((i * 7) % 30000) + 1)
		store.Update(receiver, sender, InvalidCell, InvalidCell, "20m", power, 1.0, now)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.RefreshStatsSnapshot(now)
	}
}

func BenchmarkPredictFreshnessGate(b *testing.B) {
	cfg := DefaultConfig()
	cfg.BandHalfLifeSec = map[string]int{"20m": 300}
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1.25
	predictor := NewPredictor(cfg, []string{"20m"})
	userCell := CellID(1)
	dxCell := CellID(2)
	userCoarse := CellID(3)
	dxCoarse := CellID(4)
	now := time.Now().UTC()

	predictor.Update(BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -5, 10, now, false)
	predictor.Update(BucketCombined, dxCell, userCell, dxCoarse, userCoarse, "20m", -7, 8, now, false)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = predictor.Predict(userCell, dxCell, userCoarse, dxCoarse, "20m", "FT8", 0, now)
	}
}

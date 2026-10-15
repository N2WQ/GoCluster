package pathreliability

import (
	"math"
	"testing"
)

// Masses and medians are literal occupied-bin oracles from the preexecution
// model record. No expected result calls the production selector or histogram.
func TestS6LiteralPopulationHistogramOracles(t *testing.T) {
	for _, tc := range []struct {
		name                                          string
		receive, transmit, noise, p50, rxMass, txMass float64
		rxIndex                                       int
	}{
		{"directional", .6, .4, 0, -10.5, 3, 2, 14},
		{"receive_noise", .6, .4, 6, -16.5, 3, 2, 8},
		{"exact_occupied_tie", .5, .5, 0, -3, 2.5, 2.5, 14},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.MergeReceiveWeight, cfg.MergeTransmitWeight = tc.receive, tc.transmit
			rx := sampleWithBins{Sample: Sample{Weight: 5, Count: 5, RawCount: 5, ObservationCount: 5}}
			tx := rx
			rx.snrBins[14], tx.snrBins[29] = 5, 5
			beforeRX, beforeTX := rx, tx
			got, ok := mergeSamplesWithDistribution(rx, tx, cfg, tc.noise)
			if !ok || !got.HasP50 || got.P50DB != tc.p50 || got.ObservationCount != 10 || got.Weight != 5 {
				t.Fatalf("literal median/units: %+v", got)
			}
			for i, mass := range got.snrBins {
				want := float32(0)
				if i == tc.rxIndex {
					want = float32(tc.rxMass)
				}
				if i == 29 {
					want = float32(tc.txMass)
				}
				if math.Float32bits(mass) != math.Float32bits(want) {
					t.Fatalf("bin %d bits %08x want %08x", i, math.Float32bits(mass), math.Float32bits(want))
				}
			}
			if rx != beforeRX || tx != beforeTX {
				t.Fatal("query changed source distributions")
			}
		})
	}
}

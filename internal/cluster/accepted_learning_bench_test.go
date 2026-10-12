//go:build !qualification

// File role: Measures bounded accepted recording after correctness checks;
// repeated receiver writes are independently counted by WhoSpotsMe occurrences.
// A fixed source timestamp measures duplicate history recording, including the
// CustomSCP duplicate path; this does not measure fresh persistent writes.
package cluster

import (
	"path/filepath"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func BenchmarkAcceptedLearningPathsRecording(b *testing.B) {
	for _, backend := range []string{"legacy_CW", "legacy_FT8", "custom_V"} {
		b.Run(backend, func(b *testing.B) {
			p := &outputPipeline{correctionCfg: config.CallCorrectionConfig{Enabled: true, RecentBandBonusEnabled: true,
				StabilizerEnabled: true, RecentBandWindowSeconds: 3600, RecentBandRecordMinUniqueSpotters: 2},
				whoSpotsMeStore: spot.NewWhoSpotsMeStoreWithOptions(spot.WhoSpotsMeOptions{Window: time.Hour,
					Shards: 1, MaxEntries: 4, MaxCountriesPerEntry: 2}),
			}
			p.recentBandStore = spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{Window: time.Hour,
				Shards: 1, MaxEntries: 4, MaxSpottersPerCall: 4})
			mode := "CW"
			if backend == "legacy_FT8" {
				mode = "FT8"
				p.ftRecentBandStore = newFTRecentBandStore(p.correctionCfg)
			}
			if backend == "custom_V" {
				store, err := spot.OpenCustomSCPStore(spot.CustomSCPOptions{Path: filepath.Join(b.TempDir(), "scp"),
					MaxKeys: 4, MaxSpottersPerKey: 4, HorizonDays: 60, StaticHorizonDays: 395,
					CoreMinScore: 1, CoreMinH3Cells: 1, MinSNRDBCW: 4})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := store.Close(); err != nil {
						b.Error(err)
					}
				})
				p.recentBandStore, p.customSCPStore, p.correctionCfg.CustomSCP.Enabled = store, store, true
			}
			s := spot.NewSpot("K1BENCH", "N0AAA", 14030, mode)
			s.Time, s.Confidence = time.Now().UTC().Add(-time.Second), "V"
			s.DEMetadata.ADIF, s.DEMetadata.Continent = 291, "NA"
			s.Report, s.HasReport = 10, true
			s.EnsureNormalized()
			p.recordAcceptedObservation(s)
			control := p.whoSpotsMeStore.CountryCountsByContinent("K1BENCH", "20m", time.Now().UTC())
			if len(control["NA"]) != 1 || control["NA"][0].Count != 1 {
				b.Fatal("benchmark failed its eligible exactly-one occurrence control")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.recordAcceptedObservation(s)
			}
			b.StopTimer()
			got := p.whoSpotsMeStore.CountryCountsByContinent("K1BENCH", "20m", time.Now().UTC())
			if len(got["NA"]) != 1 || got["NA"][0].Count != b.N+1 {
				b.Fatal("measured accepted recorder lost or duplicated occurrences")
			}
			var count int
			if p.customSCPStore != nil {
				count = p.customSCPStore.RecentSupportCountWithRequirements("K1BENCH", "20m", mode, spot.CustomSCPRequirements{MinScore: 1}, time.Now().UTC())
			} else if mode == "FT8" {
				count = p.ftRecentBandStore.RecentSupportCount("K1BENCH", "20m", mode, time.Now().UTC())
			} else {
				count = p.recentBandStore.RecentSupportCount("K1BENCH", "20m", mode, time.Now().UTC())
			}
			if count != 1 {
				b.Fatalf("bounded repeated receiver evidence=%d want 1", count)
			}
		})
	}
}

// File role: Checks replay construction of the canonical Custom SCP owner,
// independent consumer requirements and the configured static horizon.
package main

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestB12ReplayCustomSCPConsumerRequirementsAndStaticHorizon(t *testing.T) {
	if err := pathreliability.InitH3MappingsFromDir(testsupport.H3TableDir(t)); err != nil {
		t.Fatalf("InitH3Mappings unavailable: %v", err)
	}
	if pathreliability.EncodeCoarseCell("FN31AA") == pathreliability.InvalidCell {
		t.Fatal("fixture must provide real geography for the existing history gates")
	}
	// The nondefault 90-day control catches omitted constructor wiring: the
	// store's fallback is 395 days, which the 395-day case alone cannot detect.
	for _, staticDays := range []int{90, 395} {
		t.Run(fmt.Sprintf("static_horizon_%d", staticDays), func(t *testing.T) {
			cfg := config.CallCorrectionConfig{
				Enabled: true, RecentBandRecordMinUniqueSpotters: 1,
				CustomSCP: config.CallCorrectionCustomSCPConfig{
					Enabled: true, HistoryHorizonDays: 60, StaticHorizonDays: staticDays,
					MaxKeys: 8, MaxSpottersPerKey: 8, CleanupIntervalSeconds: 3600,
					ResolverMinScore: 5, ResolverMinUniqueSpotters: 1, ResolverMinUniqueH3Cells: 1,
					StabilizerMinScore: 7, StabilizerMinUniqueSpotters: 2, StabilizerMinUniqueH3Cells: 2,
					SFloorMinScore: 3, SFloorMinUniqueSpottersExact: 3, SFloorMinUniqueH3CellsExact: 2,
					SFloorMinUniqueSpottersFamily: 5, SFloorMinUniqueH3CellsFamily: 3,
				},
			}
			runner := &replayRunner{cfg: &config.Config{CallCorrection: cfg}, outDir: t.TempDir()}
			t.Cleanup(runner.close)
			if err := runner.openRecentBandStore(); err != nil {
				t.Fatal(err)
			}
			store, canonical := runner.recentBandStore.(*spot.CustomSCPStore)
			if !canonical || runner.cleanupRecentBand == nil {
				t.Fatal("replay must expose and own the canonical Custom SCP store")
			}
			now := time.Now().UTC().Truncate(time.Second)
			source := spot.NewSpot("K1RPL", "N0AAA", 14020, "CW")
			source.Confidence, source.Time, source.DEMetadata.Grid = "V", now.Add(-3*24*time.Hour), "FN31AA"
			source.EnsureNormalized()
			store.RecordSpot(source)
			if !store.StaticContains("K1RPL") || store.StatsSnapshot().ObservationSpotters != 1 {
				t.Fatal("actual replay owner must admit the V observation within its 60-day history horizon")
			}
			resolver := spot.CustomSCPRequirements{MinScore: 5, MinH3Cells: 1}
			stabilizer := spot.CustomSCPRequirements{MinScore: 7, MinH3Cells: 2}
			for pass := 0; pass < 2; pass++ {
				if got := store.RecentSupportCountWithRequirements("K1RPL", "20m", "CW", resolver, now); got != 1 {
					t.Errorf("resolver query inherited stricter stabilizer requirements: %d", got)
				}
				if got := store.RecentSupportCountWithRequirements("K1RPL", "20m", "CW", stabilizer, now); got != 0 {
					t.Errorf("stabilizer query must enforce its own score/cell gates: %d", got)
				}
			}
			if got := store.RecentSupportCount("K1RPL", "20m", "CW", now); got != 1 {
				t.Errorf("replay constructor still installs resolver/stabilizer maxima in default core lookup: %d", got)
			}
			futureQuery := now.Add(100 * 24 * time.Hour)
			wantStatic := 0
			if staticDays == 395 {
				wantStatic = 1
			}
			if got := store.StaticCallCount(futureQuery); got != wantStatic {
				t.Errorf("actual replay static horizon=%d days produced count=%d want %d", staticDays, got, wantStatic)
			}
			if got := store.RecentSupportCountWithRequirements("K1RPL", "20m", "CW", resolver, futureQuery); got != 0 {
				t.Errorf("100-day query must expire observation support under the separate 60-day horizon: %d", got)
			}
			if runner.recentBandStore != store || store.StatsSnapshot().StaticCalls != 1 {
				t.Fatal("queries replaced the canonical owner or reclaimed static membership through observation expiry")
			}
		})
	}
}

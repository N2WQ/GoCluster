// File role: Prevents one history consumer's loaded requirements from changing
// shared minima used by other consumers, including the separate FT support rail.
package config

import (
	"fmt"
	"testing"
)

func b12LoadConsumerConfig(t *testing.T, common, resolver, rail, stabilizerScore, stabilizerUnique, stabilizerCells int, enabled bool) CallCorrectionConfig {
	t.Helper()
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	writeTestConfigOverlay(t, dir, "pipeline.yaml", fmt.Sprintf(`call_correction:
  enabled: true
  stabilizer_enabled: true
  recent_band_record_min_unique_spotters: %d
  resolver_recent_plus1_min_unique_winner: %d
  custom_scp:
    enabled: %t
    resolver_min_score: 5
    resolver_min_unique_spotters: %d
    resolver_min_unique_h3_cells: 2
    stabilizer_min_score: %d
    stabilizer_min_unique_spotters: %d
    stabilizer_min_unique_h3_cells: %d
    s_floor_min_score: 3
    s_floor_min_unique_spotters_exact: 3
    s_floor_min_unique_h3_cells_exact: 2
    s_floor_min_unique_spotters_family: 5
    s_floor_min_unique_h3_cells_family: 3
    max_spotters_per_key: 8
`, common, rail, enabled, resolver, stabilizerScore, stabilizerUnique, stabilizerCells))
	cfg, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	return cfg.CallCorrection
}

func TestB12CustomSCPConsumerConfigIndependence(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		score, unique, cells int
	}{
		{"base", 5, 3, 2}, {"score", 10, 3, 2},
		{"receivers", 5, 7, 2}, {"cells", 5, 3, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := b12LoadConsumerConfig(t, 2, 4, 3, tc.score, tc.unique, tc.cells, true)
			if cfg.RecentBandRecordMinUniqueSpotters != 2 {
				t.Fatalf("stabilizer requirements must not silently raise the common minimum consumed by resolver and FT; got %d want 2", cfg.RecentBandRecordMinUniqueSpotters)
			}
			if cfg.CustomSCP.ResolverMinScore != 5 || cfg.CustomSCP.ResolverMinUniqueSpotters != 4 ||
				cfg.CustomSCP.ResolverMinUniqueH3Cells != 2 || cfg.ResolverRecentPlus1MinUniqueWinner != 4 {
				t.Fatalf("stabilizer-only changes modified resolver requirements: %+v", cfg)
			}
			if cfg.CustomSCP.SFloorMinScore != 3 || cfg.CustomSCP.SFloorMinUniqueSpottersExact != 3 ||
				cfg.CustomSCP.SFloorMinUniqueH3CellsExact != 2 || cfg.CustomSCP.SFloorMinUniqueSpottersFamily != 5 ||
				cfg.CustomSCP.SFloorMinUniqueH3CellsFamily != 3 {
				t.Fatalf("dedicated floor requirements must remain independent: %+v", cfg.CustomSCP)
			}
			if cfg.CustomSCP.StabilizerMinScore != tc.score || cfg.CustomSCP.StabilizerMinUniqueSpotters != tc.unique ||
				cfg.CustomSCP.StabilizerMinUniqueH3Cells != tc.cells {
				t.Fatalf("loaded stabilizer requirements lost their explicit values: %+v", cfg.CustomSCP)
			}
		})
	}
}

func TestB12CustomSCPResolverLocalNormalization(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		common, rail, wantRail int
		enabled                bool
	}{
		{"resolver_floor", 2, 1, 4, true}, {"higher_rail", 2, 6, 6, true},
		{"common_remains_explicit", 6, 1, 4, true}, {"disabled_custom", 2, 1, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := b12LoadConsumerConfig(t, tc.common, 4, tc.rail, 5, 7, 2, tc.enabled)
			if cfg.RecentBandRecordMinUniqueSpotters != tc.common || cfg.ResolverRecentPlus1MinUniqueWinner != tc.wantRail {
				t.Fatalf("loader must preserve common=%d and consumer-local rail=%d independently: common=%d rail=%d",
					tc.common, tc.wantRail, cfg.RecentBandRecordMinUniqueSpotters, cfg.ResolverRecentPlus1MinUniqueWinner)
			}
		})
	}
}

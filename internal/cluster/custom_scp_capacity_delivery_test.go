//go:build !qualification

// File role: Rejects unattainable historical minima at runtime admission and
// proves direct S-floor consumers cannot silently lower them to storage caps.
package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestCustomSCPCapacityDeliveredFloorDoesNotLowerMinimum(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		voteCount, baseCount int
		exact, family        int
		wantGlyph            string
	}{
		{"exact_at_capacity", 3, 0, 3, 4, "S"},
		{"exact_above_capacity", 3, 0, 4, 4, "?"},
		{"family_at_capacity", 0, 3, 4, 3, "S"},
		{"family_above_capacity", 0, 3, 4, 4, "?"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			at := time.Now().UTC().Truncate(time.Second)
			path := filepath.Join(t.TempDir(), "scp")
			raw := customSCPFamilySeed(t, path, at, customSCPFamilyDeliveryCase{voteCount: tc.voteCount, baseCount: tc.baseCount})
			customSCPFreshnessAssertBytes(t, path, raw)
			options := spot.CustomSCPOptions{
				Path: path, HorizonDays: 60, StaticHorizonDays: 395, MaxKeys: 8, MaxSpottersPerKey: 3,
				CoreMinScore: 3, CoreMinH3Cells: 2,
				SFloorMinScore: 3, SFloorExactMinH3Cells: 2, SFloorFamilyMinH3Cells: 2,
			}
			store := customSCPFreshnessOpen(t, options)
			if store.StaticContains("W1AW/KH6") || store.StaticContains("W1AW") || store.StatsSnapshot().ObservationSpotters != 3 {
				t.Fatal("capacity fixture requires exactly three retained observations and no static recognition")
			}
			// Deliberately bypass startup validation: public consumers must also
			// reject impossible requirements instead of manufacturing a floor.
			cfg := config.CallCorrectionConfig{CustomSCP: config.CallCorrectionCustomSCPConfig{
				Enabled: true, MaxSpottersPerKey: 3, SFloorMinScore: 3,
				SFloorMinUniqueSpottersExact: tc.exact, SFloorMinUniqueH3CellsExact: 2,
				SFloorMinUniqueSpottersFamily: tc.family, SFloorMinUniqueH3CellsFamily: 2,
			}}
			f := newAmbiguityDeliveryFixture(t, nil, cfg)
			f.pipeline.recentBandStore, f.pipeline.customSCPStore = store, store
			s := newDeliveryTestSpot("W1AW/KH6", "N0AAA", at)
			s.SourceType = spot.SourceManual
			f.pipeline.processSpot(s, nil)
			peerOriginalAssertTCP(t, f, "W1AW/KH6", tc.wantGlyph)
			peerOriginalAssertFields(t, f.peer, []string{
				"14020", "W1AW/KH6", at.Format("02-Jan-2006"), at.Format("1504Z"), " ", "N0AAA", "N0CALL-1", "192.0.2.1",
			})
			if s.ConfidencePercentOK || s.DXCall != "W1AW/KH6" || f.pipeline.tracker.CallCorrections() != 0 {
				t.Error("capacity qualification must not add a current percentage or mutate the input identity")
			}
			customSCPFreshnessCheckStored(t, f, map[string]string{"W1AW/KH6": tc.wantGlyph})
			if store.StatsSnapshot().ObservationSpotters != 3 || store.StaticContains("W1AW/KH6") || store.StaticContains("W1AW") {
				t.Error("direct output queries changed observation-only retained history")
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			customSCPFreshnessAssertBytes(t, path, raw)
			reopened := customSCPFreshnessOpen(t, options)
			if reopened.StatsSnapshot().ObservationSpotters != 3 || reopened.StaticContains("W1AW/KH6") || reopened.StaticContains("W1AW") {
				t.Error("reopen changed the independently encoded capacity population")
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			customSCPFreshnessAssertBytes(t, path, raw)
		})
	}
}

func customSCPCapacityRuntimeConfig(path string) config.CallCorrectionConfig {
	return config.CallCorrectionConfig{
		Enabled: true, StabilizerEnabled: true, RecencySeconds: 120, FrequencyToleranceHz: 500,
		MinConsensusReports: 3, RecentBandRecordMinUniqueSpotters: 3,
		// FT storage is separate: these requirements can exceed the CustomSCP cap.
		PMinUniqueSpotters: 4, VMinUniqueSpotters: 5,
		CustomSCP: config.CallCorrectionCustomSCPConfig{
			Enabled: true, Path: path, HistoryHorizonDays: 60, StaticHorizonDays: 395,
			MaxKeys: 8, MaxSpottersPerKey: 3, CleanupIntervalSeconds: 3600, BlockCacheMB: 1, MemTableSizeMB: 4,
			ResolverMinScore: 5, ResolverMinUniqueSpotters: 3, ResolverMinUniqueH3Cells: 2,
			StabilizerMinScore: 5, StabilizerMinUniqueSpotters: 3, StabilizerMinUniqueH3Cells: 2,
			SFloorMinScore: 3, SFloorMinUniqueSpottersExact: 3, SFloorMinUniqueH3CellsExact: 2,
			SFloorMinUniqueSpottersFamily: 3, SFloorMinUniqueH3CellsFamily: 2,
		},
	}
}

func TestCustomSCPCapacityRuntimeAdmissionPrecedesOwners(t *testing.T) {
	for _, tc := range []struct {
		name     string
		change   func(*config.CallCorrectionConfig)
		errField string
	}{
		{"exact_floor_above_cap", func(c *config.CallCorrectionConfig) { c.CustomSCP.SFloorMinUniqueSpottersExact = 4 }, "s_floor_min_unique_spotters_exact"},
		{"family_floor_above_cap", func(c *config.CallCorrectionConfig) { c.CustomSCP.SFloorMinUniqueSpottersFamily = 4 }, "s_floor_min_unique_spotters_family"},
		{"equality_and_separate_ft_capacity", func(*config.CallCorrectionConfig) {}, ""},
		{"disabled_stabilizer_requirement", func(c *config.CallCorrectionConfig) {
			c.StabilizerEnabled = false
			c.CustomSCP.StabilizerMinUniqueSpotters, c.CustomSCP.StabilizerMinUniqueH3Cells = 4, 4
		}, ""},
		{"disabled_custom_store", func(c *config.CallCorrectionConfig) {
			c.CustomSCP.Enabled = false
			c.CustomSCP.SFloorMinUniqueSpottersExact, c.CustomSCP.SFloorMinUniqueSpottersFamily = 4, 4
		}, ""},
		{"disabled_correction", func(c *config.CallCorrectionConfig) {
			c.Enabled = false
			c.CustomSCP.SFloorMinUniqueSpottersExact, c.CustomSCP.SFloorMinUniqueSpottersFamily = 4, 4
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "scp")
			correction := customSCPCapacityRuntimeConfig(path)
			tc.change(&correction)
			ctx, cancel := context.WithCancel(t.Context())
			r := newClusterRuntime(BuildInfo{}, &config.Config{CallCorrection: correction}, "", config.LoadDiagnostics{})
			r.ctx, r.cancel = ctx, cancel
			t.Cleanup(func() {
				cancel()
				if r.signalResolver != nil {
					r.signalResolver.Stop()
				}
				if r.recentBandStore != nil {
					r.recentBandStore.StopCleanup()
				}
				if r.customSCPStore != nil {
					if err := r.customSCPStore.Close(); err != nil {
						t.Error(err)
					}
				}
			})
			initialized := r.initializeSignalResolverAndRecentStore()
			if tc.errField != "" {
				if initialized || r.signalResolver != nil || r.recentBandStore != nil || r.customSCPStore != nil {
					t.Errorf("invalid effective requirement must fail before creating runtime owners: initialized=%t resolver=%t recent=%t custom=%t", initialized, r.signalResolver != nil, r.recentBandStore != nil, r.customSCPStore != nil)
				}
				if r.startupErr == nil || !strings.Contains(r.startupErr.Error(), tc.errField) || !strings.Contains(r.startupErr.Error(), "max_spotters_per_key") {
					t.Errorf("capacity rejection must identify the impossible requirement and storage cap: error=%v field=%s", r.startupErr, tc.errField)
				}
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Errorf("invalid startup created storage before validation: path=%s err=%v", path, err)
				}
				return
			}
			if !initialized {
				t.Fatalf("valid boundary or disabled unused requirement blocked runtime initialization: %v", r.startupErr)
			}
			wantCustom := correction.Enabled && correction.CustomSCP.Enabled
			if (r.customSCPStore != nil) != wantCustom || (r.signalResolver != nil) != correction.Enabled {
				t.Errorf("valid startup assigned incorrect owners: custom=%t want %t resolver=%t want %t", r.customSCPStore != nil, wantCustom, r.signalResolver != nil, correction.Enabled)
			}
			if wantCustom && r.recentBandStore != r.customSCPStore {
				t.Error("valid capacity must retain one canonical history owner")
			}
		})
	}
}

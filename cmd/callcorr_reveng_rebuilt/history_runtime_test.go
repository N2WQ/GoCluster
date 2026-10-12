// File role: Checks the tool's actual legacy owner fence and replay-clock
// admission, including zero-source normalization at exact window boundaries.
package main

import (
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestRevengLegacyHistoryOwners(t *testing.T) {
	for _, tc := range []struct {
		name      string
		change    func(*config.CallCorrectionConfig)
		wantError string
	}{
		{"plus1_at_capacity", func(c *config.CallCorrectionConfig) { c.ResolverRecentPlus1MinUniqueWinner = 64 }, ""},
		{"plus1_above_capacity", func(c *config.CallCorrectionConfig) { c.ResolverRecentPlus1MinUniqueWinner = 65 }, "resolver_recent_plus1_min_unique_winner"},
		{"unused_floor_and_stabilizer", func(c *config.CallCorrectionConfig) {
			c.ResolverRecentPlus1Enabled = false
			c.StabilizerEnabled, c.StabilizerEditNeighborEnabled = true, true
			c.RecentBandRecordMinUniqueSpotters, c.StabilizerEditNeighborMinSpotters = 65, 65
		}, ""},
		{"Custom_selected_but_actual_legacy_fold", func(c *config.CallCorrectionConfig) {
			c.CustomSCP.Enabled = true
			c.CustomSCP.MaxSpottersPerKey = 100
			c.CustomSCP.ResolverMinUniqueSpotters = 65
		}, "custom_scp.resolver_min_unique_spotters"},
		{"Custom_score_and_cell_not_legacy_gates", func(c *config.CallCorrectionConfig) {
			c.CustomSCP.Enabled = true
			c.CustomSCP.ResolverMinUniqueSpotters = 3
			c.CustomSCP.ResolverMinScore, c.CustomSCP.ResolverMinUniqueH3Cells = 100, 100
		}, ""},
		{"disabled_owner", func(c *config.CallCorrectionConfig) { c.Enabled = false; c.ResolverRecentPlus1MinUniqueWinner = 65 }, ""},
		{"unused_history", func(c *config.CallCorrectionConfig) {
			c.RecentBandBonusEnabled = false
			c.ResolverRecentPlus1MinUniqueWinner = 65
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := config.CallCorrectionConfig{Enabled: true, RecentBandBonusEnabled: true,
				RecentBandWindowSeconds: 3600, MaxEditDistance: 2,
				ResolverRecentPlus1Enabled: true, ResolverRecentPlus1MinUniqueWinner: 3}
			tc.change(&c)
			r, driver, history, err := newRevengHistoryOwners(c, nil, nil, nil, nil)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) || !strings.Contains(err.Error(), "effective requirement 65") ||
					!strings.Contains(err.Error(), "reverse-engineering legacy recent-band receiver capacity=64") || r != nil || driver != nil || history != nil {
					t.Fatalf("invalid actual legacy gate created owners or lost diagnostic: r=%p driver=%p history=%p err=%v", r, driver, history, err)
				}
				return
			}
			if err != nil || r == nil || driver == nil {
				t.Fatalf("unused or attainable gate blocked existing resolver construction: %v", err)
			}
			if want := c.Enabled && (c.RecentBandBonusEnabled || c.StabilizerEnabled); (history != nil) != want {
				t.Fatalf("legacy backend selection changed: history=%p want=%t", history, want)
			}
		})
	}
}

func TestRevengLegacyHistoryAdmissionClock(t *testing.T) {
	for _, at := range []time.Time{time.Date(2001, 1, 2, 12, 0, 0, 123, time.UTC), time.Date(2100, 1, 2, 12, 0, 0, 123, time.UTC)} {
		t.Run(at.Format("2006"), func(t *testing.T) {
			cfg := config.CallCorrectionConfig{RecentBandBonusEnabled: true}
			store := spot.NewRecentBandStore(time.Hour)
			makeSpot := func(receiver string, observed time.Time) *spot.Spot {
				s := spot.NewSpot("K1ABC", receiver, 14020, "CW")
				s.Time = observed
				return s
			}
			recordRecentBandObservation(makeSpot("N0AAA", at), store, cfg, at)
			recordRecentBandObservation(makeSpot("N0AAA", at.Add(time.Nanosecond)), store, cfg, at)
			recordRecentBandObservation(makeSpot("N0BBB", at.Add(time.Nanosecond)), store, cfg, at)
			if n := store.RecentSupportCount("K1ABC", "20m", "CW", at); n != 1 {
				t.Fatalf("future overwrite/new receiver changed eligible control: %d", n)
			}
			zero := makeSpot("N0CCC", time.Time{})
			recordRecentBandObservation(zero, store, cfg, at)
			if n := store.RecentSupportCount("K1ABC", "20m", "CW", at.Add(time.Hour)); n != 2 {
				t.Fatalf("zero source did not normalize to exact admission clock (inclusive end): %d", n)
			}
			if n := store.RecentSupportCount("K1ABC", "20m", "CW", at.Add(time.Hour+time.Nanosecond)); n != 0 {
				t.Fatalf("zero or rejected future evidence survived expiry: %d", n)
			}
		})
	}
}

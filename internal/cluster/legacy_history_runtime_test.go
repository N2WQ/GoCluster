//go:build !qualification

// File role: Proves legacy/FT capacity refusal before live owner creation and
// clock-aware admission through actual recording and delivered support floors.
package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/spot"
)

func TestLegacyHistoryRuntimeAdmissionPrecedesOwners(t *testing.T) {
	for _, tc := range []struct {
		name       string
		change     func(*config.CallCorrectionConfig)
		key, store string
		capacity   int
	}{
		{"stabilizer_common_above_64", func(c *config.CallCorrectionConfig) { c.RecentBandRecordMinUniqueSpotters = 65 }, "recent_band_record_min_unique_spotters", "legacy recent-band", 64},
		{"edit_above_64", func(c *config.CallCorrectionConfig) {
			c.StabilizerEditNeighborEnabled = true
			c.StabilizerEditNeighborMinSpotters = 65
		}, "stabilizer_edit_neighbor_min_spotters", "legacy recent-band", 64},
		{"resolver_plus1_above_64", func(c *config.CallCorrectionConfig) {
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 65
		}, "resolver_recent_plus1_min_unique_winner", "legacy recent-band", 64},
		{"bayes_report_above_64_with_plus1_disabled", func(c *config.CallCorrectionConfig) {
			c.BayesBonus.Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 65
		}, "resolver_recent_plus1_min_unique_winner", "legacy recent-band", 64},
		{"FT_common_above_16", func(c *config.CallCorrectionConfig) {
			c.RecentBandBonusEnabled = true
			c.RecentBandRecordMinUniqueSpotters = 17
		}, "recent_band_record_min_unique_spotters", "FT recent-band", 16},
		{"FT_global_correction_disabled", func(c *config.CallCorrectionConfig) {
			c.Enabled = false
			c.RecentBandBonusEnabled = true
			c.RecentBandRecordMinUniqueSpotters = 17
		}, "recent_band_record_min_unique_spotters", "FT recent-band", 16},
		{"general_equality_64", func(c *config.CallCorrectionConfig) {
			c.RecentBandRecordMinUniqueSpotters = 64
			c.StabilizerEditNeighborEnabled = true
			c.StabilizerEditNeighborMinSpotters = 64
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 64
		}, "", "", 0},
		{"FT_equality_16", func(c *config.CallCorrectionConfig) {
			c.Enabled = false
			c.RecentBandBonusEnabled = true
			c.RecentBandRecordMinUniqueSpotters = 16
		}, "", "", 0},
		{"disabled_edit_and_resolver_requirements", func(c *config.CallCorrectionConfig) {
			c.StabilizerEditNeighborMinSpotters = 65
			c.ResolverRecentPlus1MinUniqueWinner = 65
		}, "", "", 0},
		{"no_history_owner_unused_requirements", func(c *config.CallCorrectionConfig) {
			c.StabilizerEnabled = false
			c.RecentBandRecordMinUniqueSpotters = 65
			c.StabilizerEditNeighborEnabled = true
			c.StabilizerEditNeighborMinSpotters = 65
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 65
		}, "", "", 0},
		{"global_disabled_unused_general_requirements", func(c *config.CallCorrectionConfig) {
			c.Enabled = false
			c.RecentBandRecordMinUniqueSpotters = 65
			c.ResolverRecentPlus1Enabled = true
			c.ResolverRecentPlus1MinUniqueWinner = 65
		}, "", "", 0},
		{"Custom_owner_not_constrained_to_legacy_64", func(c *config.CallCorrectionConfig) {
			c.CustomSCP.MaxSpottersPerKey = 128
			c.CustomSCP.Enabled = true
			c.RecentBandRecordMinUniqueSpotters = 65
		}, "", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			correction := customSCPCapacityRuntimeConfig(filepath.Join(t.TempDir(), "scp"))
			correction.CustomSCP.Enabled = false
			correction.RecentBandRecordMinUniqueSpotters = 2
			correction.MaxEditDistance = -1
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
			if tc.key != "" {
				if initialized || r.signalResolver != nil || r.recentBandStore != nil || r.customSCPStore != nil {
					t.Fatalf("invalid capacity created live owners: initialized=%t resolver=%t recent=%t custom=%t", initialized, r.signalResolver != nil, r.recentBandStore != nil, r.customSCPStore != nil)
				}
				if r.startupErr == nil {
					t.Fatal("capacity rejection did not retain its startup error")
				}
				for _, fragment := range []string{tc.key, tc.store, fmt.Sprintf("receiver capacity=%d", tc.capacity), "effective requirement"} {
					if !strings.Contains(r.startupErr.Error(), fragment) {
						t.Errorf("startup error omits %q: %v", fragment, r.startupErr)
					}
				}
				return
			}
			if !initialized || r.startupErr != nil {
				t.Fatalf("valid equality/unused requirement blocked initialization: %v", r.startupErr)
			}
			wantHistory := correction.Enabled && (correction.CustomSCP.Enabled || correction.RecentBandBonusEnabled || correction.StabilizerEnabled)
			if (r.signalResolver != nil) != correction.Enabled || (r.recentBandStore != nil) != wantHistory || (r.customSCPStore != nil) != (correction.Enabled && correction.CustomSCP.Enabled) {
				t.Errorf("valid configuration created wrong owners: resolver=%t recent=%t custom=%t", r.signalResolver != nil, r.recentBandStore != nil, r.customSCPStore != nil)
			}
		})
	}
}

func legacyRuntimeHistorySpot(call, reporter, mode string, at time.Time) *spot.Spot {
	s := newDeliveryTestSpot(call, reporter, at)
	s.Mode, s.ModeNorm = mode, mode
	if modeSupportsFTConfidenceGlyph(mode) {
		s.Frequency = 14074.1
	}
	s.SourceType = spot.SourceManual
	s.EnsureNormalized()
	return s
}

func legacyRuntimeRecordAt(p *outputPipeline, store *spot.RecentBandStore, s *spot.Spot, now time.Time) {
	if modeSupportsFTConfidenceGlyph(s.ModeNorm) {
		p.recordFTRecentBandObservationAt(s, now)
		return
	}
	recordRecentBandObservationAt(s, store, nil, p.correctionCfg, now)
}

func legacyRuntimeAssertStored(t *testing.T, f ambiguityDeliveryFixture, mode, glyph string, at time.Time) {
	t.Helper()
	frequency := 14020.0
	if modeSupportsFTConfidenceGlyph(mode) {
		frequency = 14074.1
	}
	assertRows := func(label string, rows []*spot.Spot, ring bool) {
		t.Helper()
		if len(rows) != 1 {
			t.Fatalf("%s count=%d want one accepted observation", label, len(rows))
		}
		s := rows[0]
		if s.DXCall != "K1SCP" || s.DECall != "N0DDD" || s.Mode != mode || s.Frequency != frequency || !s.Time.Equal(at) || s.Confidence != glyph || s.SourceType != spot.SourceManual || strings.TrimSpace(s.Comment) != "" || s.ConfidencePercentOK {
			t.Errorf("%s changed accepted tuple/historical confidence: %+v", label, s)
		}
		if ring && (s.SpotterIP != "192.0.2.1" || s.SnapshotForAsync() != s) {
			t.Errorf("ring lost source IP or sealed publication ownership: %+v", s)
		}
	}
	assertRows("ring", f.ring.GetRecent(4), true)
	f.writer.Stop()
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err := reopened.Recent(4)
	if err != nil {
		t.Fatal(err)
	}
	assertRows("reopened archive", rows, false)
}

func TestLegacyHistoryRuntimeRecordingClocks(t *testing.T) {
	const window = time.Minute
	for _, mode := range []string{"CW", "RTTY", "FT8"} {
		for _, offset := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
			t.Run(fmt.Sprintf("%s/%s", mode, offset), func(t *testing.T) {
				now := time.Now().UTC().Add(offset)
				store := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{Window: window, MaxSpottersPerCall: 2})
				p := &outputPipeline{correctionCfg: config.CallCorrectionConfig{RecentBandBonusEnabled: true}, ftRecentBandStore: store}
				zero := legacyRuntimeHistorySpot("K1ZERO", "N0AAA", mode, time.Time{})
				legacyRuntimeRecordAt(p, store, zero, now)
				// An immediate support read could miss wrong normalization. The
				// exact lower boundary proves zero was normalized to admissionNow.
				boundary := now.Add(window)
				if got := store.RecentSupportCount("K1ZERO", "20m", mode, boundary); got != 1 {
					t.Fatalf("zero timestamp must remain eligible exactly at admissionNow+window: count=%d", got)
				}
				if got := store.RecentSupportCountNormalized("K1ZERO", "20m", mode, boundary); got != 1 {
					t.Fatalf("normalized count disagrees at exact zero-time boundary: %d", got)
				}
				if got := store.RecentSupportCount("K1ZERO", "20m", mode, boundary.Add(time.Nanosecond)); got != 0 {
					t.Fatalf("zero timestamp must expire one nanosecond beyond admissionNow+window: %d", got)
				}
				if store.ActiveCallCount(boundary) != 0 {
					t.Fatal("expired recording left an active call reference")
				}
				for _, reporter := range []string{"N0AAA", "N0BBB"} {
					legacyRuntimeRecordAt(p, store, legacyRuntimeHistorySpot("K1KEEP", reporter, mode, now), now)
				}
				for _, reporter := range []string{"N0AAA", "N0CCC"} {
					legacyRuntimeRecordAt(p, store, legacyRuntimeHistorySpot("K1KEEP", reporter, mode, now.Add(2*window)), now)
				}
				if got := store.RecentSupportCount("K1KEEP", "20m", mode, now); got != 2 {
					t.Fatalf("future overwrite/new receiver damaged valid full-cap history: %d", got)
				}
				if store.ActiveCallCount(now) != 1 {
					t.Fatal("rejected future admission changed active-call references")
				}
				later := now.Add(10 * time.Second)
				legacyRuntimeRecordAt(p, store, legacyRuntimeHistorySpot("K1LATER", "N0DDD", mode, later), later)
				if got := store.RecentSupportCount("K1LATER", "20m", mode, now); got != 0 {
					t.Fatalf("backward query counted a later valid observation: %d", got)
				}
				if got := store.RecentSupportCount("K1LATER", "20m", mode, later); got != 1 {
					t.Fatalf("backward exclusion deleted the later observation: %d", got)
				}
			})
		}
	}
}

func TestLegacyHistoryRuntimeDeliveredFloors(t *testing.T) {
	for _, mode := range []string{"CW", "RTTY", "FT8"} {
		for _, tc := range []struct {
			name        string
			minimum     int
			futureQuery bool
			wantGlyph   string
		}{
			{"capacity_equality_future_rejection_preserves_support", 2, false, "S"},
			{"impossible_minimum_is_not_lowered", 3, false, "?"},
			{"later_observations_excluded_from_present_floor", 2, true, "?"},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				temporalDeliveryDirectory(t)
				at := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
				cfg := config.CallCorrectionConfig{RecentBandBonusEnabled: true, RecentBandWindowSeconds: 12 * 60 * 60, RecentBandRecordMinUniqueSpotters: tc.minimum}
				f := newAmbiguityDeliveryFixture(t, nil, cfg)
				if modeSupportsFTConfidenceGlyph(mode) {
					// FT is excluded from these clients' default mode selection.
					// Actual PASS/RESUME acknowledgements establish the recipient
					// eligibility before an output absence can mean a hold.
					p4FTEnableReceiver(t, f.normalConn, f.normalReader)
					p4FTEnableReceiver(t, f.diagConn, f.diagReader)
				}
				// Deliberately bypass startup for an impossible direct request.
				// Production startup capacity is separately proven above.
				store := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{Window: 12 * time.Hour, MaxSpottersPerCall: 2})
				f.pipeline.recentBandStore = store
				if modeSupportsFTConfidenceGlyph(mode) {
					f.pipeline.ftConfidence = newFTConfidenceController(cfg)
					f.pipeline.ftRecentBandStore = store
				}
				seedAt := at
				if tc.futureQuery {
					// Valid at its supplied admission clock, ineligible when the
					// present live floor queries that population backward.
					seedAt = at.Add(time.Hour)
				}
				for _, reporter := range []string{"N0AAA", "N0BBB"} {
					legacyRuntimeRecordAt(f.pipeline, store, legacyRuntimeHistorySpot("K1SCP", reporter, mode, seedAt), seedAt)
				}
				if got := store.RecentSupportCount("K1SCP", "20m", mode, seedAt); got != 2 {
					t.Fatalf("fixture must establish two eligible receivers before negative inference: %d", got)
				}
				if !tc.futureQuery {
					for _, reporter := range []string{"N0AAA", "N0CCC"} {
						legacyRuntimeRecordAt(f.pipeline, store, legacyRuntimeHistorySpot("K1SCP", reporter, mode, at.Add(24*time.Hour)), at)
					}
				}
				s := legacyRuntimeHistorySpot("K1SCP", "N0DDD", mode, at)
				if modeSupportsFTConfidenceGlyph(mode) {
					s.Confidence = ""
				}
				f.pipeline.processSpot(s, nil)
				if modeSupportsFTConfidenceGlyph(mode) {
					if f.ring.GetCount() != 0 || f.pipeline.ftConfidence.pendingSpots != 1 {
						t.Fatal("single FT observation must be held before forced release")
					}
					f.pipeline.releaseDueFT(time.Now().UTC(), true)
					if f.pipeline.ftConfidence.pendingSpots != 0 || len(f.pipeline.ftConfidence.pending) != 0 || f.pipeline.ftConfidence.queue.Len() != 0 {
						t.Fatal("forced FT release retained pending reservations")
					}
				}
				for _, diagnostic := range []bool{false, true} {
					conn, reader, comment := f.normalConn, f.normalReader, mode
					if diagnostic {
						conn, reader, comment = f.diagConn, f.diagReader, mode+" --%"
					}
					line := correctionEvidenceReadDX(t, conn, reader)
					if len(line) < 78 || strings.TrimSpace(line[27:39]) != "K1SCP" || line[71:72] != tc.wantGlyph || strings.TrimSpace(line[39:71]) != comment {
						t.Errorf("TCP diag=%t must deliver historical glyph=%s with unavailable-percentage comment=%q: %q", diagnostic, tc.wantGlyph, comment, line)
					}
				}
				frequency := "14020"
				if modeSupportsFTConfidenceGlyph(mode) {
					frequency = "14074.1"
				}
				peerOriginalAssertFields(t, f.peer, []string{frequency, "K1SCP", at.Format("02-Jan-2006"), at.Format("1504Z"), " ", "N0DDD", "N0CALL-1", "192.0.2.1"})
				if s.DXCall != "K1SCP" || s.ConfidencePercentOK || f.pipeline.tracker.CallCorrections() != 0 {
					t.Error("historical floor introduced identity mutation or current numerical confidence")
				}
				legacyRuntimeAssertStored(t, f, mode, tc.wantGlyph, at)
			})
		}
	}
}

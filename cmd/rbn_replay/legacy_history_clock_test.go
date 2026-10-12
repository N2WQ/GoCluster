// File role: Proves replay owns legacy maintenance and admission on evaluation
// time, including untouched physical keys and refusal before owner creation.
package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	rbnfeed "dxcluster/rbn"
	"dxcluster/spot"
)

func replayLegacyClockConfig() config.CallCorrectionConfig {
	cfg := replayTemporalFreshConfig()
	cfg.TemporalDecoder.Enabled = false
	cfg.RecentBandBonusEnabled, cfg.StabilizerEnabled = true, true
	cfg.RecentBandWindowSeconds, cfg.RecentBandRecordMinUniqueSpotters = 3600, 2
	return cfg
}

func replayLegacyClockRunner(t *testing.T, at time.Time) (*replayRunner, *spot.RecentBandStore) {
	t.Helper()
	r := newReplayRunner(replayRequest{})
	r.cfg, r.outDir = &config.Config{CallCorrection: replayLegacyClockConfig()}, t.TempDir()
	r.dayStart, r.dayEnd = at.UTC().Truncate(24*time.Hour), at.UTC().Truncate(24*time.Hour).Add(24*time.Hour)
	r.nextSampleAt = r.dayEnd.Add(time.Hour) // Row tests isolate row-owned maintenance.
	r.stabilityCollector = newReplayStabilityCollector(replayStabilityConfig{})
	t.Cleanup(r.close)
	if err := r.buildReplayRuntime(); err != nil {
		t.Fatal(err)
	}
	store, ok := r.recentBandStore.(*spot.RecentBandStore)
	if !ok || r.cleanupRecentBand != nil {
		t.Fatal("legacy replay owner must exist without a wall-clock cleanup owner")
	}
	// The concrete owner's existing worker handle is inspected without unsafe
	// access. No support query or elapsed-time sleep can conceal maintenance.
	if !reflect.ValueOf(store).Elem().FieldByName("quit").IsNil() {
		t.Fatal("legacy replay started a wall-clock periodic cleanup worker")
	}
	file, err := os.CreateTemp(t.TempDir(), "samples-")
	if err != nil {
		t.Fatal(err)
	}
	r.runbookSamplesFile = file
	return r, store
}

// Cardinality reads happen at single-owner barriers and do not invoke query
// pruning. Counts of untouched keys and active references must agree first.
func replayLegacyClockKeys(store *spot.RecentBandStore) int {
	shards := reflect.ValueOf(store).Elem().FieldByName("shards")
	total := 0
	for i := 0; i < shards.Len(); i++ {
		total += shards.Index(i).FieldByName("entries").Len()
	}
	return total
}

func replayLegacyClockSeedUntouched(store *spot.RecentBandStore, at time.Time) {
	store.RecordAt("K9AAA", "40m", "CW", "N0HST", time.Time{}, at)
	store.RecordAt("K9BBB", "80m", "CW", "N0HST", time.Time{}, at)
}

func replayLegacyClockAssertUntouched(t *testing.T, store *spot.RecentBandStore, now time.Time, retained bool) {
	t.Helper()
	want := 0
	if retained {
		want = 1
	}
	bands := store.ActiveCallCountsByBand(now)
	if bands["40m"] != want || bands["80m"] != want {
		t.Fatalf("untouched physical support references=%v, retained=%t", bands, retained)
	}
	if replayLegacyClockKeys(store) != store.ActiveCallCount(now) {
		t.Fatal("distinct untouched/released calls disagree with physical key cardinality")
	}
}

func TestReplayLegacyClockMaintenanceCadence(t *testing.T) {
	at := time.Date(2020, 2, 14, 12, 0, 0, 123, time.UTC)
	r := &replayRunner{recentBandStore: spot.NewRecentBandStore(time.Minute)}
	store := r.recentBandStore.(*spot.RecentBandStore)
	replayLegacyClockSeedUntouched(store, at)
	r.maintainLegacyHistory(time.Time{})
	if !r.nextLegacyCleanup.IsZero() || replayLegacyClockKeys(store) != 2 {
		t.Fatal("zero replay maintenance clock mutated state or invented a wall deadline")
	}
	r.maintainLegacyHistory(at)
	if !r.nextLegacyCleanup.Equal(at.Add(10 * time.Minute)) {
		t.Fatalf("first evaluation clock deadline=%v", r.nextLegacyCleanup)
	}
	// Rows are already expired at this clock, but maintenance is not due.
	// Inspect physical keys before any query can prune them opportunistically.
	r.maintainLegacyHistory(at.Add(10*time.Minute - time.Nanosecond))
	replayLegacyClockAssertUntouched(t, store, at, true)
	if !r.nextLegacyCleanup.Equal(at.Add(10 * time.Minute)) {
		t.Fatal("not-due maintenance advanced the sole scalar deadline")
	}
	r.maintainLegacyHistory(at.Add(10 * time.Minute))
	replayLegacyClockAssertUntouched(t, store, at, false)
	if replayLegacyClockKeys(store) != 0 || !r.nextLegacyCleanup.Equal(at.Add(20*time.Minute)) {
		t.Fatal("maintenance did not run exactly at deadline equality")
	}
	gap := at.Add(4*time.Hour + 17*time.Minute)
	r.maintainLegacyHistory(gap)
	if !r.nextLegacyCleanup.Equal(gap.Add(10 * time.Minute)) {
		t.Fatal("large evaluation gap did not schedule one interval from its current clock")
	}
	r.maintainLegacyHistory(gap)
	if !r.nextLegacyCleanup.Equal(gap.Add(10 * time.Minute)) {
		t.Fatal("repeated same-clock maintenance moved the deadline")
	}
}

func TestReplayLegacyClockMaintenanceDispatch(t *testing.T) {
	for _, path := range []string{"ordinary_row", "sample", "terminal_drain"} {
		t.Run(path, func(t *testing.T) {
			at := time.Date(2020, 2, 14, 12, 0, 0, 123, time.UTC)
			r, store := replayLegacyClockRunner(t, at)
			replayLegacyClockSeedUntouched(store, at)
			r.maintainLegacyHistory(at)
			dispatch := func(now time.Time) {
				switch path {
				case "ordinary_row":
					row := rbnHistoryRow{Time: now, FreqKHz: 14020, Band: "20m", DXCall: "K1NEW", Spotter: "N0AAA", Mode: "CW", SpotClass: rbnfeed.SpotClassCQ}
					if err := r.processCSVRow(row, true); err != nil {
						t.Fatal(err)
					}
				case "sample":
					if err := r.emitSample(now); err != nil {
						t.Fatal(err)
					}
				case "terminal_drain":
					r.drainTemporal(now, true)
				}
			}
			dispatch(at.Add(10 * time.Minute))
			replayLegacyClockAssertUntouched(t, store, at, true)
			if !r.nextLegacyCleanup.Equal(at.Add(20 * time.Minute)) {
				t.Fatal("real replay dispatch did not advance historical maintenance")
			}
			expired := at.Add(time.Hour + time.Nanosecond)
			dispatch(expired)
			replayLegacyClockAssertUntouched(t, store, expired, false)
			wantCalls := 0
			if path == "ordinary_row" {
				wantCalls = 1 // Only the accepted current row's distinct call.
			}
			if store.ActiveCallCount(expired) != wantCalls || replayLegacyClockKeys(store) != wantCalls {
				t.Fatal("expired untouched keys survived actual dispatch before queries")
			}
			// Query only after the physical readback has proved maintenance.
			if store.RecentSupportCount("K9AAA", "40m", "CW", expired) != 0 || store.RecentSupportCount("K9BBB", "80m", "CW", expired) != 0 {
				t.Fatal("maintenance-expired untouched support remained queryable")
			}
		})
	}
}

func TestReplayLegacyClockForcedTemporalRelease(t *testing.T) {
	at := time.Date(2020, 2, 14, 12, 0, 0, 123, time.UTC)
	cfg := replayTemporalFreshConfig()
	cfg.RecentBandBonusEnabled, cfg.RecentBandWindowSeconds = true, 3600
	cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 73*3600, 74*3600
	r, key := replayTemporalFreshRunner(t, cfg, at, 2*time.Minute)
	r.outDir = t.TempDir()
	t.Cleanup(r.close)
	if err := r.openRecentBandStore(); err != nil {
		t.Fatal(err)
	}
	store := r.recentBandStore.(*spot.RecentBandStore)
	replayLegacyClockSeedUntouched(store, at)
	r.maintainLegacyHistory(at)
	s := replayTemporalFreshSpot("K1ABD", at)
	replayTemporalFreshHold(t, r, key, s, at, "K1ABC")
	r.drainTemporal(at.Add(10*time.Minute), false)
	replayLegacyClockAssertUntouched(t, store, at, true)
	if len(r.temporalPending) != 1 || r.abMetrics.Temporal.HandoffAttempts != 0 {
		t.Fatal("ordinary maintenance released the future-due temporal control")
	}
	expired := at.Add(2 * time.Hour)
	r.drainTemporal(expired, true)
	replayLegacyClockAssertUntouched(t, store, expired, false)
	// The forced envelope may reach acceptance and record its original old
	// timestamp under the existing valid-admission policy. That one 20m key is
	// distinct from untouched 40m/80m history, which maintenance must remove.
	if len(r.temporalPending) != 0 || r.abMetrics.Temporal.HandoffAttempts != 1 ||
		store.ActiveCallCountsByBand(expired)["20m"] != 1 || replayLegacyClockKeys(store) != 1 {
		t.Fatal("terminal force failed acceptance ownership or untouched physical maintenance")
	}
	r.drainTemporal(expired.Add(10*time.Minute), true)
	if store.ActiveCallCount(expired) != 0 || replayLegacyClockKeys(store) != 0 || r.abMetrics.Temporal.HandoffAttempts != 1 {
		t.Fatal("later terminal maintenance retained old accepted history or repeated its handoff")
	}
}

func TestReplayLegacyClockProcessOutcomeAdmission(t *testing.T) {
	wall := time.Now().UTC().Truncate(time.Second)
	for _, offset := range []time.Duration{-24 * time.Hour, 24 * time.Hour} {
		for _, zero := range []bool{false, true} {
			name := offset.String() + "/explicit"
			if zero {
				name = offset.String() + "/zero"
			}
			t.Run(name, func(t *testing.T) {
				at := wall.Add(offset)
				r, store := replayLegacyClockRunner(t, at)
				s := replayTemporalFreshSpot("K1ZERO", at)
				s.Confidence = "V"
				if zero {
					s.Time = time.Time{}
				}
				r.processOutcome(s, replayResolverApplyOutcome{Confidence: replayConfidenceOutcome{Final: "V"}}, at)
				if r.abMetrics.Output.ConfidenceCounts.Samples != 1 || replayLegacyClockKeys(store) != 1 {
					t.Fatal("actual accepted replay outcome did not record one eligible observation")
				}
				boundary := at.Add(time.Hour)
				if store.RecentSupportCount("K1ZERO", "20m", "CW", boundary) != 1 {
					t.Fatal("actual replay recorder substituted another clock or lost boundary equality")
				}
				if store.RecentSupportCountNormalized("K1ZERO", "20m", "CW", boundary.Add(time.Nanosecond)) != 0 {
					t.Fatal("actual replay recorder failed exact zero/explicit observation expiry at +1ns")
				}
			})
		}
	}
}

func TestReplayLegacyClockProcessOutcomeRejectsFuture(t *testing.T) {
	at := time.Now().UTC().Add(24 * time.Hour)
	r, store := replayLegacyClockRunner(t, at)
	valid := replayTemporalFreshSpot("K1ZERO", at.Add(-time.Minute))
	valid.Confidence = "V"
	r.processOutcome(valid, replayResolverApplyOutcome{Confidence: replayConfidenceOutcome{Final: "V"}}, at)
	bad := replayTemporalFreshSpot("K1ZERO", at.Add(time.Nanosecond))
	bad.Confidence = "V"
	r.processOutcome(bad, replayResolverApplyOutcome{Confidence: replayConfidenceOutcome{Final: "V"}}, at)
	if replayLegacyClockKeys(store) != 1 || store.RecentSupportCount("K1ZERO", "20m", "CW", at) != 1 {
		t.Fatal("future actual replay outcome replaced valid history")
	}
	boundary := valid.Time.Add(time.Hour)
	if store.RecentSupportCount("K1ZERO", "20m", "CW", boundary) != 1 || store.RecentSupportCount("K1ZERO", "20m", "CW", boundary.Add(time.Nanosecond)) != 0 {
		t.Fatal("future refusal changed the valid observation timestamp")
	}
}

func TestReplayLegacyClockCapacityBeforeOwnersAndScratch(t *testing.T) {
	for _, constructor := range []string{"open_store", "build_runtime"} {
		for _, gate := range []string{"stabilizer", "plus1"} {
			t.Run(constructor+"/"+gate, func(t *testing.T) {
				r := newReplayRunner(replayRequest{})
				cfg := replayLegacyClockConfig()
				key := "recent_band_record_min_unique_spotters"
				if gate == "stabilizer" {
					cfg.RecentBandRecordMinUniqueSpotters = 65
				} else {
					cfg.ResolverRecentPlus1Enabled, cfg.ResolverRecentPlus1MinUniqueWinner = true, 65
					key = "resolver_recent_plus1_min_unique_winner"
				}
				r.cfg, r.outDir = &config.Config{CallCorrection: cfg}, t.TempDir()
				t.Cleanup(r.close)
				scratch := filepath.Join(r.outDir, "custom_scp_runtime")
				if err := os.Mkdir(scratch, 0o755); err != nil {
					t.Fatal(err)
				}
				sentinel := filepath.Join(scratch, "existing-evidence")
				const contents = "must survive legacy capacity refusal"
				if err := os.WriteFile(sentinel, []byte(contents), 0o600); err != nil {
					t.Fatal(err)
				}
				construct := r.openRecentBandStore
				if constructor == "build_runtime" {
					construct = r.buildReplayRuntime
				}
				err := construct()
				if err == nil || !strings.Contains(err.Error(), key) || !strings.Contains(err.Error(), "65") || !strings.Contains(err.Error(), "capacity=64") || !strings.Contains(err.Error(), "replay legacy") {
					t.Fatalf("capacity refusal lacks effective requirement/store/cap diagnostics: %v", err)
				}
				if r.recentBandStore != nil || r.cleanupRecentBand != nil || r.resolver != nil || r.driver != nil || r.temporalDecoder != nil || r.tracker != nil {
					t.Fatal("invalid legacy capacity created relevant runtime owners")
				}
				if data, err := os.ReadFile(sentinel); err != nil || string(data) != contents {
					t.Fatalf("capacity refusal changed scratch evidence: %q %v", data, err)
				}
			})
		}
	}
}

func TestReplayLegacyClockUnusedConsumersAndDisabledOwners(t *testing.T) {
	for _, control := range []string{"equal64", "FT_only17", "unused_floor65", "unused_edit65", "disabled", "no_history", "custom"} {
		t.Run(control, func(t *testing.T) {
			cfg := replayLegacyClockConfig()
			switch control {
			case "equal64":
				cfg.RecentBandRecordMinUniqueSpotters = 64
			case "FT_only17":
				cfg.RecentBandRecordMinUniqueSpotters = 17
			case "unused_floor65":
				cfg.StabilizerEnabled, cfg.RecentBandRecordMinUniqueSpotters = false, 65
			case "unused_edit65":
				cfg.StabilizerEditNeighborMinSpotters = 65
			case "disabled":
				cfg.Enabled, cfg.RecentBandRecordMinUniqueSpotters = false, 65
			case "no_history":
				cfg.RecentBandBonusEnabled, cfg.StabilizerEnabled, cfg.RecentBandRecordMinUniqueSpotters = false, false, 65
			case "custom":
				cfg.CustomSCP.Enabled, cfg.CustomSCP.MaxKeys, cfg.CustomSCP.MaxSpottersPerKey = true, 8, 8
			}
			r := &replayRunner{cfg: &config.Config{CallCorrection: cfg}, outDir: t.TempDir()}
			t.Cleanup(r.close)
			if err := r.openRecentBandStore(); err != nil {
				t.Fatal(err)
			}
			at := time.Date(2020, 2, 14, 12, 0, 0, 123, time.UTC)
			r.maintainLegacyHistory(at)
			if control == "disabled" || control == "no_history" || control == "custom" {
				if !r.nextLegacyCleanup.IsZero() {
					t.Fatal("legacy maintenance changed disabled/CustomSCP ownership")
				}
				if control == "custom" {
					if _, ok := r.recentBandStore.(*spot.CustomSCPStore); !ok || r.cleanupRecentBand == nil {
						t.Fatal("CustomSCP lost its existing store/cleanup ownership")
					}
				} else if r.recentBandStore != nil || r.cleanupRecentBand != nil {
					t.Fatal("disabled history created an owner")
				}
			} else if !r.nextLegacyCleanup.Equal(at.Add(10*time.Minute)) || r.cleanupRecentBand != nil {
				t.Fatal("valid legacy capacity did not initialize evaluation-owned maintenance")
			}
		})
	}
}

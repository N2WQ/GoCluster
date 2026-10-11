//go:build !qualification

// File role: Keeps historical consumer policies independent through actual
// runtime construction, stabilizer admission and TCP/peer/archive delivery.
package cluster

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/peer"
	"dxcluster/spot"

	"gopkg.in/yaml.v3"
)

func customSCPConsumerRuntime(t *testing.T, correction config.CallCorrectionConfig) *clusterRuntime {
	t.Helper()
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
	if !r.initializeSignalResolverAndRecentStore() {
		t.Fatal("production history/resolver initialization failed")
	}
	if r.customSCPStore == nil || r.recentBandStore != r.customSCPStore {
		t.Fatal("runtime must expose one concrete canonical owner for lookup, recording, stats and cleanup")
	}
	return r
}

type customSCPConsumerReceipt struct{ call, glyph string }

// Only the FIFO barrier has a published current resolver population. Its
// retained minority call has 33% support and produces P without V admission;
// the separately keyed subject still exercises static S and the history gate.
func customSCPConsumerBarrierResolver(t *testing.T, at time.Time) *spot.SignalResolver {
	t.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 8, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 4,
		InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
		HysteresisWindows: 1, MaxEditDistance: 6,
	})
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14021, "20m", "CW", 500)
	ambiguityDeliveryAdd(t, r, key, "K1BAR", "N0AAA", 14021, at)
	ambiguityDeliveryAdd(t, r, key, "K1BAQ", "N0BBB", 14021, at)
	ambiguityDeliveryAdd(t, r, key, "K1BAQ", "N0CCC", 14021, at)
	if driver.Drain() != 3 {
		t.Fatal("positive FIFO barrier evidence was not admitted")
	}
	driver.Sweep(at.Add(2 * time.Nanosecond))
	if snapshot, ok := r.Lookup(key); !ok || snapshot.State != spot.ResolverStateConfident || snapshot.TotalReporters != 3 ||
		snapshot.Winner != "K1BAQ" || snapshot.WinnerSupport != 2 || snapshot.RunnerUp != "K1BAR" || snapshot.RunnerSupport != 1 {
		t.Fatalf("positive FIFO barrier requires a published 2-versus-1 population with an unambiguous leader: snapshot=%+v present=%t", snapshot, ok)
	}
	return r
}

// An independently eligible P spot is a positive FIFO barrier. Stop on that
// receipt rather than waiting negatively for a possibly held historical spot.
func customSCPConsumerReadThroughBarrier(t *testing.T, f ambiguityDeliveryFixture, want []customSCPConsumerReceipt, at time.Time) {
	t.Helper()
	for _, diagnostic := range []bool{false, true} {
		conn, reader := f.normalConn, f.normalReader
		if diagnostic {
			conn, reader = f.diagConn, f.diagReader
		}
		var got []customSCPConsumerReceipt
		for i := 0; i < 3; i++ {
			line := correctionEvidenceReadDX(t, conn, reader)
			if len(line) < 78 {
				t.Fatalf("short actual TCP output: %q", line)
			}
			receipt := customSCPConsumerReceipt{strings.TrimSpace(line[27:39]), line[71:72]}
			got = append(got, receipt)
			if receipt.call == "K1BAR" {
				break
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("actual TCP historical-policy sequence diagnostic=%t: got=%+v want=%+v", diagnostic, got, want)
		}
	}
	var calls []string
	for i := 0; i < 3; i++ {
		frame, err := peer.ParseFrame(peerAdmissionReadSpot(t, f.peer))
		if err != nil || frame.Type != "PC61" || len(frame.Fields) != 8 {
			t.Fatalf("actual peer receipt: frame=%+v err=%v", frame, err)
		}
		call := frame.Fields[1]
		if call != customSCPDeliveryCall && call != "K1BAR" {
			t.Errorf("peer received unexpected original identity %q", call)
		}
		frequency := "14020"
		if call == "K1BAR" {
			frequency = "14021"
		}
		wantFields := []string{frequency, call, at.Format("02-Jan-2006"), at.Format("1504Z"), " ", "N0AAA", "N0CALL-1", "192.0.2.1"}
		if !reflect.DeepEqual(frame.Fields, wantFields) {
			t.Errorf("peer changed original source fields: got=%q want=%q", frame.Fields, wantFields)
		}
		calls = append(calls, call)
		if call == "K1BAR" {
			break
		}
	}
	wantCalls := make([]string, 0, len(want))
	for _, receipt := range want {
		wantCalls = append(wantCalls, receipt.call)
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Errorf("actual peer historical-policy sequence: got=%q want=%q", calls, wantCalls)
	}
}

func TestCustomSCPConsumerDeliveredStabilizerIndependence(t *testing.T) {
	// All three distinct receivers/cells are just over five days old: each has
	// the documented score 5. Static recognition is deliberately identical.
	for _, tc := range []struct {
		name                                              string
		resolverScore, resolverCells, resolverCount       int
		stabilizerScore, stabilizerCells, stabilizerCount int
		wantResolver, wantHold                            bool
	}{
		{"resolver_score_permissive", 3, 2, 2, 7, 2, 2, true, true},
		{"stabilizer_score_permissive", 7, 2, 2, 3, 2, 2, false, false},
		{"resolver_cells_permissive", 3, 2, 2, 3, 4, 2, true, true},
		{"stabilizer_cells_permissive", 3, 4, 2, 3, 2, 2, false, false},
		{"resolver_count_permissive", 3, 2, 2, 3, 2, 4, true, true},
		{"stabilizer_count_permissive", 3, 2, 4, 3, 2, 2, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			at := time.Now().UTC().Truncate(time.Second)
			path := filepath.Join(t.TempDir(), "scp")
			raw := customSCPFreshnessSeed(t, path, at.Add(-5*24*time.Hour), true, true)
			customSCPFreshnessAssertBytes(t, path, raw)
			cfg := config.CallCorrectionConfig{
				Enabled: true, StabilizerEnabled: true, RecencySeconds: 120, FrequencyToleranceHz: 500,
				MinConsensusReports: 3, MaxEditDistance: 6,
				StabilizerMaxChecks: 1, StabilizerTimeoutAction: stabilizerTimeoutRelease,
				RecentBandRecordMinUniqueSpotters: 2,
				CustomSCP: config.CallCorrectionCustomSCPConfig{
					Enabled: true, Path: path, HistoryHorizonDays: 60, StaticHorizonDays: 395,
					MaxKeys: 8, MaxSpottersPerKey: 8, CleanupIntervalSeconds: 3600, BlockCacheMB: 1, MemTableSizeMB: 4,
					ResolverMinScore: tc.resolverScore, ResolverMinUniqueH3Cells: tc.resolverCells, ResolverMinUniqueSpotters: tc.resolverCount,
					StabilizerMinScore: tc.stabilizerScore, StabilizerMinUniqueH3Cells: tc.stabilizerCells, StabilizerMinUniqueSpotters: tc.stabilizerCount,
					SFloorMinScore: 3, SFloorMinUniqueSpottersExact: 2, SFloorMinUniqueH3CellsExact: 2,
					SFloorMinUniqueSpottersFamily: 2, SFloorMinUniqueH3CellsFamily: 2,
				},
			}
			r := customSCPConsumerRuntime(t, cfg)
			if r.customSCPStore.StatsSnapshot().ObservationSpotters != 3 || !r.customSCPStore.StaticContains(customSCPDeliveryCall) {
				t.Fatal("production initializer did not recover all observations and static recognition")
			}
			probe := newDeliveryTestSpot(customSCPDeliveryCall, "N0AAA", at)
			probe.SourceType, probe.Confidence = spot.SourceManual, "S"
			resolverQuery := func() bool {
				return r.recentBandStore.HasRecentSupport(customSCPDeliveryCall, "20m", "CW", tc.resolverCount, at)
			}
			stabilizerQuery := func() bool {
				return evaluateTelnetStabilizerDelay(probe, r.recentBandStore, cfg, at, spot.ResolverSnapshot{}, false).ShouldDelay
			}
			for _, resolverFirst := range []bool{true, false} {
				var resolverPass, hold bool
				if resolverFirst {
					resolverPass, hold = resolverQuery(), stabilizerQuery()
				} else {
					hold, resolverPass = stabilizerQuery(), resolverQuery()
				}
				if resolverPass != tc.wantResolver || hold != tc.wantHold {
					t.Errorf("consumer queries must use independent policy without order effects: resolverFirst=%t resolver=%t hold=%t want resolver=%t hold=%t",
						resolverFirst, resolverPass, hold, tc.wantResolver, tc.wantHold)
				}
			}
			f := newAmbiguityDeliveryFixture(t, customSCPConsumerBarrierResolver(t, at), cfg)
			f.pipeline.recentBandStore, f.pipeline.customSCPStore = r.recentBandStore, r.customSCPStore
			f.pipeline.stabilizerEnabled = true
			f.pipeline.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 4)
			t.Cleanup(f.pipeline.telnetStabilizer.Stop)
			s := newDeliveryTestSpot(customSCPDeliveryCall, "N0AAA", at)
			s.SourceType, s.Comment = spot.SourceManual, " "
			f.pipeline.processSpot(s, nil)
			if s.Confidence != "S" {
				t.Errorf("same static recognition must remain separate from consumer recency: glyph=%s", s.Confidence)
			}
			barrier := newDeliveryTestSpot("K1BAR", "N0AAA", at)
			barrier.SourceType, barrier.Comment, barrier.Frequency = spot.SourceManual, " ", 14021
			f.pipeline.processSpot(barrier, nil)
			want := []customSCPConsumerReceipt{{"K1BAR", "P"}}
			stored := map[string]string{"K1BAR": "P"}
			wantPending := int64(1)
			if !tc.wantHold {
				want = []customSCPConsumerReceipt{{customSCPDeliveryCall, "S"}, {"K1BAR", "P"}}
				stored[customSCPDeliveryCall], wantPending = "S", 0
			}
			customSCPConsumerReadThroughBarrier(t, f, want, at)
			if f.pipeline.telnetStabilizer.Pending() != wantPending {
				t.Errorf("delivered consumer decision lost hold ownership: pending=%d want %d", f.pipeline.telnetStabilizer.Pending(), wantPending)
			}
			customSCPFreshnessCheckStored(t, f, stored)
			f.pipeline.telnetStabilizer.Stop()
			if f.pipeline.telnetStabilizer.Pending() != 0 || r.customSCPStore.StatsSnapshot().ObservationSpotters != 3 {
				t.Fatal("consumer queries or hold retirement changed canonical retained evidence")
			}
			if err := r.customSCPStore.Close(); err != nil {
				t.Fatal(err)
			}
			customSCPFreshnessAssertBytes(t, path, raw)
		})
	}
}

func TestCustomSCPConsumerCanonicalRecordingAndCleanupOwner(t *testing.T) {
	temporalDeliveryDirectory(t)
	at := time.Now().UTC().Truncate(time.Second)
	path := filepath.Join(t.TempDir(), "scp")
	customSCPFreshnessSeed(t, path, at.Add(-5*24*time.Hour), true, true)
	cfg := config.CallCorrectionConfig{Enabled: true, CustomSCP: config.CallCorrectionCustomSCPConfig{
		Enabled: true, Path: path, HistoryHorizonDays: 60, StaticHorizonDays: 395,
		MaxKeys: 8, MaxSpottersPerKey: 8, CleanupIntervalSeconds: 3600, BlockCacheMB: 1, MemTableSizeMB: 4,
		ResolverMinScore: 3, ResolverMinUniqueSpotters: 2, ResolverMinUniqueH3Cells: 2,
		StabilizerMinScore: 7, StabilizerMinUniqueSpotters: 4, StabilizerMinUniqueH3Cells: 4,
	}}
	r := customSCPConsumerRuntime(t, cfg)
	before := r.customSCPStore.StatsSnapshot()
	s := newDeliveryTestSpot("K1REC", "N0AAA", at)
	s.SourceType, s.Confidence = spot.SourceManual, "V"
	// Use the production recording route with the initialized canonical owner.
	// A query-only wrapper must not make this concrete admission disappear.
	recordRecentBandObservation(s, r.recentBandStore, r.customSCPStore, cfg)
	after := r.customSCPStore.StatsSnapshot()
	if after.ObservationSpotters != before.ObservationSpotters+1 || after.ObservationKeys != before.ObservationKeys+1 || !r.customSCPStore.StaticContains("K1REC") {
		t.Errorf("production admission must reach canonical stats/static history: before=%+v after=%+v recordedStatic=%t", before, after, r.customSCPStore.StaticContains("K1REC"))
	}
	r.recentBandStore.StopCleanup()
	if err := r.customSCPStore.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := customSCPFreshnessOpen(t, spot.CustomSCPOptions{
		Path: path, HorizonDays: 60, StaticHorizonDays: 395, MaxSpottersPerKey: 8, CoreMinScore: 3, CoreMinH3Cells: 2,
	})
	if reopened.StatsSnapshot().ObservationSpotters != 4 || !reopened.StaticContains("K1REC") || !reopened.StaticContains(customSCPDeliveryCall) {
		t.Errorf("canonical stop/close must preserve independently reopened recording: stats=%+v recordedStatic=%t", reopened.StatsSnapshot(), reopened.StaticContains("K1REC"))
	}
}

// Run the actual configuration loader on a private copy of the existing
// complete configuration. Only the two count requirements differ between runs.
func customSCPConsumerFTConfig(t *testing.T, sourceDir string, stabilizerCount int) config.CallCorrectionConfig {
	t.Helper()
	directory := t.TempDir()
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.EqualFold(filepath.Ext(entry.Name()), ".yaml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(sourceDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if entry.Name() == "pipeline.yaml" {
			var pipeline map[string]any
			if err := yaml.Unmarshal(data, &pipeline); err != nil {
				t.Fatal(err)
			}
			correction, ok := pipeline["call_correction"].(map[string]any)
			if !ok {
				t.Fatal("existing pipeline lacks call_correction mapping")
			}
			custom, ok := correction["custom_scp"].(map[string]any)
			if !ok {
				t.Fatal("existing pipeline lacks custom_scp mapping")
			}
			correction["recent_band_bonus_enabled"] = true
			correction["recent_band_record_min_unique_spotters"] = 2
			custom["enabled"] = true
			custom["resolver_min_unique_spotters"] = 2
			custom["stabilizer_min_unique_spotters"] = stabilizerCount
			data, err = yaml.Marshal(pipeline)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(directory, entry.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := config.Load(directory)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.CallCorrection.RecentBandRecordMinUniqueSpotters != 2 {
		t.Errorf("loading stabilizer count %d changed the independently configured FT/common minimum: got %d want 2", stabilizerCount, loaded.CallCorrection.RecentBandRecordMinUniqueSpotters)
	}
	return loaded.CallCorrection
}

func TestCustomSCPConsumerStabilizerCountDoesNotChangeDeliveredFTFloor(t *testing.T) {
	sourceDir, err := filepath.Abs(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{2, 5} {
		name := "stabilizer_count_two"
		if count == 5 {
			name = "stabilizer_count_five"
		}
		t.Run(name, func(t *testing.T) {
			cfg := customSCPConsumerFTConfig(t, sourceDir, count)
			temporalDeliveryDirectory(t)
			f := newAmbiguityDeliveryFixture(t, nil, cfg)
			p4FTEnableReceiver(t, f.normalConn, f.normalReader)
			p4FTEnableReceiver(t, f.diagConn, f.diagReader)
			p := f.pipeline
			p.recentBandStore, p.customSCPStore = nil, nil
			p.ftConfidence, p.ftRecentBandStore = newFTConfidenceController(cfg), newFTRecentBandStore(cfg)
			if p.ftRecentBandStore == nil {
				t.Fatal("loaded FT history must be enabled")
			}
			at := time.Now().UTC().Truncate(time.Second)
			for _, receiver := range []string{"N0AAA", "N0BBB"} {
				p.ftRecentBandStore.Record("K1FLO", "20m", "FT8", receiver, at.Add(-time.Minute))
			}
			s := spot.NewSpot("K1FLO", "N0CCC", 14074.13, "FT8")
			s.SourceType, s.Time, s.Comment, s.SpotterIP = spot.SourceManual, at, " ", "192.0.2.1"
			p.processSpot(s, nil)
			if p.ftConfidence.pendingSpots != 1 || f.ring.GetCount() != 0 {
				t.Fatal("production single FT report must enter the bounded burst before support-floor evaluation")
			}
			p.releaseDueFT(time.Now().UTC(), true)
			peerOriginalAssertTCP(t, f, "K1FLO", "S")
			peerOriginalAssertFields(t, f.peer, []string{
				"14074.13", "K1FLO", at.Format("02-Jan-2006"), at.Format("1504Z"), " ", "N0CCC", "N0CALL-1", "192.0.2.1",
			})
			if s.ConfidencePercentOK || p.ftConfidence.pendingSpots != 0 {
				t.Error("historical FT recognition must not invent a current percentage or retain its burst")
			}
			customSCPFreshnessCheckStored(t, f, map[string]string{"K1FLO": "S"})
		})
	}
}

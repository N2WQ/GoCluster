package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/stats"
	"howett.net/plist"
)

// These literal representative points define the expected grids independently
// of the implementation. They are private fixtures, never archive observations.
func studyTestCTYPoints() map[string]cty.PrefixInfo {
	return map[string]cty.PrefixInfo{
		"K":   {Prefix: "K", Latitude: 40, Longitude: 74},
		"DL":  {Prefix: "DL", Latitude: 50, Longitude: -10},
		"N0B": {Prefix: "N0B", Latitude: 40, Longitude: 74},
		"N0C": {Prefix: "N0C", Latitude: 40, Longitude: 74},
		"N0D": {Prefix: "N0D", Latitude: 50, Longitude: -10},
		"N0F": {Prefix: "N0F", Latitude: 50, Longitude: -10},
	}
}

func studyTestCTYBytes(t *testing.T, points map[string]cty.PrefixInfo) []byte {
	t.Helper()
	data, err := plist.Marshal(points, plist.XMLFormat)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func studyTestCTY(t *testing.T, points map[string]cty.PrefixInfo) *cty.CTYDatabase {
	t.Helper()
	db, err := cty.LoadCTYDatabaseFromReader(bytes.NewReader(studyTestCTYBytes(t, points)))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// Runtime fixtures use real frozen H3 input files and a private CTY plist. No
// source model/license file is replaced or fabricated to create qualification.
func studyTestBindGeography(t *testing.T, p *studyPlan, cfg *config.Config, points map[string]cty.PrefixInfo) {
	t.Helper()
	cfg.CTY.Enabled, cfg.CTY.File = true, filepath.Join(t.TempDir(), "cty.plist")
	if err := os.WriteFile(cfg.CTY.File, studyTestCTYBytes(t, points), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.H3TablePath = testsupport.H3TableDir(t)
	paths := []string{cfg.CTY.File, filepath.Join(cfg.H3TablePath, "res1.bin"), filepath.Join(cfg.H3TablePath, "res2.bin")}
	assets := make([]studyFile, 0, 3)
	for _, path := range paths {
		sum, err := studyHashFile(path)
		if err != nil {
			t.Fatal(err)
		}
		assets = append(assets, studyFile{Path: path, SHA256: sum})
	}
	p.InputScenario = studyInputScenario{ReceiverGeography: studyReceiverGeography,
		CTYSHA256: assets[0].SHA256, H3Res1SHA256: assets[1].SHA256, H3Res2SHA256: assets[2].SHA256}
	for i := range p.Arms {
		p.Arms[i].Assets = append([]studyFile(nil), assets...)
	}
}

func TestCCV2StudyReceiverGeography(t *testing.T) {
	points := studyTestCTYPoints()
	points["Z"] = cty.PrefixInfo{Prefix: "Z", Latitude: 0, Longitude: 0}
	points["Q"] = cty.PrefixInfo{Prefix: "Q", Latitude: 91, Longitude: 0}
	db := studyTestCTY(t, points)
	for _, tc := range []struct{ receiver, lookup, prefix, grid, reason string }{
		{"K1ABC", "K1ABC", "K", "FN30", "cty_representative"},
		{" dl1abc-7 ", "DL1ABC", "DL", "JO50", "cty_representative"},
		{"K1ABC/DL-#", "K1ABC/DL", "DL", "JO50", "cty_representative"},
		{"DL/K1ABC-2", "DL/K1ABC", "DL", "JO50", "cty_representative"},
		{"Z1ABC", "Z1ABC", "Z", "JJ00", "cty_representative"},
		{"Q1ABC", "Q1ABC", "Q", "", "invalid_coordinates"},
		{"X1ABC", "X1ABC", "", "", "cty_no_match"},
		{"", "", "", "", "empty_receiver"},
	} {
		t.Run(tc.receiver, func(t *testing.T) {
			got := studyDeriveReceiverLocation(db, tc.receiver)
			if got.LookupCall != tc.lookup || got.CTYPrefix != tc.prefix || got.Grid != tc.grid || got.Reason != tc.reason ||
				got.Known != (tc.grid != "") || got.Derived != (tc.grid != "") || !studyReceiverLocationBinding(got, tc.receiver) {
				t.Fatalf("literal CTY point/portable precedence: %+v", got)
			}
			s := spot.NewSpotNormalized("K1ABC", tc.receiver, 14020, "CW")
			studyApplyReceiverLocation(s, got)
			if s.DEMetadata.Grid != tc.grid || s.DEGridNorm != tc.grid || s.DEMetadata.GridDerived != got.Derived {
				t.Fatalf("derived provenance did not reach normalized DE input: %+v", s)
			}
		})
	}
	// Invalid finite/nonfinite coordinates belong to an existing record's input
	// uncertainty. They do not masquerade as absent or corrupt CTY assets.
	for _, bad := range []float64{181, math.NaN(), math.Inf(1)} {
		db.Data["Q"] = cty.PrefixInfo{Prefix: "Q", Latitude: 0, Longitude: bad}
		got := studyDeriveReceiverLocation(db, "Q1ABC")
		if got.Reason != "invalid_coordinates" || got.Known {
			t.Fatalf("invalid coordinates became known: %+v", got)
		}
		if _, err := json.Marshal(got); err != nil {
			t.Fatalf("invalid coordinates escaped into serialized decision: %v", err)
		}
	}
}

func studyTestCustomSCPConfig() config.CallCorrectionConfig {
	cfg := replayTemporalFreshConfig()
	cfg.TemporalDecoder.Enabled = false
	cfg.RecentBandRecordMinUniqueSpotters = 4
	cfg.CustomSCP = config.CallCorrectionCustomSCPConfig{
		Enabled: true, HistoryHorizonDays: 60, StaticHorizonDays: 395, MaxKeys: 8, MaxSpottersPerKey: 8,
		ResolverMinScore: 5, ResolverMinUniqueSpotters: 4, ResolverMinUniqueH3Cells: 2,
		StabilizerMinScore: 5, StabilizerMinUniqueSpotters: 2, StabilizerMinUniqueH3Cells: 2,
		SFloorMinScore: 3, SFloorMinUniqueSpottersExact: 2, SFloorMinUniqueH3CellsExact: 2,
		SFloorMinUniqueSpottersFamily: 3, SFloorMinUniqueH3CellsFamily: 3,
		CleanupIntervalSeconds: 3600, BlockCacheMB: 1, MemTableSizeMB: 4,
	}
	return cfg
}

func TestCCV2StudyCSVGeographyReachesCustomSCP(t *testing.T) {
	for _, scenario := range []string{"two_cells", "same_cell", "unknown", "ordinary_no_study"} {
		t.Run(scenario, func(t *testing.T) {
			// A current private day avoids depending on the store's wall-clock
			// 60-day retention horizon. The expected four receivers/two cells are
			// literal; no DE grid, confidence, or history observation is seeded.
			date := time.Now().UTC().Format("2006-01-02")
			var csv strings.Builder
			for i := 0; i < 12; i++ {
				receiver := []string{"N0BBB", "N0CCC", "N0DDD", "N0FFF"}[i%4]
				fmt.Fprintf(&csv, "%s,14020,20m,K1ABC,30,%s 00:01:%02d,CQ,CW\n", receiver, date, i*2)
			}
			// B is retained in the raw denominator but cannot learn even though
			// it arrives after consensus has reached V and its CTY grid is known.
			fmt.Fprintf(&csv, "N0AAA,14020,20m,K1ABC,30,%s 00:01:30,CQ,CW\n", date)
			in := studyInput{DateUTC: date, Phase: "tuning", Archive: studyTestZIPDate(t, date, csv.String()), MaxRows: 13, MaxCSVBytes: 4096}
			day, err := loadStudyRawDay(in, nil, 300)
			if err != nil {
				t.Fatal(err)
			}
			p, cfg := studyTestPlan(t), &config.Config{CallCorrection: studyTestCustomSCPConfig()}
			points := studyTestCTYPoints()
			points["N0A"] = cty.PrefixInfo{Prefix: "N0A", Latitude: 50, Longitude: -10}
			if scenario == "same_cell" {
				points["N0D"], points["N0F"] = points["N0B"], points["N0C"]
			}
			if scenario == "unknown" {
				points = map[string]cty.PrefixInfo{"K": points["K"]}
			}
			studyTestBindGeography(t, &p, cfg, points)
			r := newReplayRunner(replayRequest{dateValue: date})
			r.cfg, r.replayCfg, r.outDir, r.zipPath = cfg, defaultReplayConfig(), t.TempDir(), in.Archive.Path
			t.Cleanup(r.close)
			if err := r.resolveDay(); err != nil {
				t.Fatal(err)
			}
			if err := pathreliability.InitRequiredH3Mappings(cfg.H3TablePath); err != nil {
				t.Fatal(err)
			}
			if err := r.configureExternalDependencies(); err != nil {
				t.Fatal(err)
			}
			if err := r.buildReplayRuntime(); err != nil {
				t.Fatal(err)
			}
			if err := r.openReplayOutputs(); err != nil {
				t.Fatal(err)
			}
			if scenario != "ordinary_no_study" {
				r.study, err = newStudyObserver(day, r, filepath.Join(r.outDir, "decisions.jsonl"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = r.study.close() })
			}
			// Use the ordinary replay entry point, which initializes the sample
			// clock and services real scheduled work and EOF maintenance.
			if err := r.replayDay(); err != nil {
				t.Fatal(err)
			}
			store, ok := r.recentBandStore.(*spot.CustomSCPStore)
			if !ok {
				t.Fatal("actual replay did not construct canonical CustomSCP owner")
			}
			if r.abMetrics.Output.ConfidenceCounts.V < 4 {
				t.Fatalf("actual resolver did not produce V learning observations: %+v", r.abMetrics.Output.ConfidenceCounts)
			}
			settings := correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{
				Cfg: cfg.CallCorrection, RecentBandStore: store})
			if settings.RecentBandRecordMinUniqueSpotters != 4 || settings.RecentBandCustomSCPRequirements != (spot.CustomSCPRequirements{MinScore: 5, MinH3Cells: 2}) {
				t.Fatalf("mapped consumer requirements changed: %+v", settings)
			}
			count := store.RecentSupportCountWithRequirements("K1ABC", "20m", "CW", settings.RecentBandCustomSCPRequirements, r.lastRecordTime)
			want := 0
			if scenario == "two_cells" {
				want = 4
			}
			if count != want || store.HasRecentSupportWithRequirements("K1ABC", "20m", "CW", settings.RecentBandRecordMinUniqueSpotters, settings.RecentBandCustomSCPRequirements, r.lastRecordTime) != (want == 4) {
				t.Fatalf("CSV→resolver V→real CustomSCP count=%d want=%d stats=%+v", count, want, store.StatsSnapshot())
			}
			if r.study != nil {
				if err := r.study.complete(); err != nil {
					t.Fatal(err)
				}
				if r.resolver.MetricsSnapshot().Accepted != 12 || r.study.inputCoverage.FocalRows != 12 || !r.study.inputCoverage.complete(12) {
					t.Fatalf("B learning or incomplete warmup input coverage: %+v", r.study.inputCoverage)
				}
				if r.study.coverage.Denominator != 12 || r.study.coverage.R1Strong != 0 || r.study.coverage.R2Strong != 0 || r.study.coverage.R1Unknown["unavailable_context"] != 12 || r.study.coverage.R2Unknown["unavailable_context"] != 12 {
					t.Fatalf("warmup rows left full reference denominator: %+v", r.study.coverage)
				}
				if scenario == "unknown" && r.study.inputCoverage.UnknownNoMatch != 12 || scenario != "unknown" && r.study.inputCoverage.Known != 12 {
					t.Fatalf("known/unknown focal-A coverage changed: %+v", r.study.inputCoverage)
				}
				if store.StatsSnapshot().ObservationSpotters != 4 {
					t.Fatalf("resolver V learning must retain exactly four A receivers: %+v", store.StatsSnapshot())
				}
			} else if r.resolver.MetricsSnapshot().Accepted != 13 || store.StatsSnapshot().ObservationSpotters != 5 {
				t.Fatalf("ordinary replay unexpectedly inherited A-only study inputs: accepted=%d stats=%+v", r.resolver.MetricsSnapshot().Accepted, store.StatsSnapshot())
			}
		})
	}
}

func TestCCV2StudyGeographyCoverageAndFrozenReferences(t *testing.T) {
	at := time.Date(2026, 11, 15, 0, 10, 0, 0, time.UTC)
	rows := []studyRaw{
		studyTestRaw(at.Add(-30*time.Second), "N0AAA", "N0AAA", "K1ABC", false),
		studyTestRaw(at.Add(-time.Second), "N0EEE", "N0EEE", "K1ABC", false),
		studyTestRaw(at, "N0KKK", "N0KKK", "K1ABC", false),
		studyTestRaw(at, "N0BBB", "N0BBB", "K1ABD", true),
		studyTestRaw(at.Add(time.Second), "N0CCC", "N0CCC", "K1ABD", true),
		studyTestRaw(at.Add(2*time.Second), "N0DDD", "N0DDD", "K1ABD", true),
		studyTestRaw(at.Add(81*time.Second), "N0AAA", "N0AAA", "K1ABC", false),
		studyTestRaw(at.Add(100*time.Second), "N0EEE", "N0EEE", "K1ABC", false),
		studyTestRaw(at.Add(195*time.Second), "N0KKK", "N0KKK", "K1ABC", false),
	}
	day := studyTestReferenceDay(rows)
	day.Rows[3].Episode, day.Rows[4].Episode, day.Rows[5].Episode = "frozen-episode", "frozen-episode", "frozen-episode"
	points := studyTestCTYPoints()
	delete(points, "N0C")
	points["N0D"] = cty.PrefixInfo{Prefix: "N0D", Latitude: 91}
	r := newReplayRunner(replayRequest{})
	r.cfg, r.tracker, r.ctyDB = &config.Config{}, stats.NewTracker(), studyTestCTY(t, points)
	o, err := newStudyObserver(day, r, filepath.Join(t.TempDir(), "decisions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = o.close() })
	before := hashAnyStable(day.Rows)
	for i, raw := range day.Rows {
		if err := o.checkRaw(raw.Row, raw.Valid, raw.Ordinal); err != nil {
			t.Fatal(err)
		}
		if !studyFocalRow(raw) {
			continue
		}
		s := newReplaySpot(raw.Row)
		if err := o.begin(s, raw.Ordinal); err != nil {
			t.Fatal(err)
		}
		pending := o.pending[s]
		if pending.RawSHA256 != hashAnyStable(raw.Row) || pending.EpisodeID != raw.Episode ||
			pending.R1 != day.reference(raw, false) || pending.R2Frozen != day.reference(raw, true) {
			t.Fatalf("input derivation changed raw ID/episode/reference: %+v", pending)
		}
		if i == 3 && (!pending.R1.Strong || !pending.R2Frozen.Strong || pending.R1.Identity != "K1ABC" || pending.R2Frozen.Identity != "K1ABC") {
			t.Fatalf("independent literal raw reference labels changed: %+v", pending)
		}
		status := []string{"output", "suppressed", "license_rejected"}[i-3]
		o.noteTemporal(s, "held")
		o.finish(s, replayResolverApplyOutcome{}, raw.Row.Time, status, r)
	}
	if err := o.complete(); err != nil {
		t.Fatal(err)
	}
	if before != hashAnyStable(day.Rows) || o.inputCoverage != (studyInputCoverage{FocalRows: 3, Known: 1, UnknownNoMatch: 1, UnknownCoordinates: 1}) {
		t.Fatalf("nondelivery input coverage or raw reference image changed: %+v", o.inputCoverage)
	}
	// Complete artifact input coverage is computed before decisions, including
	// warmup (covered by the actual CSV test) and every terminal status here.
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(o.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"output", "suppressed", "license_rejected"} {
		if !bytes.Contains(data, []byte(`"status":"`+status+`"`)) {
			t.Fatalf("missing terminal status %s", status)
		}
	}
}

func TestCCV2StudyGeographySetupFailuresBeforeData(t *testing.T) {
	for _, failure := range []string{"missing_cty", "wrong_cty_hash", "corrupt_cty", "empty_cty", "oversized_prefix", "corrupt_h3", "cty_disabled", "role_swap"} {
		t.Run(failure, func(t *testing.T) {
			p, cfg := studyTestPlan(t), &config.Config{CallCorrection: studyTestCustomSCPConfig()}
			studyTestBindGeography(t, &p, cfg, studyTestCTYPoints())
			if failure == "missing_cty" {
				if err := os.Remove(cfg.CTY.File); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "wrong_cty_hash" || failure == "corrupt_cty" || failure == "empty_cty" || failure == "oversized_prefix" {
				data := []byte("not a plist")
				if failure == "empty_cty" {
					data = studyTestCTYBytes(t, map[string]cty.PrefixInfo{})
				}
				if failure == "oversized_prefix" {
					data = studyTestCTYBytes(t, map[string]cty.PrefixInfo{"K": {Prefix: strings.Repeat("K", 129)}})
				}
				if err := os.WriteFile(cfg.CTY.File, data, 0o600); err != nil {
					t.Fatal(err)
				}
				if failure != "wrong_cty_hash" {
					p.InputScenario.CTYSHA256 = fmt.Sprintf("%x", studySHA256Bytes(data))
					for i := range p.Arms {
						p.Arms[i].Assets[0].SHA256 = p.InputScenario.CTYSHA256
					}
				}
			}
			if failure == "corrupt_h3" {
				cfg.H3TablePath = t.TempDir()
				for i, name := range []string{"res1.bin", "res2.bin"} {
					data := []byte("not a geography table")
					path := filepath.Join(cfg.H3TablePath, name)
					if err := os.WriteFile(path, data, 0o600); err != nil {
						t.Fatal(err)
					}
					sum := fmt.Sprintf("%x", studySHA256Bytes(data))
					for j := range p.Arms {
						p.Arms[j].Assets[i+1] = studyFile{Path: path, SHA256: sum}
					}
					if i == 0 {
						p.InputScenario.H3Res1SHA256 = sum
					} else {
						p.InputScenario.H3Res2SHA256 = sum
					}
				}
			}
			if failure == "cty_disabled" {
				cfg.CTY.Enabled = false
			}
			if failure == "role_swap" {
				p.InputScenario.H3Res1SHA256, p.InputScenario.H3Res2SHA256 = p.InputScenario.H3Res2SHA256, p.InputScenario.H3Res1SHA256
			}
			out := t.TempDir()
			in := studyInput{DateUTC: "2026-11-15", Archive: studyFile{Path: filepath.Join(t.TempDir(), "uninspected.zip"), SHA256: strings.Repeat("a", 64)}}
			err := studyRunDay(p, "setup-failure-fixture", studyCLIRequest{Output: out}, p.Arms[0], p.Candidates[0], in, cfg, nil, false)
			if err == nil || strings.Contains(err.Error(), "uninspected.zip") {
				t.Fatalf("setup failure inspected archive or succeeded: %v", err)
			}
			if _, err := os.Stat(filepath.Join(out, "complete.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("setup failure published success: %v", err)
			}
			if _, err := os.Stat(filepath.Join(out, "FAILED.json")); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, db := range []*cty.CTYDatabase{nil, studyTestCTY(t, map[string]cty.PrefixInfo{})} {
		r := newReplayRunner(replayRequest{})
		r.cfg, r.ctyDB = &config.Config{}, db
		path := filepath.Join(t.TempDir(), "decisions.jsonl")
		if _, err := newStudyObserver(studyTestReferenceDay(nil), r, path); err == nil {
			t.Fatal("unavailable CTY was converted to unknown row coverage")
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("missing CTY setup opened a decision artifact: %v", err)
		}
	}
}

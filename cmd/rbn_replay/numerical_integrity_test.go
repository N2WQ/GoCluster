// File role: Proves replay rejects invalid configured numerical evidence before
// destroying scratch history or constructing runtime owners.
package main

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/config"
)

const b21ReplayValidModel = `{"modes":["CW"],"snr_band_edges":[0,10],"alphabet":"A?","unknown_char":"?","sub_counts":[[[[1,0],[0,1]]]],"del_counts":[[[0,0]]],"ins_counts":[[[0,0]]]}`

func b21ReplayWriteAsset(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "configured-evidence")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func b21ReplayRunner(t *testing.T) (*replayRunner, string, []byte) {
	t.Helper()
	out := t.TempDir()
	scratch := filepath.Join(out, "custom_scp_runtime")
	if err := os.Mkdir(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(scratch, "persisted-observation")
	contents := []byte{0, 0xff, 'o', 'l', 'd', '\r', '\n'}
	if err := os.WriteFile(sentinel, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	r := &replayRunner{outDir: out, cfg: &config.Config{CallCorrection: config.CallCorrectionConfig{
		Enabled: true, MinSpotterReliability: 0.87,
		AdaptiveMinReports: config.AdaptiveMinReportsConfig{Enabled: true,
			Groups: []config.AdaptiveMinReportsGroup{{Name: "fixture", Bands: []string{"20m"}, NormalMinReports: 3}},
		},
		CustomSCP: config.CallCorrectionCustomSCPConfig{
			Enabled: true, HistoryHorizonDays: 60, StaticHorizonDays: 395,
			MaxKeys: 8, MaxSpottersPerKey: 8, CleanupIntervalSeconds: 3600,
			SFloorMinUniqueSpottersExact: 3, SFloorMinUniqueH3CellsExact: 2,
			SFloorMinUniqueSpottersFamily: 3, SFloorMinUniqueH3CellsFamily: 2,
		},
	}}}
	t.Cleanup(r.close)
	return r, sentinel, contents
}

func b21ReplayAssertRefused(t *testing.T, r *replayRunner, sentinel string, contents []byte, err error, key string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "call_correction."+key+" ") {
		t.Errorf("configured invalid evidence must fail with its config key %q: %v", key, err)
	}
	var path string
	switch key {
	case "spotter_reliability_file":
		path = r.cfg.CallCorrection.SpotterReliabilityFile
	case "spotter_reliability_file_cw":
		path = r.cfg.CallCorrection.SpotterReliabilityFileCW
	case "spotter_reliability_file_rtty":
		path = r.cfg.CallCorrection.SpotterReliabilityFileRTTY
	case "confusion_model_file":
		path = r.cfg.CallCorrection.ConfusionModelFile
	}
	if path = strings.TrimSpace(path); path != "" && err != nil && !strings.Contains(err.Error(), path) {
		t.Errorf("configured asset error lost path %q: %v", path, err)
	}
	if r.adaptiveMinReports != nil || r.recentBandStore != nil || r.cleanupRecentBand != nil ||
		r.temporalDecoder != nil || r.tracker != nil || r.resolver != nil || r.driver != nil {
		t.Error("invalid evidence constructed a runtime or cleanup owner")
	}
	got, readErr := os.ReadFile(sentinel)
	if readErr != nil || !bytes.Equal(got, contents) {
		t.Errorf("invalid evidence changed existing scratch bytes: got %x, want %x, read error %v", got, contents, readErr)
	}
}

func b21ReplaySetReliability(cfg *config.CallCorrectionConfig, key, path string) {
	switch key {
	case "spotter_reliability_file":
		cfg.SpotterReliabilityFile = path
	case "spotter_reliability_file_cw":
		cfg.SpotterReliabilityFileCW = path
	case "spotter_reliability_file_rtty":
		cfg.SpotterReliabilityFileRTTY = path
	default:
		panic("unknown reliability fixture key")
	}
}

func TestB21ReplayReliabilityRefusalBeforeScratchAndOwners(t *testing.T) {
	for _, key := range []string{"spotter_reliability_file", "spotter_reliability_file_cw", "spotter_reliability_file_rtty"} {
		for _, value := range []string{"NaN", "+Inf", "-Inf", "missing"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				r, sentinel, contents := b21ReplayRunner(t)
				path := filepath.Join(t.TempDir(), "missing")
				if value != "missing" {
					path = b21ReplayWriteAsset(t, "N0GOOD 0.95\nN0BAD "+value+"\n")
				}
				b21ReplaySetReliability(&r.cfg.CallCorrection, key, " "+path+" ")
				b21ReplayAssertRefused(t, r, sentinel, contents, r.buildReplayRuntime(), key)
			})
		}
		// Reliability is explicitly configured even when the global correction
		// flag is off: the offline constructor still builds a resolver.
		t.Run(key+"/global_disabled", func(t *testing.T) {
			r, sentinel, contents := b21ReplayRunner(t)
			r.cfg.CallCorrection.Enabled = false
			b21ReplaySetReliability(&r.cfg.CallCorrection, key, b21ReplayWriteAsset(t, "N0BAD NaN\n"))
			b21ReplayAssertRefused(t, r, sentinel, contents, r.buildReplayRuntime(), key)
		})
	}
}

func TestB21ReplayConfusionRefusalBeforeScratchAndOwners(t *testing.T) {
	for _, tc := range []struct{ name, from, to string }{
		{"negative_substitution", "[[1,0],[0,1]]", "[[-1,0],[0,1]]"},
		{"negative_deletion", `"del_counts":[[[0,0]]]`, `"del_counts":[[[-1,0]]]`},
		{"negative_insertion", `"ins_counts":[[[0,0]]]`, `"ins_counts":[[[-1,0]]]`},
		{"overflow_substitution", "[[1,0],[0,1]]", "[[9223372036854775807,1],[0,1]]"},
		{"overflow_deletion", `"del_counts":[[[0,0]]]`, `"del_counts":[[[9223372036854775807,1]]]`},
		{"overflow_insertion", `"ins_counts":[[[0,0]]]`, `"ins_counts":[[[9223372036854775807,1]]]`},
		{"descending_edges", "[0,10]", "[10,0]"},
		{"equal_edges", "[0,10]", "[0,0]"},
		{"nonfinite_json_edge", "[0,10]", "[0,1e309]"},
		{"malformed_json", b21ReplayValidModel, "{"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, sentinel, contents := b21ReplayRunner(t)
			r.cfg.CallCorrection.ConfusionModelEnabled = true
			r.cfg.CallCorrection.ConfusionModelWeight = 1
			r.cfg.CallCorrection.ConfusionModelFile = b21ReplayWriteAsset(t, strings.Replace(b21ReplayValidModel, tc.from, tc.to, 1))
			b21ReplayAssertRefused(t, r, sentinel, contents, r.buildReplayRuntime(), "confusion_model_file")
		})
	}
	for _, tc := range []struct {
		name, body string
		global     bool
		weight     float64
	}{
		{"empty_path", "", true, 1},
		{"missing_file", "missing", true, 1},
		{"zero_weight_still_loads", "{", true, 0},
		{"global_disabled_still_loads", "{", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, sentinel, contents := b21ReplayRunner(t)
			r.cfg.CallCorrection.Enabled = tc.global
			r.cfg.CallCorrection.ConfusionModelEnabled = true
			r.cfg.CallCorrection.ConfusionModelWeight = tc.weight
			if tc.body == "missing" {
				r.cfg.CallCorrection.ConfusionModelFile = filepath.Join(t.TempDir(), "missing")
			} else if tc.body != "" {
				r.cfg.CallCorrection.ConfusionModelFile = b21ReplayWriteAsset(t, tc.body)
			}
			b21ReplayAssertRefused(t, r, sentinel, contents, r.buildReplayRuntime(), "confusion_model_file")
		})
	}
}

func TestB21ReplayDirectNonfiniteWeightsPrecedeAssetLoads(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		for _, key := range []string{"min_spotter_reliability", "confusion_model_weight"} {
			for _, global := range []bool{false, true} {
				t.Run(key+"/"+b21ReplayFloatName(value)+"/global_"+map[bool]string{false: "off", true: "on"}[global], func(t *testing.T) {
					r, sentinel, contents := b21ReplayRunner(t)
					r.cfg.CallCorrection.Enabled = global
					// An asset failure is an independent ordering oracle: invalid
					// direct weights must be diagnosed before any configured load.
					r.cfg.CallCorrection.SpotterReliabilityFile = filepath.Join(t.TempDir(), "not-readable")
					if key == "min_spotter_reliability" {
						r.cfg.CallCorrection.MinSpotterReliability = value
					} else {
						r.cfg.CallCorrection.ConfusionModelEnabled = true
						r.cfg.CallCorrection.ConfusionModelWeight = value
					}
					b21ReplayAssertRefused(t, r, sentinel, contents, r.buildReplayRuntime(), key)
				})
			}
		}
	}
}

func b21ReplayFloatName(value float64) string {
	if math.IsNaN(value) {
		return "NaN"
	}
	if math.IsInf(value, 1) {
		return "+Inf"
	}
	return "-Inf"
}

func TestB21ReplayValidAndDisabledEvidenceControls(t *testing.T) {
	path := b21ReplayWriteAsset(t, "N0AAA -2\nN0BBB 2\nN0CCC 0.95\n")
	cfg := config.CallCorrectionConfig{SpotterReliabilityFile: path, SpotterReliabilityFileCW: path, SpotterReliabilityFileRTTY: path}
	base, cw, rtty, err := loadSpotterReliability(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, weights := range []map[string]float64{base, cw, rtty} {
		if len(weights) != 3 || weights["N0AAA"] != 0 || weights["N0BBB"] != 1 || weights["N0CCC"] != 0.95 {
			t.Errorf("finite reliability clamping changed: %v", weights)
		}
	}
	for _, tc := range []struct {
		name          string
		global, model bool
		weight        float64
	}{
		{"valid_model_zero_weight", true, true, 0},
		{"disabled_model_ignores_invalid_path_and_weight", true, false, math.NaN()},
		{"empty_optional_paths_global_disabled", false, false, math.Inf(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, sentinel, contents := b21ReplayRunner(t)
			r.cfg.CallCorrection.Enabled = tc.global
			r.cfg.CallCorrection.ConfusionModelEnabled = tc.model
			r.cfg.CallCorrection.ConfusionModelWeight = tc.weight
			r.cfg.CallCorrection.ConfusionModelFile = filepath.Join(t.TempDir(), "missing-disabled-model")
			if tc.model {
				r.cfg.CallCorrection.ConfusionModelFile = b21ReplayWriteAsset(t, b21ReplayValidModel)
			}
			loaded, loadErr := loadConfusionModel(r.cfg.CallCorrection)
			if loadErr != nil || (loaded != nil) != tc.model {
				t.Fatalf("own model flag/zero-weight load contract changed: model=%v err=%v", loaded != nil, loadErr)
			}
			if err := r.buildReplayRuntime(); err != nil {
				t.Fatalf("valid or disabled evidence refused: %v", err)
			}
			if r.resolver == nil || r.driver == nil || r.tracker == nil || r.adaptiveMinReports == nil {
				t.Fatal("valid replay did not construct its ordinary owners")
			}
			if tc.global {
				if r.recentBandStore == nil || r.cleanupRecentBand == nil {
					t.Error("valid enabled replay did not construct its CustomSCP owner")
				}
				if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
					t.Errorf("valid enabled replay did not replace its scratch directory: %v", err)
				}
			} else {
				got, err := os.ReadFile(sentinel)
				if err != nil || !bytes.Equal(got, contents) || r.recentBandStore != nil || r.cleanupRecentBand != nil {
					t.Errorf("disabled global history changed scratch/ownership: bytes=%x err=%v", got, err)
				}
			}
		})
	}
}

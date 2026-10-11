// File role: Exercises the actual offline evaluator startup using valid input,
// distinguishing configured-asset refusal from silent unit-weight/model fallback.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const b21RevengValidModel = `{"modes":["CW"],"snr_band_edges":[0,10],"alphabet":"A?","unknown_char":"?","sub_counts":[[[[1,0],[0,1]]]],"del_counts":[[[0,0]]],"ins_counts":[[[0,0]]]}`

// TestB21RevengMainHelper runs main in the package's already prepared stable
// executable. No separate executable or network listener is created.
func TestB21RevengMainHelper(t *testing.T) {
	if os.Getenv("DXC_B21_REVENG_MAIN_HELPER") != "run" {
		t.Skip("actual-main subprocess only")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing helper separator")
	}
	os.Args = append([]string{"callcorr_reveng_rebuilt"}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	main()
	os.Exit(0)
}

type b21RevengFixture struct {
	root, config, rbn, labels, out string
}

func b21RevengWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func b21RevengEditConfig(t *testing.T, path string, edit func(map[string]any)) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	edit(doc)
	data, err = yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func b21RevengSection(t *testing.T, doc map[string]any, key string) map[string]any {
	t.Helper()
	section, ok := doc[key].(map[string]any)
	if !ok {
		t.Fatalf("copied fixture config has no mapping %q", key)
	}
	return section
}

func b21RevengNewFixture(t *testing.T, global bool) b21RevengFixture {
	t.Helper()
	root := t.TempDir()
	f := b21RevengFixture{root: root, config: filepath.Join(root, "config"), rbn: filepath.Join(root, "rbn"),
		labels: filepath.Join(root, "labels"), out: filepath.Join(root, "evaluation")}
	for _, path := range []string{f.config, f.rbn, f.labels} {
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	files, err := filepath.Glob(filepath.Join("..", "..", "data", "config", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("config inventory: %v / %d", err, len(files))
	}
	for _, source := range files {
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(f.config, filepath.Base(source)), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Production loader fills blank allowlist paths with its deployment default,
	// so provide an actual private empty file rather than an empty setting.
	allowlist := filepath.Join(root, "empty-allowlist.txt")
	b21RevengWrite(t, allowlist, "")
	b21RevengEditConfig(t, filepath.Join(f.config, "data.yaml"), func(doc map[string]any) {
		b21RevengSection(t, doc, "cty")["enabled"] = false
		fcc := b21RevengSection(t, doc, "fcc_uls")
		fcc["enabled"], fcc["allowlist_path"] = false, allowlist
		fcc["db_path"] = filepath.Join(root, "unused-fcc.db")
		b21RevengSection(t, doc, "ised")["enabled"] = false
		b21RevengSection(t, doc, "ised")["db_path"] = filepath.Join(root, "unused-ised.db")
	})
	// YAML requires these paths to be nonblank even when their feature is off.
	// Keep every unrelated configured asset valid; only the selected asset is
	// replaced by a counterexample in each actual-main test.
	reliability := filepath.Join(root, "valid-reliability.txt")
	model := filepath.Join(root, "valid-model.json")
	b21RevengWrite(t, reliability, "K1ABC 0.95\n")
	b21RevengWrite(t, model, b21RevengValidModel)
	f.editCorrection(t, func(cc map[string]any) {
		cc["enabled"] = global
		cc["spotter_reliability_file"], cc["spotter_reliability_file_cw"], cc["spotter_reliability_file_rtty"] = reliability, reliability, reliability
		cc["confusion_model_enabled"], cc["confusion_model_file"], cc["confusion_model_weight"] = false, model, 0
		cc["recent_band_bonus_enabled"], cc["stabilizer_enabled"] = false, false
		b21RevengSection(t, cc, "custom_scp")["enabled"] = false
		b21RevengSection(t, cc, "temporal_decoder")["enabled"] = false
		b21RevengSection(t, cc, "adaptive_min_reports")["enabled"] = false
	})
	b21RevengWrite(t, filepath.Join(f.rbn, "20260426.csv"),
		"callsign,freq,band,dx,db,date,mode,tx_mode\nK1ABC,3547.0,80m,OZ4ADX,5,2026-04-26 03:18:00,CQ,CW\n")
	b21RevengWrite(t, filepath.Join(f.labels, "corrections-2026.log"),
		"26-Apr 0318Z OZ4ADX 3547.0 OZ4ADY 5 K1ABC CW\n")
	return f
}

func (f b21RevengFixture) editCorrection(t *testing.T, edit func(map[string]any)) {
	t.Helper()
	b21RevengEditConfig(t, filepath.Join(f.config, "pipeline.yaml"), func(doc map[string]any) {
		edit(b21RevengSection(t, doc, "call_correction"))
	})
}

func (f b21RevengFixture) asset(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(f.root, "configured-evidence")
	b21RevengWrite(t, path, body)
	return path
}

func (f b21RevengFixture) run(t *testing.T) (int, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	args := []string{"-test.run=^TestB21RevengMainHelper$", "--", "-config-dir", f.config,
		"-rbn-dir", f.rbn, "-other-dir", f.labels, "-out", f.out, "-eval-negatives", "1"}
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Dir = f.root
	cmd.Env = append(os.Environ(), "DXC_B21_REVENG_MAIN_HELPER=run")
	cmd.WaitDelay = 2 * time.Second
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("actual main missed execution bound: %v\n%s", ctx.Err(), output)
	}
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("actual main failed to launch/join: %v\n%s", err, output)
		}
		code = exitErr.ExitCode()
	}
	return code, string(output)
}

var b21RevengArtifacts = []string{"inferred_method.json", "training_report.json", "eval_report.json", "summary.txt"}

func (f b21RevengFixture) assertRefused(t *testing.T, key, path string) {
	t.Helper()
	code, output := f.run(t)
	if code != 1 || !strings.Contains(output, "call_correction."+key+" ") || !strings.Contains(output, path) {
		t.Errorf("actual main must refuse configured asset with key %q and path %q: exit=%d\n%s", key, path, code, output)
	}
	if strings.Contains(output, "Warning:") || strings.Contains(output, "Wrote outputs to") {
		t.Errorf("invalid configured evidence continued with a fallback: %s", output)
	}
	for _, base := range b21RevengArtifacts {
		if _, err := os.Stat(filepath.Join(f.out, base)); !os.IsNotExist(err) {
			t.Errorf("invalid configured evidence produced evaluation artifact %s: %v", base, err)
		}
	}
	// main creates the output directory before reading config; that existing
	// behavior is separate from whether evaluation artifacts are produced.
	if info, err := os.Stat(f.out); err != nil || !info.IsDir() {
		t.Errorf("actual-main fixture did not reach its existing output-directory step: %v", err)
	}
}

func TestB21RevengActualMainReliabilityRefusal(t *testing.T) {
	for _, key := range []string{"spotter_reliability_file", "spotter_reliability_file_cw", "spotter_reliability_file_rtty"} {
		for _, value := range []string{"NaN", "+Inf", "-Inf", "missing"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				f := b21RevengNewFixture(t, true)
				path := filepath.Join(f.root, "missing-evidence")
				if value != "missing" {
					path = f.asset(t, "N0GOOD 0.95\nN0BAD "+value+"\n")
				}
				f.editCorrection(t, func(cc map[string]any) { cc[key] = path })
				f.assertRefused(t, key, path)
			})
		}
		t.Run(key+"/global_disabled_still_loads", func(t *testing.T) {
			f := b21RevengNewFixture(t, false)
			path := f.asset(t, "N0BAD NaN\n")
			f.editCorrection(t, func(cc map[string]any) { cc[key] = path })
			f.assertRefused(t, key, path)
		})
	}
}

func TestB21RevengActualMainConfusionRefusal(t *testing.T) {
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
		{"malformed_json", b21RevengValidModel, "{"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := b21RevengNewFixture(t, true)
			path := f.asset(t, strings.Replace(b21RevengValidModel, tc.from, tc.to, 1))
			f.editCorrection(t, func(cc map[string]any) {
				cc["confusion_model_enabled"], cc["confusion_model_file"], cc["confusion_model_weight"] = true, path, 1
			})
			f.assertRefused(t, "confusion_model_file", path)
		})
	}
	for _, tc := range []struct {
		name, body string
		global     bool
		weight     int
	}{
		{"missing_file", "missing", true, 1},
		{"zero_weight_still_loads", "{", true, 0},
		{"global_disabled_still_loads", "{", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := b21RevengNewFixture(t, tc.global)
			path := ""
			if tc.body == "missing" {
				path = filepath.Join(f.root, "missing-evidence")
			} else if tc.body != "" {
				path = f.asset(t, tc.body)
			}
			f.editCorrection(t, func(cc map[string]any) {
				cc["confusion_model_enabled"], cc["confusion_model_file"], cc["confusion_model_weight"] = true, path, tc.weight
			})
			f.assertRefused(t, "confusion_model_file", path)
		})
	}
}

func TestB21RevengActualMainValidAndDisabledControls(t *testing.T) {
	for _, tc := range []struct {
		name, requiredLog string
		global, model     bool
		reliability       bool
	}{
		{"valid_optional_assets_model_disabled", "Wrote outputs to", true, false, false},
		{"disabled_model_ignores_missing_path", "Wrote outputs to", false, false, false},
		{"valid_model_zero_weight", "Loaded call correction confusion model from", true, true, false},
		{"finite_clamped_reliability_all_paths", "Loaded 3 RTTY spotter reliability weights from", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := b21RevengNewFixture(t, tc.global)
			f.editCorrection(t, func(cc map[string]any) {
				cc["confusion_model_file"] = filepath.Join(f.root, "missing-disabled-model")
				if tc.model {
					cc["confusion_model_enabled"], cc["confusion_model_file"] = true, f.asset(t, b21RevengValidModel)
				}
				if tc.reliability {
					path := f.asset(t, "N0AAA -2\nN0BBB 2\nN0CCC 0.95\n")
					cc["spotter_reliability_file"], cc["spotter_reliability_file_cw"], cc["spotter_reliability_file_rtty"] = path, path, path
				}
			})
			code, output := f.run(t)
			if code != 0 || !strings.Contains(output, tc.requiredLog) || strings.Contains(output, "Warning:") {
				t.Fatalf("valid/disabled actual main did not complete normally: exit=%d\n%s", code, output)
			}
			for _, base := range b21RevengArtifacts {
				body, err := os.ReadFile(filepath.Join(f.out, base))
				if err != nil || len(body) == 0 {
					t.Fatalf("valid actual main missing artifact %s: %v", base, err)
				}
			}
			data, err := os.ReadFile(filepath.Join(f.out, "eval_report.json"))
			if err != nil {
				t.Fatal(err)
			}
			var report evalReport
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatal(err)
			}
			if report.RBNRows != 1 || report.Replay.RBNRows != 1 || report.Replay.CandidateRows != 1 || report.Labels.Parsed != 1 || report.PositivesTotal != 1 {
				t.Errorf("fixture must exercise a real row and external correction label: %+v", report)
			}
		})
	}
}

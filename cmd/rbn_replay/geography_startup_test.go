// File role: Proves replay's cold startup validates geography before download or scratch IO.
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

func TestReplayGeographyStartup(t *testing.T) {
	const env, marker = "GOCLUSTER_REPLAY_GEOGRAPHY_CHILD", "REPLAY_GEOGRAPHY_CHILD_EXECUTED"
	if mode := os.Getenv(env); mode != "" {
		if pathreliability.EncodeCoarseCell("FN31") != pathreliability.InvalidCell {
			t.Fatal("child prewarmed")
		}
		configDir, work := replayGeographyConfig(t, mode)
		t.Chdir(work)
		r := newReplayRunner(replayRequest{dateValue: "2026-11-10", configDir: configDir, archiveDir: filepath.Join(work, "archive")})
		defer r.close()
		if mode == "missing_before_io" {
			scratch := filepath.Join(work, "archive", "rbn_replay", "2026-11-10", "custom_scp_runtime")
			if err := os.MkdirAll(scratch, 0700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(scratch, "existing-evidence")
			if err := os.WriteFile(sentinel, []byte("untouched"), 0600); err != nil {
				t.Fatal(err)
			}
			err := r.run()
			if err == nil || !strings.Contains(err.Error(), "h3_table_path") || !strings.Contains(err.Error(), "res1.bin") || !strings.Contains(err.Error(), "res2.bin") {
				t.Fatalf("missing geography did not fail before IO: %v", err)
			}
			if r.archiveDir != "" || r.outDir != "" || r.zipPath != "" || r.licenseInputsDir != "" || r.recentBandStore != nil || r.resolver != nil {
				t.Fatal("invalid geography created later owners")
			}
			if got, err := os.ReadFile(sentinel); err != nil || string(got) != "untouched" {
				t.Fatalf("scratch changed: %q %v", got, err)
			}
			entries, err := os.ReadDir(scratch)
			if err != nil || len(entries) != 1 {
				t.Fatalf("scratch ownership changed: %v", err)
			}
		} else {
			err := r.loadClusterConfig()
			if mode == "global_disabled" {
				if err == nil || !strings.Contains(err.Error(), "call_correction.enabled must be true") {
					t.Fatalf("global disabled error changed: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if mode == "custom_enabled" {
				if pathreliability.EncodeCoarseCell("FN31") == pathreliability.InvalidCell {
					t.Fatal("replay startup omitted geography")
				}
				r.outDir = t.TempDir()
				if err := r.openRecentBandStore(); err != nil {
					t.Fatal(err)
				}
				at := time.Now().UTC().Truncate(time.Second)
				for _, tc := range []struct {
					call, second string
					want         bool
				}{{"K1TWO", "IO91", true}, {"K1ONE", "FN31", false}} {
					for i, grid := range []string{"FN31", tc.second} {
						s := spot.NewSpot(tc.call, fmt.Sprintf("N%dAAA", i), 14020, "CW")
						s.Time, s.Confidence, s.HasReport, s.Report, s.DEMetadata.Grid = at, "V", true, 20, grid
						s.EnsureNormalized()
						recordRecentBandObservation(s, r.recentBandStore, r.cfg.CallCorrection, at)
					}
					if got := r.recentBandStore.HasRecentSupport(tc.call, "20m", "CW", 2, at); got != tc.want {
						t.Fatalf("%s two-vs-one-cell support=%t", tc.call, got)
					}
				}
			} else if pathreliability.EncodeCoarseCell("FN31") != pathreliability.InvalidCell {
				t.Fatal("disabled replay owner initialized geography")
			}
		}
		t.Log(marker + " " + mode)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"custom_enabled", "custom_disabled", "global_disabled", "missing_before_io"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestReplayGeographyStartup$", "-test.v", "-test.timeout=40s")
			cmd.Env = append(os.Environ(), env+"="+mode)
			out, err := cmd.CombinedOutput()
			if err != nil || !bytes.Contains(out, []byte(marker+" "+mode)) {
				t.Fatalf("child: %v\n%s", err, out)
			}
		})
	}
}

// Config and CWD deliberately differ: h3_table_path remains CWD-relative, never
// config-relative or a package/repository fallback. Only private files change.
func replayGeographyConfig(t *testing.T, mode string) (string, string) {
	t.Helper()
	repo := filepath.Dir(filepath.Dir(testsupport.H3TableDir(t)))
	directory, work := t.TempDir(), t.TempDir()
	entries, err := os.ReadDir(filepath.Join(repo, "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") || entry.Name() == "openai.yaml" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(repo, "data", "config", entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var raw map[string]any
		if err := yaml.Unmarshal(data, &raw); err != nil {
			t.Fatal(err)
		}
		switch entry.Name() {
		case "data.yaml":
			raw["h3_table_path"] = "tables"
		case "path_reliability.yaml":
			raw["enabled"] = false
			raw["voacap_fallback"].(map[string]any)["enabled"] = false
			raw["native_160m_fallback"].(map[string]any)["enabled"] = false
		case "pipeline.yaml":
			correction := raw["call_correction"].(map[string]any)
			correction["enabled"] = mode != "global_disabled"
			custom := correction["custom_scp"].(map[string]any)
			custom["enabled"] = mode != "custom_disabled"
			custom["resolver_min_unique_spotters"], custom["resolver_min_unique_h3_cells"] = 2, 2
			custom["block_cache_mb"], custom["memtable_size_mb"] = 1, 4
		}
		data, err = yaml.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, entry.Name()), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if mode == "custom_enabled" {
		if err := os.Mkdir(filepath.Join(work, "tables"), 0700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"res1.bin", "res2.bin"} {
			data, err := os.ReadFile(filepath.Join(testsupport.H3TableDir(t), name))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(work, "tables", name), data, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	return directory, work
}
